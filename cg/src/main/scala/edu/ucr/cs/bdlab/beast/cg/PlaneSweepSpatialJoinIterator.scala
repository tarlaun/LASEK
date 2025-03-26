package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

/**
 * A class that runs the plane-sweep join algorithm and emits records one pair at a time.
 * Used to avoid keeping all pairs in memory before producing the final result.
 * We include the duplicate avoidance testing here since it is more efficient to test when we already know the MBRs.
 */
class PlaneSweepSpatialJoinIterator[T1 <: IFeature, T2 <: IFeature]
  (var r1: Array[T1], var r2: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, numMBRTests: LongAccumulator = null)
  extends Iterator[(T1, T2)] with Logging {

  /**
   * A comparator that sorts features by xmin which is needed for the planesweep algorithm
   */
  val featureComparator: (IFeature, IFeature) => Boolean =
    (f1, f2) => f1.getGeometry.getEnvelopeInternal.getMinX < f2.getGeometry.getEnvelopeInternal.getMinX
  r1 = r1.sortWith(featureComparator)
  r2 = r2.sortWith(featureComparator)

  /**A counter that keeps track of the result size. Used for debugging and logging.*/
  var count: Long = 0

  // Initialize the plane-sweep algorithm and make it ready to emit records
  var i: Int = 0
  var j: Int = 0
  var ii: Int = 0
  var jj: Int = 0

  /**The list that is currently active, either 1 or 2*/
  var activeList: Int = 0

  /**Prepare the first result (if any)*/
  seekToNextOutput()

  // Accessor methods for envelope coordinates
  def xmin1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMinX
  def xmax1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMaxX
  def ymin1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMinY
  def ymax1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMaxY
  def xmin2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMinX
  def xmax2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMaxX
  def ymin2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMinY
  def ymax2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMaxY

  /**Tests if the MBRs of two features, one on each list, overlap*/
  def rectangleOverlaps(a: Int, b: Int): Boolean =
    r1(a).getGeometry.getEnvelopeInternal.intersects(r2(b).getGeometry.getEnvelopeInternal)

  /**
   * Move to the next matching pair of records. The matching pair (if any), it should always be stored in ii and jj
   * @return whether a result was found `true` or an end-of-list was reached `false`
   */
  def seekToNextOutput(): Boolean = {
    while (i < r1.length  && j < r2.length) {
      // If not list is currently activated, activate the list with the left-most rectangle
      if (activeList == 0) {
        activeList = if (xmin1(i) < xmin2(j)) 1 else 2
        ii = i
        jj = j
      } else if (activeList == 1) {
        jj += 1
      } else if (activeList == 2) {
        ii += 1
      }
      if (activeList == 1) {
        // Fix the record in list 1, and go through list 2 until the first matching pair is found
        while (jj < r2.length && xmin2(jj) <= xmax1(ii)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj) && referencePointTest(ii, jj)) {
            // Found a result, return it
            count += 1
            return true
          }
          jj += 1
        }
        do {
          i += 1;
        } while (i < r1.length && xmax1(i) < xmin2(j))
        // Reset the active list
        activeList = 0
      } else if (activeList == 2) {
        // Fix the record in list 2, and go through list 1 until the first matching pair is found
        while (ii < r1.length && xmin1(ii) <= xmax2(jj)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj) && referencePointTest(ii, jj)) {
            // Found a result, return it
            count += 1
            return true
          }
          ii += 1
        }
        // Skip until the first record that might produce a result from list 2
        do {
          j += 1;
        } while (j < r2.length && xmax2(j) < xmin1(i))
        // Reset the active list
        activeList = 0
      }
    }
    logDebug(s"Planesweep processed ${r1.length} X ${r2.length} pairs and found ${count} matches")
    // Finished the lists without finding any results
    false
  }

  /**
   * Run the reference point test between two records #i and #j in the two datasets.
   * Returns `true` if this pair should be reported to the answer. A pair is reported in three cases:
   *
   *  - If its reference point, i.e., top-left corner of the intersection, falls in the duplicate avoidance MBR
   *  - If the intersection MBR has a width of zero and its right-most edge is coincident
   *    with the right-most edge of the duplicate avoidance MBR.
   *  - If the intersection MBR has a height of zero and its top-most edge is coincident
   *    with the top-most edge of the duplicate avoidance MBR
   *
   * The last two conditions are added to handle cases of vertical lines, horizontal lines, or points that
   * define the boundary of a partition. For example, think of the right-most point of a partition that
   * does not technically fall inside the partition but does not belong to any other partitions either.
   * @param i1 the index of the first record
   * @param i2 the index of the second record
   * @return `true` if this pair should be reported in the answer
   */
  private def referencePointTest(i1: Int, i2: Int): Boolean = {
    // No duplicate avoidance test needed
    if (dupAvoidanceMBR == null)
      return true
    if (numMBRTests != null) numMBRTests.add(1)

    val refPointX1: Double = xmin1(i1) max xmin2(i2)
    val refPointX2: Double = xmax1(i1) min xmax2(i2)
    val refPointY1: Double = ymin1(i1) max ymin2(i2)
    val refPointY2: Double = ymax1(i1) min ymax2(i2)

    if (refPointX1 < dupAvoidanceMBR.getMinCoord(0))
      return false
    if (refPointX1 > dupAvoidanceMBR.getMaxCoord(0))
      return false
    if (refPointX1 == dupAvoidanceMBR.getMaxCoord(0) && refPointX2 > refPointX1)
      return false

    if (refPointY1 < dupAvoidanceMBR.getMinCoord(1))
      return false
    if (refPointY1 > dupAvoidanceMBR.getMaxCoord(1))
      return false
    if (refPointY1 == dupAvoidanceMBR.getMaxCoord(1) && refPointY2 > refPointY1)
      return false

    // If all previous tests fails, then we should report this point
    true
  }

  override def hasNext: Boolean = i < r1.size && j < r2.size

  override def next(): (T1, T2) = {
    val matchedPair = (r1(ii), r2(jj))
    seekToNextOutput()
    matchedPair
  }
}
