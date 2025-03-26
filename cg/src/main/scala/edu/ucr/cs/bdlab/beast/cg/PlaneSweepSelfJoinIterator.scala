package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

/**
 * A class that runs the plane-sweep self-join algorithm and emits records one pair at a time.
 * Used to avoid keeping all pairs in memory before producing the final result.
 * We include the duplicate avoidance testing here since it is more efficient to test when we already know the MBRs.
 */
class PlaneSweepSelfJoinIterator[T <: IFeature]
  (var r: Array[T], dupAvoidanceMBR: EnvelopeNDLite, numMBRTests: LongAccumulator = null)
  extends Iterator[(T, T)] with Logging {

  /**
   * A comparator that sorts features by xmin which is needed for the planesweep algorithm
   */
  val featureComparator: (IFeature, IFeature) => Boolean =
    (f1, f2) => f1.getGeometry.getEnvelopeInternal.getMinX < f2.getGeometry.getEnvelopeInternal.getMinX
  r = r.sortWith(featureComparator)

  /**A counter that keeps track of the result size. Used for debugging and logging.*/
  var count: Long = 0

  // Initialize the plane-sweep algorithm and make it ready to emit records
  var i: Int = 0
  var j: Int = 0

  /**Prepare the first result (if any)*/
  seekToNextOutput()

  // Accessor methods for envelope coordinates
  private def xmin(i: Int): Double = r(i).getGeometry.getEnvelopeInternal.getMinX
  private def xmax(i: Int): Double = r(i).getGeometry.getEnvelopeInternal.getMaxX
  private def ymin(i: Int): Double = r(i).getGeometry.getEnvelopeInternal.getMinY
  private def ymax(i: Int): Double = r(i).getGeometry.getEnvelopeInternal.getMaxY

  /**Tests if the MBRs of two features, one on each list, overlap*/
  private def rectangleOverlaps(a: Int, b: Int): Boolean =
    r(a).getGeometry.getEnvelopeInternal.intersects(r(b).getGeometry.getEnvelopeInternal)

  /**
   * Move to the next matching pair of records. The matching pair (if any), it should always be stored in ii and jj
   * @return whether a result was found `true` or an end-of-list was reached `false`
   */
  private def seekToNextOutput(): Boolean = {
    while (i < r.length) {
      j += 1
      while (j < r.length && xmin(j) <= xmax(i)) {
        if (numMBRTests != null) numMBRTests.add(1)
        if (rectangleOverlaps(i, j) && referencePointTest(i, j)) {
          // Found a result, return it
          count += 1
          return true
        }
        j += 1
      }
      i += 1
      j = i + 1
    }
    logDebug(s"Self planesweep processed ${r.length} records and found $count matches")
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

    val refPointX1: Double = xmin(i1) max xmin(i2)
    val refPointX2: Double = xmax(i1) min xmax(i2)
    val refPointY1: Double = ymin(i1) max ymin(i2)
    val refPointY2: Double = ymax(i1) min ymax(i2)

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

  override def hasNext: Boolean = i < r.size && j < r.size

  override def next(): (T, T) = {
    val matchedPair = (r(i), r(j))
    seekToNextOutput()
    matchedPair
  }
}
