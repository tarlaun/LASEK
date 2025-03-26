package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.util.CompactLongArray
import edu.ucr.cs.bdlab.raptor

/**
 * Breaks [[Intersections]] by tile. This class acts as an iterator
 * for pairs of MetadataTileID and [[CompactIntersections]].
 * MetadataTileID is a 64-bit long that is a concatenation of a metadata ID (most significant)
 * and tile ID (least significant) with 32-bit each.
 *
 * @param intersections the list of all intersections as pairs of metadata ID and intersections
 */
class CompactIntersectionsTileBreaker(intersections: Iterator[(Int, Intersections)])
  extends Iterator[(Long, CompactIntersections)] {

  /** The index of the intersection that will be returned next */
  var i: Int = 0

  /**The metadata ID currently being processed and will be returned by next() */
  var metadataID: Long = _

  /**The [[Intersections]] currently being processed and from which next() will return its value*/
  var currentIntersections: Intersections = _

  def moveToNextIntersections: Unit = {
    // Jump to the next file with non-empty intersections
    while (intersections.hasNext && (currentIntersections == null || currentIntersections.getNumIntersections == 0)) {
      val n = intersections.next()
      metadataID = n._1
      currentIntersections = n._2
    }
    if (currentIntersections != null && currentIntersections.getNumIntersections > 0)
      i = 0
  }

  // Initialize by moving to the next intersections
  moveToNextIntersections

  override def hasNext: Boolean = currentIntersections != null

  override def next(): (Long, CompactIntersections) = {
    val firstRange: Int = i
    val tileID = currentIntersections.getTileID(i)
    do {
      i += 1
    } while (i < currentIntersections.getNumIntersections && currentIntersections.getTileID(i) == tileID)
    val ys = CompactLongArray.constructLongArray(currentIntersections.ys, firstRange, i)
    val xs = CompactLongArray.constructLongArray(currentIntersections.xs, 2 * firstRange, 2 * i)
    val tileIDs = CompactLongArray.constructLongArray(currentIntersections.tileID, firstRange, i)
    val geometryIndexes = CompactLongArray.constructLongArray(currentIntersections.polygonIndexes, firstRange, i)
    val compactIntersections = new CompactIntersections(tileIDs, ys, xs, geometryIndexes, currentIntersections.featureIDs)
    val metadataTileID: Long = (metadataID << 32) | tileID

    // Move to the next intersections, if needed
    if (i >= currentIntersections.getNumIntersections) {
      currentIntersections = null
      moveToNextIntersections
    }
    (metadataTileID, compactIntersections)
  }
}
