package edu.ucr.cs.bdlab.raptor

/**
 * Flatten a list of intersections into tuples in the format
 * (FileTileID: Long, (GeometryID: Long, y: Int, x1: Int, x2: Int)) where
 *  - FileTileID is a 64-bit long where the higher 32-bits represent the file ID and the lower 32-bits represent the tile ID
 *  - GeometryID is a unique ID for each geometry or feature
 *  - y is the scan line index of the intersection in pixel space
 *  - x1 is the index of the first column in the raster (i.e., pixel) in the intersections (inclusive)
 *  - x2 is the index at which that intersection ends (inclusive)
 *
 * @param FileIDs a list of IDs that represent the files for each intersection
 * @param intersectionsList a list of intersections
 */
class IntersectionsIterator(FileIDs:Array[Int], intersectionsList : Array[Intersections])
  extends Iterator[(Long, PixelRange)]  {
  assert(!intersectionsList.isEmpty && intersectionsList(0).getNumIntersections != 0,
      "All intersections cannot be empty")

  var fileTileID: Long = _
  var geometryID: Long = _
  var y: Int = _
  var x1: Int = _

  /**The intersection that contains the nextTuple*/
  var currentI: Int = -1

  /**The range that contains nextTuple*/
  var currentRange: Int = _

  /**The tuple that will be returned if next is called*/
  override def hasNext: Boolean = currentI < intersectionsList.length - 1 || currentRange < currentIntersection.getNumIntersections - 1

  private def currentIntersection: Intersections = intersectionsList(currentI)

  /**
   * Prefetches the next record and returns the value of that tuple or null if end of file is reached
   * @return either the next tuple or null
   */
  def next: (Long, PixelRange) = {
    if (currentI == -1 || currentRange == currentIntersection.getNumIntersections - 1) {
      // First time or finished the current intersection
      currentI += 1
      assert(currentIntersection.getNumIntersections > 0)
      currentRange = 0
    } else {
      currentRange += 1
    }
    fileTileID = (FileIDs(currentI).toLong << 32) | currentIntersection.getTileID(currentRange)
    geometryID = currentIntersection.getFeatureID(currentRange)
    y = currentIntersection.getY(currentRange)
    x1 = currentIntersection.getX1(currentRange)
    (fileTileID, PixelRange(geometryID, y, x1, currentIntersection.getX2(currentRange)))
  }

  implicit def orderingByPolygonID[A <: IntersectionsIterator ] : Ordering[A] = {
    Ordering.by(x => (x.fileTileID,x.geometryID,x.y,x.x1))
  }
}
