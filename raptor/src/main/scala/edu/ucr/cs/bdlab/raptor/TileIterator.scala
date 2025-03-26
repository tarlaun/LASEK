package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.spark.util.LongAccumulator
import org.apache.spark.internal.Logging
import scala.reflect.ClassTag

/**
 * Converts a list of intersections and a list of tiles to a list of pixel values in the following format:
 * 1. Long: The feature (or geometry) ID
 * 2. RasterMetadata: the metadata of the raster file
 * 3. Int: x-coordinate
 * 4. Int: y-coordinate
 * 5. T: pixel value as provided in the raster data
 * Both inputs should be sorted on the key (RasterTileID) which is a concatenation of the raster file ID
 * in the most significant 32 bits and tile ID is the least significant 32 bits.
 *
 * @param intersections an iterator of intersections in the following format
 *                      (RasterTileID, (GeometryID, Y, X1, X2))
 * @param tiles         a list of (RasterTileID, ITile) pairs sorted by the key
 */
class TileIterator[T](intersections: Iterator[(Long, PixelRange)], tiles: Iterator[(Long, ITile[T])],
                      numTiles: LongAccumulator = null, numRanges: LongAccumulator = null)
                     (implicit @transient t: ClassTag[T])
  extends Iterator[RaptorJoinResultTile[T]] with Logging {

  /** The tile currently being processed */
  var currentTile: (Long, ITile[T]) = _

  /** The range that nextTuple is contained in */
  var currentRange: (Long, PixelRange) = _

  /** The current tile ID */
  var currentTileID: Long = -1

  /** The current tile ID */
  var currentGeometryID: Long = -1

  /** The x-coordinate that nextTuple contains */
  var x: Int = _

  /** The tuple that will be returned when next is called */
  var nextTuple: RaptorJoinResultTile[T] = _

  /** A flag that is raised when end-of-file is reached */
  var eof: Boolean = false

  /** A flag */
  var fromBeginning: Boolean = true

  /**
   * Prefetches the next tuple and returns it. If end-of-file is reached, this function will return null
   *
   * @return the next record or null if end-of-file is reached
   */
  private def prefetchNext: RaptorJoinResultTile[T] = {
    var maskTile: MaskTile[T] = null
    if (currentRange == null && intersections.hasNext) {
      if (numRanges != null)
        numRanges.add(1)
      currentRange = intersections.next()
      currentTileID = currentRange._1
      currentGeometryID = currentRange._2.geometryID
    }
    while (intersections.hasNext || currentRange != null) {
      if (fromBeginning) {
        while (currentTile == null || currentTile._1 < currentTileID) {
          assert(tiles.hasNext, "Could not locate a tile that has intersections")
          currentTile = tiles.next()
          if (numTiles != null)
            numTiles.add(1)
        }
        fromBeginning = false
        maskTile = new MaskTile[T](currentTile._2)
      }
      for (i <- currentRange._2.x1 to currentRange._2.x2) {
        try {
          maskTile.setMask(i, currentRange._2.y)
        } catch {
          case _: IndexOutOfBoundsException =>
            val a = s"\nmaskTile: ${maskTile.tile.tileID}; currentRange: ${currentRange._1}\n"
            logInfo(a)
            val b = s"maskTile Height && Width: ${maskTile.tileHeight}, ${maskTile.tileWidth}; currentRange Row && Column: ${currentRange._2.y - maskTile.y1}, ${i - maskTile.x1}\n"
            logInfo(b)
            val c = s"maskTile Mask Length: ${maskTile.mask.length * 64}; pixelOffset: ${maskTile.getPixelOffset(i, currentRange._2.y)}\n"
            logInfo(c)
            throw new IndexOutOfBoundsException(a + b + c)
        }
      }
      if (numRanges != null)
        numRanges.add(1)
      currentRange = if (intersections.hasNext) intersections.next() else null
      if (currentRange == null) {
        return RaptorJoinResultTile(currentGeometryID, maskTile)
      }
      if (currentRange._1 != currentTileID || currentRange._2.geometryID != currentGeometryID) {
        val geometryID = currentGeometryID
        currentTileID = currentRange._1
        currentGeometryID = currentRange._2.geometryID
        fromBeginning = true
        return RaptorJoinResultTile(geometryID, maskTile)
      }
    }
    null
  }

  nextTuple = prefetchNext

  override def hasNext: Boolean = nextTuple != null

  override def next: RaptorJoinResultTile[T] = {
    val toReturn = nextTuple
    nextTuple = prefetchNext
    toReturn
  }

}
