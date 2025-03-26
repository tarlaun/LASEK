package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.spark.util.LongAccumulator

/**
 * Converts a list of intersections and a list of tiles to a list of pixel values in the following format:
 * 1. Long: The feature (or geometry) ID
 * 2. RasterMetadata: the metadata of the raster file
 * 3. Int: x-coordinate
 * 4. Int: y-coordinate
 * 5. T: pixel value as provided in the raster data
 * Both inputs should be sorted on the key (RasterTileID) which is a concatenation of the raster file ID
 * in the most significant 32 bits and tile ID is the least significant 32 bits.
 * @param intersections an iterator of intersections in the following format
 *                      (RasterTileID, (GeometryID, Y, X1, X2))
 * @param tiles a list of (RasterTileID, ITile) pairs sorted by the key
 */
class PixelIterator3[T](intersections: Iterator[(Long, PixelRange)], tiles: Iterator[(Long, ITile[T])],
                        numTiles: LongAccumulator = null, numRanges: LongAccumulator = null)
                        extends Iterator[RaptorJoinResult[T]] {

  /**The tile currently being processed*/
  var currentTile: (Long, ITile[T]) = _

  /**The range that nextTuple is contained in*/
  var currentRange: (Long, PixelRange) = _

  /**The x-coordinate that nextTuple contains*/
  var x: Int = _

  /**The tuple that will be returned when next is called*/
  var nextTuple: RaptorJoinResult[T] = _

  /**A flag that is raised when end-of-file is reached*/
  var eof: Boolean = false

  /**
   * Prefetches the next tuple and returns it. If end-of-file is reached, this function will return null
   * @return the next record or null if end-of-file is reached
   */
  private def prefetchNext: RaptorJoinResult[T] = {
    while (!eof) {
      x += 1
      if (currentRange == null || x > currentRange._2.x2) {
        // First time or current range has ended, fetch next range
        if (!intersections.hasNext) {
          eof = true
          return null
        }
        if (numRanges != null)
          numRanges.add(1)
        currentRange = intersections.next()
        x = currentRange._2.x1
        while (currentTile == null || currentTile._1 < currentRange._1) {
          assert(tiles.hasNext, "Could not locate a tile that has intersections")
          currentTile = tiles.next()
          if (numTiles != null)
            numTiles.add(1)
        }
      }

      if (!currentTile._2.isEmpty(x, currentRange._2.y)) {
        // Found a valid pixel, return it
        val pixelValue: T = currentTile._2.getPixelValue(x, currentRange._2.y)
        // TODO the raster ID might not be correct if multiple files have the same rasterMetadata
        return RaptorJoinResult(currentRange._2.geometryID, currentTile._2.rasterMetadata, x, currentRange._2.y, pixelValue)
      }
    }
    // End-of-file reached
    null
  }

  nextTuple = prefetchNext

  override def hasNext: Boolean = nextTuple != null

  override def next: RaptorJoinResult[T] = {
    val toReturn = nextTuple
    nextTuple = prefetchNext
    toReturn
  }

}
