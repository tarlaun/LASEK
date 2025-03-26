package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.hadoop.fs.Path
import org.apache.spark.util.LongAccumulator


/**
 * Converts a list of intersections and a list of raster files names to a list of pixel values in the following format:
 * 1. Long: The feature (or geometry) ID
 * 2. Int: the raster file ID
 * 3. Int: x-coordinate
 * 4. Int: y-coordinate
 * 5. T: pixel value as provided in the raster data
 * @param intersections an iterator of intersections in the following format
 *                      (FileID_TileID, (GeometryID, Y, X1, X2))
 * @param rasterFiles a list of raster file names *
 */
class PixelIterator[T](intersections: Iterator[(Long, PixelRange)], rasterFiles: Array[String], rasterLayer: String,
                    numTiles: LongAccumulator = null) extends Iterator[RaptorJoinResult[T]]{

  /**The raster file that nextTuple points to*/
  var currentRasterFileID: Int = -1

  /**The raster file that is currently being read*/
  var rasterFile: IRasterReader[T] = _

  /**The tile ID for the range currently being processed*/
  var currentTileID: Int = -1

  /**The tile currently being processed*/
  var currentTile: ITile[T] = _

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
        currentRange = intersections.next()
        x = currentRange._2.x1
        // Open the raster file if needed
        val newRasterFileID: Int = (currentRange._1 >>> 32).toInt
        val tileID: Int = (currentRange._1 & 0xffffffffL).toInt
        if (currentRasterFileID != newRasterFileID) {
          currentRasterFileID = newRasterFileID
          // Open a new raster file
          val conf_file = new BeastOptions()
          if (rasterLayer != null)
            conf_file.set(IRasterReader.RasterLayerID, rasterLayer)
          val rasterPath = new Path(rasterFiles(currentRasterFileID))
          if (rasterFile != null)
            rasterFile.close()
          rasterFile = RasterHelper.createRasterReader[T](rasterPath.getFileSystem(conf_file.loadIntoHadoopConf(null)), rasterPath, conf_file)
        }
        if (currentTileID != tileID) {
          currentTile = rasterFile.readTile(tileID)
          currentTileID = tileID
          if (numTiles != null)
            numTiles.add(1)
        }
      }

      if (!currentTile.isEmpty(x, currentRange._2.y)) {
        // Found a valid pixel, return it
        val pixelValue: T = currentTile.getPixelValue(x, currentRange._2.y)
        return RaptorJoinResult(currentRange._2.geometryID, currentTile.rasterMetadata, x, currentRange._2.y, pixelValue)
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
