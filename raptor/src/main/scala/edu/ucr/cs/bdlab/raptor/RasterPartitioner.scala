package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.spark.Partitioner

/**
 * Divides the raster grid into partitions of contiguous tiles with roughly the same number of tiles in each partition
 * and roughly square-like partitions.
 * @param rasterMetadata the metadata that describes the raster space
 * @param numPartitions the desired number of partitions to divide the space in
 */
class RasterPartitioner(@transient rasterMetadata: RasterMetadata, override val numPartitions: Int) extends Partitioner {
  require(numPartitions > 0, s"Invalid number of partitions $numPartitions")

  /** Number of columns is ceil(raster-width / tile-width) */
  private val numColumns: Int = (rasterMetadata.rasterWidth + rasterMetadata.tileWidth - 1) / rasterMetadata.tileWidth

  /** Number of rows is ceil(raster-height / tile-height) */
  private val numRows: Int = (rasterMetadata.rasterHeight + rasterMetadata.tileHeight - 1) / rasterMetadata.tileHeight

  // Compute the partition width and height
  private val (partitionWidth: Int, partitionHeight: Int) = {
    // Number of tiles per row per partition
    var w: Int = 1
    // Number of tiles per column per partition
    var h: Int = 1
    while (((numColumns + w - 1) / w) * ((numRows + h - 1) / h) > numPartitions) {
      if (w < numColumns && w * rasterMetadata.tileWidth < h * rasterMetadata.tileHeight || h == numRows)
        w += 1
      else
        h += 1
    }
    (w, h)
  }

  private val numPartitionsPerRow: Int = (numColumns + partitionWidth - 1) / partitionWidth

  /**
   * Returns the partition of the given tileID
   * @param tile the tile ID in the input RasterMetadata to return its partition
   * @return the partition ID associated with the given tile
   */
  override def getPartition(tile: Any): Int = tile match {
    case tileID: Int =>
      if (tileID >= numColumns * numRows)
        return 0
      val tileX: Int = tileID % numColumns
      val tileY: Int = tileID / numColumns
      val parX: Int = tileX / partitionWidth
      val parY: Int = tileY / partitionHeight
      parY * numPartitionsPerRow + parX
    case t: ITile[_] => getPartition(t.tileID)
  }
}
