package edu.ucr.cs.bdlab.beast.satex

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.raptor.MemoryTile
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

import java.awt.geom.AffineTransform
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Convert raptor join iterator into image iterator.
 * NOTE that the join result is repartitioned and sorted.
 * So we just go thru the iterator, once we found the new geometry id, we put the tiles into image and return the image.
 * The tile here is MaskTile, which has a function IsEmpty to check if one pixel is empty.
 *
 * @param raptorResults an iterator of raptor join result in order in the following format
 *                      (GeometryID, ITile[T])
 */
class ImageIterator[T](raptorResults: Iterator[(Long, ITile[T])],
                       numCrossTiff: LongAccumulator,
                       numTooManyTiles: LongAccumulator,
                       numTooLessPixels: LongAccumulator)
                      (implicit @transient t: ClassTag[T])
  extends Iterator[(Long, ITile[T])] with Logging {

  /** The current geometry ID */
  var currentGeometryID: Long = -1

  var currentTuple: (Long, ITile[T]) = _

  var currentTileArray: ArrayBuffer[ITile[T]] = _
  var currentPixelNum: Int = 0
  val maxTileArraySize: Int = 100
  val minPixelNum: Int = 20

  var currentMinX: Int = Int.MaxValue
  var currentMinY: Int = Int.MaxValue
  var currentMaxX: Int = Int.MinValue
  var currentMaxY: Int = Int.MinValue
  var currentImage: (Long, ITile[T]) = _
  var ret: Boolean = false

  /** The tuple that will be returned when next is called */
  var nextTuple: (Long, ITile[T]) = _

  private def clearData(): Unit = {
    currentGeometryID = currentTuple._1
    currentTileArray.clear()
    currentMinX = Int.MaxValue
    currentMinY = Int.MaxValue
    currentMaxX = Int.MinValue
    currentMaxY = Int.MinValue
    currentPixelNum = 0
  }

  private def image(): Unit = {
    // if the number of pixels is below minPixelNum, we dispose the geometry because the image will not be meaningful
    if (currentPixelNum < minPixelNum) {
      numTooLessPixels.add(1)
      // logWarning(currentGeometryID + " has " + currentPixelNum + " pixels, which is less than " + minPixelNum)
      clearData()
      return
    }
    //val image = new BufferedImage(currentMaxX - currentMinX + 1, currentMaxY - currentMinY + 1, BufferedImage.TYPE_INT_ARGB)
    val outputTile = new MemoryTile[T](0, new RasterMetadata(0, 0, currentMaxX - currentMinX + 1, currentMaxY - currentMinY + 1,
      currentMaxX - currentMinX + 1, currentMaxY - currentMinY + 1, 0, new AffineTransform()))
    for (tile <- currentTileArray) {
      for ((x, y, value) <- tile.pixels) {
        outputTile.setPixelValue(x - currentMinX, y - currentMinY, value)
      }
    }
    currentImage = (currentGeometryID, outputTile)
    clearData()
    ret = true
  }

  /**
   * Prefetches the next image and returns it. If end-of-file is reached, this function will return null
   *
   * @return the next record or null if end-of-file is reached
   */
  private def prefetchNext: (Long, ITile[T]) = {
    if (!raptorResults.hasNext) return null
    ret = false
    while (raptorResults.hasNext) {
      currentTuple = raptorResults.next()
      if (currentTuple._1 != currentGeometryID && currentGeometryID != -1) {
        image()
      } else if (currentGeometryID == -1) {
        currentGeometryID = currentTuple._1
        currentTileArray = new ArrayBuffer[ITile[T]]()
        // we need to set a maxTileArraySize threshold. If the size go beyond this threshold, we dispose the geometry
      } else if (currentTileArray.size == maxTileArraySize ||
        // if there are two different raster files on the geometry, we could not use this iterator.
        // We should use MBR instead
        !currentTuple._2.rasterMetadata.equals(currentTileArray.last.rasterMetadata)) {
        var currentTileArraySize = maxTileArraySize
        while (raptorResults.hasNext && currentTuple._1 == currentGeometryID) {
          currentTuple = raptorResults.next()
          currentTileArraySize += 1
        }
        if (currentTileArray.size == maxTileArraySize) {
          numTooManyTiles.add(1)
          // logWarning(currentGeometryID + " has " + currentTileArraySize + " tiles, which is more than " + maxTileArraySize)
        } else {
          numCrossTiff.add(1)
          // logWarning(currentGeometryID + " overlaps with 2 different raster files")
        }
        currentImage = (currentGeometryID, null)
        clearData()
        ret = true
      }

      currentTileArray += currentTuple._2
      for (x <- currentTuple._2.x1 to currentTuple._2.x2; y <- currentTuple._2.y1 to currentTuple._2.y2) {
        if (!currentTuple._2.isEmpty(x, y)) {
          currentPixelNum += 1
          currentMinX = Math.min(currentMinX, x)
          currentMaxX = Math.max(currentMaxX, x)
          currentMinY = Math.min(currentMinY, y)
          currentMaxY = Math.max(currentMaxY, y)
        }
      }

      if (ret) return currentImage
    }
    image()
    if (ret) currentImage else null
  }

  nextTuple = prefetchNext

  override def hasNext: Boolean = nextTuple != null

  override def next: (Long, ITile[T]) = {
    val toReturn = nextTuple
    nextTuple = prefetchNext
    toReturn
  }

}
