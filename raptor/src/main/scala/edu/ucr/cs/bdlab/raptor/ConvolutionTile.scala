package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType}


/**
 * A tile that holds partial answer for the convolution operator
 * @param tileID the ID of this tile in the raster metadata
 * @param metadata the raster metadata that defines the underlying raster
 * @param w the size of the window in pixels
 * @param weights the array of weights in the window sorted in row-major order
 * @param sourceTile the first tile to add
 * @tparam T the type of measurement values in tiles
 */
@DefaultSerializer(classOf[ConvolutionTileSerializer])
abstract class AbstractConvolutionTile[T](tileID: Int, metadata: RasterMetadata, val w: Int, val weights: Array[Float],
                                          val sourceTileID: Int,
                                          private[raptor] var pixelValues: Array[Float] = null,
                                          private[raptor] var pixelExists: BitArray = null)
  extends ITile[T](tileID, metadata) {

  // To save memory, limit the scope of this tile to the affecting tile only
  override val x1: Int = metadata.getTileX1(tileID) max (metadata.getTileX1(sourceTileID) - w)
  override val y1: Int = metadata.getTileY1(tileID) max (metadata.getTileY1(sourceTileID) - w)
  override val x2: Int = metadata.getTileX2(tileID) min (metadata.getTileX2(sourceTileID) + w)
  override val y2: Int = metadata.getTileY2(tileID) min (metadata.getTileY2(sourceTileID) + w)
  override val tileWidth: Int = x2 - x1 + 1
  override val tileHeight: Int = y2 - y1 + 1

  assert(weights.length == (2 * w + 1) * (2 * w + 1),
    s"Invalid size for weights array ${weights.length} for window size w=$w! Should be ${(2 * w + 1) * (2 * w + 1)}")

  if (pixelValues == null) {
    pixelValues = new Array[Float](tileWidth * tileHeight * numComponents)
    pixelExists = new BitArray(tileWidth * tileHeight)
  }
  assert(pixelExists != null)

  override def isEmpty(i: Int, j: Int): Boolean = !pixelExists.get((j - y1) * tileWidth + (i - x1))

  private[raptor] def pixelOffset(i: Int, j: Int): Int = numComponents * ((j - y1) * tileWidth + (i - x1))

  /**
   * Adds the given input tile into this partial convolution tile
   * @param tile the tile to compute into this tile
   */
  private[raptor] def addTile[U](tile: ITile[U]): Unit = {
    assert(tile.tileID == sourceTileID)
    // Calculate the range of pixels in the given tile that would affect pixels in this tile
    val minX = (this.x1 - w) max tile.x1
    val minY = (this.y1 - w) max tile.y1
    val maxX = (this.x2 + w) min tile.x2
    val maxY = (this.y2 + w) min tile.y2
    // Loop over all pixels in the given tile that will be used
    for (x <- minX to maxX; y <- minY to maxY; if tile.isDefined(x, y)) {
      val pixelValue: Any = tile.getPixelValue(x, y)
      // Loop over all affected pixels in this tile
      for (ix <- ((x - w) max x1) to ((x + w) min x2); iy <- ((y - w) max y1) to ((y + w) min y2)) {
        val dx = ix - (x - w)
        val dy = iy - (y - w)
        val weight: Float = weights(dy * (2 * w + 1) + dx)
        updatePixel(ix, iy, weight, pixelValue)
      }
    }
  }

  /**
   * Merge another convolution tile into this tile
   * @param other the other convolution tile to merge
   * @return this tile after updating
   */
  def merge(other: AbstractConvolutionTile[T]): AbstractConvolutionTile[T] = {
    if (this.tileID != sourceTileID) {
      // This is a partial tile, need to create a full tile
      val fullTile: AbstractConvolutionTile[T] = if (numComponents == 1)
        new ConvolutionTileSingleBand(this.tileID, this.metadata, this.w, this.weights, this.tileID).asInstanceOf[AbstractConvolutionTile[T]]
      else
        new ConvolutionTileMultiBand(this.tileID, this.metadata, this.numComponents, this.w, this.weights, this.tileID).asInstanceOf[AbstractConvolutionTile[T]]
      fullTile.merge(this)
      fullTile.merge(other)
      fullTile
    } else if (other.tileID != other.sourceTileID) {
      for (x <- (this.x1 max other.x1) to (this.x2 min other.x2); y <- (this.y1 max other.y1) to (this.y2 min other.y2); if other.isDefined(x, y))
        updatePixel(x, y, 1.0f, other.getPixelValue(x, y))
      this
    } else {
      for (i <- 0 until pixelValues.length)
        pixelValues(i) += other.pixelValues(i)
      this.pixelExists.inplaceOr(other.pixelExists)
      this
    }
  }

  /**
   * Update the pixel value in this tile by adding weight * T
   * @param x the location of the pixel to update
   * @param y the location of the pixel to update
   * @param weight the weight to be multiplied by the given value
   * @param value the value to add
   */
  protected def updatePixel(x: Int, y: Int, weight: Float, value: Any): Unit

  /**
   * The type of each component in the data. Since convolution requires computing weighted average, we assume that
   * the average is always a floating point number.
   * @return
   */
  override def componentType: DataType = FloatType
}

@DefaultSerializer(classOf[ConvolutionTileSerializer])
class ConvolutionTileSingleBand(tileID: Int, metadata: RasterMetadata, w: Int, weights: Array[Float],
                                stid: Int,
                               @transient val values: Array[Float] = null, @transient val exists: BitArray = null)
  extends AbstractConvolutionTile[Float](tileID, metadata, w, weights, stid, values, exists) {

  override protected def updatePixel(x: Int, y: Int, weight: Float, value: Any): Unit = {
    val offset = pixelOffset(x, y)
    pixelExists.set(offset, true)
    value match {
      case x: Byte => pixelValues(offset) += weight * x
      case x: Short => pixelValues(offset) += weight * x
      case x: Int => pixelValues(offset) += weight * x
      case x: Long => pixelValues(offset) += weight * x
      case x: Float => pixelValues(offset) += weight * x
      case x: Double => pixelValues(offset) += (weight * x).toFloat
    }
  }

  override def getPixelValue(i: Int, j: Int): Float = pixelValues(pixelOffset(i, j))

  override def numComponents: Int = 1
}

@DefaultSerializer(classOf[ConvolutionTileSerializer])
class ConvolutionTileMultiBand(tileID: Int, metadata: RasterMetadata, override val numComponents: Int,
                               w: Int, weights: Array[Float], stid: Int,
                               @transient val values: Array[Float] = null,
                               @transient val exists: BitArray = null)
  extends AbstractConvolutionTile[Array[Float]](tileID, metadata, w, weights, stid, values, exists) {

  override protected def updatePixel(x: Int, y: Int, weight: Float, value: Any): Unit = {
    val offset = pixelOffset(x, y)
    pixelExists.set(offset / numComponents, true)
    value match {
      case x: Array[Byte] => for (b <- 0 until numComponents) pixelValues(offset + b) += weight * x(b)
      case x: Array[Short] => for (b <- 0 until numComponents) pixelValues(offset + b) += weight * x(b)
      case x: Array[Int] => for (b <- 0 until numComponents) pixelValues(offset + b) += weight * x(b)
      case x: Array[Long] => for (b <- 0 until numComponents) pixelValues(offset + b) += weight * x(b)
      case x: Array[Float] => for (b <- 0 until numComponents) pixelValues(offset + b) += weight * x(b)
      case x: Array[Double] => for (b <- 0 until numComponents) pixelValues(offset + b) += (weight * x(b)).toFloat
    }
  }

  override def getPixelValue(i: Int, j: Int): Array[Float] = {
    val x = new Array[Float](numComponents)
    val offset = pixelOffset(i, j)
    for (b <- 0 until numComponents)
      x(b) = pixelValues(offset + b)
    x
  }
}