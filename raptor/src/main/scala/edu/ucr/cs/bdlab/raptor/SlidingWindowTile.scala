package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}

import scala.reflect.ClassTag

class SlidingWindowTile[T, U](val tileID: Int, metadata: RasterMetadata, w: Int, winFunc: (Array[T], Array[Boolean]) => U)
                             (implicit t: ClassTag[T], u: ClassTag[U]) extends Serializable {

  assert(w <= metadata.tileWidth, s"Window size is too large $w > ${metadata.tileWidth} (tile width)")
  assert(w <= metadata.tileHeight, s"Window size is too large $w > ${metadata.tileHeight} (tile height)")

  def x1: Int = metadata.getTileX1(tileID)
  def y1: Int = metadata.getTileY1(tileID)
  def x2: Int = metadata.getTileX2(tileID)
  def y2: Int = metadata.getTileY2(tileID)

  /**
   * Maps each tile ID that might be relevant to computing this tile to a position in the array
   *
   * @return
   */
  val tileIDToArrayIndex: Map[Int, Int] = {
    val minX: Int = x1 - w
    val maxX: Int = x2 + w
    val minY: Int = y1 - w
    val maxY: Int = y2 + w
    var x = minX
    var y = minY
    var resultMap = Map[Int, Int]()
    while (y <= maxY && x <= maxX) {
      if (x >= metadata.x1 && x < metadata.x2 && y >= metadata.y1 && y < metadata.y2) {
        val tid = metadata.getTileIDAtPixel(x, y)
        resultMap = resultMap + ((tid, resultMap.size))
      }
      x = (x + metadata.tileWidth) / metadata.tileWidth * metadata.tileWidth
      if (x > maxX) {
        x = minX
        y = (y + metadata.tileHeight) / metadata.tileHeight * metadata.tileHeight
      }
    }
    resultMap
  }

  /** The list of all tiles that are used to calculate this tile */
  val inputTiles: Array[ITile[T]] = new Array[ITile[T]](tileIDToArrayIndex.size)

  var _resultTile: ITile[U] = _

  def addTile(tile: ITile[T]): Unit = {
    assert(tileIDToArrayIndex.contains(tile.tileID), s"Unexpected tile #${tile.tileID} while computing tile #$tileID")
    assert(inputTiles(tileIDToArrayIndex(tile.tileID)) == null, s"Duplicate entry for tile #${tile.tileID}")
    assert(_resultTile == null, "Cannot add tiles after computing the final tile")
    inputTiles(tileIDToArrayIndex(tile.tileID)) = tile
    if (!inputTiles.contains(null)) {
      // Already got all expected tiles, do the computation
      computeResultTile()
      // Remove input tiles to reduce memory footprint and speed up serialization
      for (i <- inputTiles.indices)
        inputTiles(i) = null
    }
  }

  def merge(other: SlidingWindowTile[T, U]): SlidingWindowTile[T, U] = {
    for (t <- other.inputTiles; if t != null)
      addTile(t)
    this
  }

  def resultTile: ITile[U] = {
    computeResultTile()
    _resultTile
  }

  private def computeResultTile(): Unit = {
    if (_resultTile != null)
      return
    val tile = new MemoryTile[U](tileID, metadata)
    val pixelValue: Array[T] = new Array[T]((2 * w + 1) * (2 * w + 1))
    val pixelDefined: Array[Boolean] = new Array[Boolean]((2 * w + 1) * (2 * w + 1))
    def computePixel(x: Int, y: Int): Unit = {
      var i: Int = 0
      for (iy <- y - w to y + w; ix <- x - w to x + w) {
        pixelDefined(i) = isPixelDefined(ix, iy)
        if (pixelDefined(i))
          pixelValue(i) = getPixel(ix, iy)
        i += 1
      }
      if (pixelDefined.contains(true)) {
        // At least one value is defined
        val resultValue: U = winFunc(pixelValue, pixelDefined)
        tile.setPixelValue(x, y, resultValue)
      }
    }
    // Handle the first w rows and the last w rows using the inefficient method
    for (x <- x1 to x2) {
      for (y <- y1 until y1 + w)
        computePixel(x, y)
      for (y <- y2 - w + 1 to y2)
        computePixel(x, y)
    }
    // Handle the first w columns and the last w columns using the inefficient method
    for (y <- y1 + w to y2 - w) {
      for (x <- x1 until x1 + w)
        computePixel(x, y)
      for (x <- x2 - w + 1 to x2)
        computePixel(x, y)
    }
    // Finally, compute all pixels that are in the core tile efficiently
    val coreTile = inputTiles(tileIDToArrayIndex(tileID))
    if (coreTile != null) {
      for (y <- y1 + w to y2 - w; x <- x1 + w to x2 - w) {
        var i: Int = 0
        for (iy <- y - w to y + w; ix <- x - w to x + w) {
          pixelDefined(i) = coreTile.isDefined(ix, iy)
          if (pixelDefined(i))
            pixelValue(i) = coreTile.getPixelValue(ix, iy)
          i += 1
        }
        if (pixelDefined.contains(true)) {
          // At least one value is defined
          val resultValue: U = winFunc(pixelValue, pixelDefined)
          tile.setPixelValue(x, y, resultValue)
        }
      }
    }
    _resultTile = tile
  }

  /**
   * Tells if the input pixel at the given location is defined in one of the input tiles
   * @param i the index of the column in the raster
   * @param j the index of the row in the raster
   * @return true iff the pixel at (i,j) in the input raster is defined.
   */
  private def isPixelDefined(i: Int, j: Int): Boolean = {
    if (i < metadata.x1 || i >= metadata.x2 || j < metadata.y1 || j >= metadata.y2)
      return false
    val tileID = metadata.getTileIDAtPixel(i, j)
    val tile = inputTiles(tileIDToArrayIndex(tileID))
    tile != null && tile.isDefined(i, j)
  }

  /**
   * Returns the value of the input pixel at position (i, j).
   * This function should only be called when the pixel is defined according to [[isPixelDefined(Int, Int)]]
   * otherwise, the return value is undefined.
   *
   * @param i the index of the column in the raster
   * @param j the index of the row in the raster
   * @return the value of the given pixel in the input raster
   */
  private def getPixel(i: Int, j: Int): T = {
    val tileID = metadata.getTileIDAtPixel(i, j)
    inputTiles(tileIDToArrayIndex(tileID)).getPixelValue(i, j)
  }
}
