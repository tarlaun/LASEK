/*
 * Copyright 2022 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata

import scala.reflect.ClassTag

/**
 * A memory tiles that is used for window operations. Each pixel in this tile stores up-to `n` values.
 * Each value is at a specific position. If the value is absent, a corresponding bit is unset for that value.
 * @param tileID the ID of the tile
 * @param metadata the raster metadata
 * @param numValues how many values are stored per pixel
 * @param tiles The actual data is stored in separate tiles. This allows us to store any type of value even if T is an array
 * @tparam T the type of measurement values in tiles
 */
@DefaultSerializer(classOf[MemoryTileWindowSerializer[Any]])
protected class MemoryTileWindow[T](val tileID: Int, val metadata: RasterMetadata,
                                    private[raptor] val numValues: Int,
                                    private[raptor] val tiles: Array[MemoryTile[T]]) {

  def setValue(i: Int, j: Int, position: Int, value: T): Unit =
    tiles(position).setPixelValue(i, j, value)

  def getValue(i: Int, j: Int, position: Int): T = tiles(position).getPixelValue(i, j)

  /**
   * Returns `true` if this pixel is empty for all values.
   * @param i the column of the pixel
   * @param j the row of the pixel
   * @return `true` if all values are empty for the given pixel. `false` if at least one value existing
   */
  def isPixelEmpty(i: Int, j: Int): Boolean = {
    for (i <- 0 until numValues)
      if (tiles(i).isDefined(i, j))
        return false
    // All are emtpy, return true
    true
  }

  /**
   * Read all the values at the given pixel position and use them to fill the passed two arrays.
   * @param x the column of the pixel to read
   * @param y the row of the pixel to read
   * @param values the output array that will contain all values
   * @param defined the output array that will contain which values are defined (true)
   * @return the number of valid values that were filled in the array
   */
  def readValues(x: Int, y: Int, values: Array[T], defined: Array[Boolean]): Int = {
    assert(values.length == numValues)
    assert(defined.length == numValues)
    var n: Int = 0
    for (i <- 0 until numValues) {
      defined(i) = tiles(i).isDefined(x, y)
      if (defined(i)) {
        n += 1
        values(i) = tiles(i).getPixelValue(x, y)
      }
    }
    n
  }

  /**
   * Merge another window memory tile into this one. Takes any defined values in the other window memory tile
   * and merged it into this one.
   * @param another the other memory tile to merge with
   */
  def mergeWith(another: MemoryTileWindow[T]): Unit = {
    assert(another.tileID == this.tileID)
    for (i <- 0 until numValues; y <- tiles(0).y1 to tiles(0).y2; x <- tiles(0).x1 to tiles(0).x2) {
      if (another.tiles(i).isDefined(x, y)) {
        this.tiles(i).setPixelValue(x, y, another.tiles(i).getPixelValue(x, y))
      }
    }
  }
}

object MemoryTileWindow {
  def create[T](tiles: Array[MemoryTile[T]]): MemoryTileWindow[T] =
    new MemoryTileWindow[T](tiles(0).tileID, tiles(0).rasterMetadata, tiles.length, tiles)

  def create[T: ClassTag](tileID: Int, metadata: RasterMetadata, numValues: Int): MemoryTileWindow[T] = {
    val w = new MemoryTileWindow[T](tileID, metadata, numValues, new Array[MemoryTile[T]](numValues))
    for (i <- 0 until numValues)
      w.tiles(i) = new MemoryTile[T](tileID, metadata)
    w
  }

}