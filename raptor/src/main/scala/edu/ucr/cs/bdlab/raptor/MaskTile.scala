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
import edu.ucr.cs.bdlab.beast.geolite.{ITile, ITileSerializer}
import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.sql.types.DataType

/**
 * A tile that filters the values of another tile according to a filter function
 */
@DefaultSerializer(classOf[MaskTileSerializer[Any]])
class MaskTile[T](private[raptor] val tile: ITile[T], var mask: Array[Long] = null) extends ITile[T](tile.tileID, tile.rasterMetadata) {
  if (mask == null)
    mask = new Array[Long]((this.tileWidth * this.tileHeight + 63) / 64)

  /**
   * Computes the position of the pixel in the row-major order of pixels
   *
   * @param i the column of the pixel
   * @param j the row of the pixel
   * @return the position of this pixel among all pixels in this tile
   */
  @inline
  def getPixelOffset(i: Int, j: Int): Long = (j - y1) * this.tileWidth + (i - x1)

  def setMask(i: Int, j: Int): Unit = BitArray.setBit(mask, getPixelOffset(i, j))

  override def getPixelValue(i: Int, j: Int): T = tile.getPixelValue(i, j)

  override def isEmpty(i: Int, j: Int): Boolean = !BitArray.isBitSet(mask, getPixelOffset(i, j)) || tile.isEmpty(i, j)

  override def numComponents: Int = tile.numComponents

  override def componentType: DataType = tile.componentType
}
