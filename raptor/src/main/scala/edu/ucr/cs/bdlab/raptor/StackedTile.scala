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

import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
 * A tile that consists of several tiles stacked on top of each other.
 * All the stacked tiles must have the same ID and the same metadata which this tile will inherit.
 */
class StackedTile[T](tiles: ITile[_]*)(implicit t: ClassTag[T])
  extends ITile[Array[T]](tiles.head.tileID, tiles.head.rasterMetadata) {
  // Make sure that all tiles have the same tile ID and metadata
  require(tiles.forall(_.tileID == tileID), "Cannot stack tiles with different IDs")
  require(tiles.forall(_.rasterMetadata == rasterMetadata), "Cannot stack tiles with different metadata")

  override def getPixelValue(x: Int, y: Int): Array[T] = {
    val outVal = new Array[T](numComponents)
    var i = 0
    tiles.foreach(tile => tile.getPixelValue(x, y) match {
      case vs: Array[_] =>
        for (v <- vs) {
          outVal(i) = v.asInstanceOf[T]
          i += 1
        }
      case v: Any =>
        outVal(i) = v.asInstanceOf[T]
        i += 1
    })
    outVal
  }

  /**
   * A pixel in a stacked tile is empty if any of the underlying tiles is empty
   * @param i the index of the column
   * @param j the index of the row
   *  @return `true` if this pixel is empty in any of the stacked tiles. `false` if at least one is empty.
   */
  override def isEmpty(i: Int, j: Int): Boolean = {
    for (t <- tiles)
      if (t.isEmpty(i, j))
        return true
    false
  }

  override val numComponents: Int = tiles.map(_.numComponents).sum

  override def componentType: DataType = tiles.head.componentType
}
