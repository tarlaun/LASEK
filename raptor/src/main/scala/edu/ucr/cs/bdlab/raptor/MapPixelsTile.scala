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
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 * A tile that maps the underlying pixels of a tile using the provided map function
 * @param tile the underlying tile to read the pixels from
 * @param mapFunction the function that maps input pixels to output pixels
 * @tparam T the type of measurement values in the input tile
 * @tparam U the type of measurement values in the output tile
 */
@DefaultSerializer(classOf[ITileSerializer[Any]])
class MapPixelsTile[T, U: ClassTag](tile: ITile[T], mapFunction: T => U)
                                   (implicit u: ClassTag[U])
  extends ITile[U](tile.tileID, tile.rasterMetadata) {

  override def getPixelValue(i: Int, j: Int): U = mapFunction(tile.getPixelValue(i, j))

  override def isEmpty(i: Int, j: Int): Boolean = tile.isEmpty(i, j)

  override val numComponents: Int =
    if (!u.runtimeClass.isArray) {
      // No array. A single component
      1
    } else {
      // An array, can only determine the number of components for a given value
      var n = -1
      var y = y1
      while (n == -1 && y < y2) {
        var x = x1
        while (n == -1 && x < x2) {
          if (isDefined(x, y)) {
            val value = getPixelValue(x, y)
            n = java.lang.reflect.Array.getLength(value)
          }
          x += 1
        }
        y += 1
      }
      if (n == -1) {
        // This only happens if all values are empty. No way to know the size then so we just use 3
        // We don't use 1 since this will cause the return type to be a single value instead of array
        n = 3
      }
      n
    }

  val componentTypeNumber: Int = MemoryTile.inferComponentType(u.runtimeClass)

  override val componentType: DataType = componentTypeNumber match {
      case 1 => ByteType
      case 2 => ShortType
      case 3 => IntegerType
      case 4 => LongType
      case 5 => FloatType
      case 6 => DoubleType
      case _ => throw new RuntimeException(s"Unrecognized component type ${componentType}")
    }
}
