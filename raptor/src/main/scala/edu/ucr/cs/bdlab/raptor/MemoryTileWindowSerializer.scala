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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}

class MemoryTileWindowSerializer[T] extends Serializer[MemoryTileWindow[T]] {

  override def write(kryo: Kryo, output: Output, tile: MemoryTileWindow[T]): Unit = {
    output.writeInt(tile.numValues)
    for (i <- 0 until tile.numValues)
      kryo.writeObject(output, tile.tiles(i))
  }

  override def read(kryo: Kryo, input: Input, klass: Class[MemoryTileWindow[T]]): MemoryTileWindow[T] = {
    val numValues = input.readInt()
    val tiles = new Array[MemoryTile[T]](numValues)
    for (i <- 0 until numValues)
      tiles(i) = kryo.readObject(input, classOf[MemoryTile[T]])
    MemoryTileWindow.create(tiles)
  }
}
