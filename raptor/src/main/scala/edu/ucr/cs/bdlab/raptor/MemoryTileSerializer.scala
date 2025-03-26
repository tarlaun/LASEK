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
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata

import scala.reflect.ClassTag

/**
 * A Kryo serialize/deserializer for [[MemoryTile]]
 */
class MemoryTileSerializer extends Serializer[MemoryTile[Any]] {

  override def write(kryo: Kryo, output: Output, tile: MemoryTile[Any]): Unit = {
    tile.compress
    output.writeInt(tile.tileID)
    kryo.writeObject(output, tile.rasterMetadata)
    output.writeInt(tile.componentTypeNumber)
    output.writeInt(tile.numComponents)
    for (l <- tile.pixelExists)
      output.writeLong(l)
    output.writeInt(tile.compression)
    output.writeInt(tile.data.length)
    output.writeLongs(tile.data)
  }

  override def read(kryo: Kryo, input: Input, klass: Class[MemoryTile[Any]]): MemoryTile[Any] = {
    // Read a tile and do not decompress immediately. Let it lazily decompress when needed
    val tileID: Int = input.readInt()
    val metadata = kryo.readObject(input, classOf[RasterMetadata])
    // Note: We use [Byte] to make the code compile and run but we override the info in the next lines
    val tile = new MemoryTile[Byte](tileID, metadata)
    tile.componentTypeNumber = input.readInt()
    tile.bitsPerComponent = MemoryTile.NumberOfBitsPerType(tile.componentTypeNumber)
    tile.numComponents = input.readInt()
    for (i <- tile.pixelExists.indices)
      tile.pixelExists(i) = input.readLong()
    tile.compression = input.readInt()
    tile.data = input.readLongs(input.readInt())
    tile.asInstanceOf[MemoryTile[Any]]
  }
}
