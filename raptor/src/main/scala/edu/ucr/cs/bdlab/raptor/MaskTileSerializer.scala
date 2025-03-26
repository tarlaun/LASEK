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
import edu.ucr.cs.bdlab.beast.geolite.ITile

/**
 * A Kryo serializer for GeoTiffTile. It avoids compressing the tile if it's already compressed and can be
 * more efficient that looping over all pixels.
 */
class MaskTileSerializer[T] extends Serializer[MaskTile[T]] {
  override def write(kryo: Kryo, output: Output, tile: MaskTile[T]): Unit = {
    kryo.writeClassAndObject(output, tile.tile)
    output.writeLongs(tile.mask)
  }

  override def read(kryo: Kryo, input: Input, tileClass: Class[MaskTile[T]]): MaskTile[T] = {
    val tile: ITile[T] = kryo.readClassAndObject(input).asInstanceOf[ITile[T]]
    val mask = input.readLongs((tile.tileWidth * tile.tileHeight + 63) / 64)
    new MaskTile[T](tile, mask)
  }
}

