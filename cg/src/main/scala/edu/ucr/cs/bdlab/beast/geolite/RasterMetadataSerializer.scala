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
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import java.awt.geom.AffineTransform

/**
 * A Kryo serializer for [[RasterMetadata]]
 */
class RasterMetadataSerializer extends Serializer[RasterMetadata] {
  override def write(kryo: Kryo, output: Output, metadata: RasterMetadata): Unit = {
    output.writeInt(metadata.x1)
    output.writeInt(metadata.y1)
    output.writeInt(metadata.x2)
    output.writeInt(metadata.y2)
    output.writeInt(metadata.tileWidth)
    output.writeInt(metadata.tileHeight)
    output.writeInt(metadata.srid)
    val matrix = new Array[Double](6)
    metadata.g2m.getMatrix(matrix)
    output.writeDoubles(matrix)
  }

  override def read(kryo: Kryo, input: Input, klass: Class[RasterMetadata]): RasterMetadata = {
    val x1 = input.readInt()
    val y1 = input.readInt()
    val x2 = input.readInt()
    val y2 = input.readInt()
    val tileWidth = input.readInt()
    val tileHeight = input.readInt()
    val srid = input.readInt()
    val matrix = input.readDoubles(6)
    val g2m = new AffineTransform(matrix)
    new RasterMetadata(x1, y1, x2, y2, tileWidth, tileHeight, srid, g2m)
  }

  override def isImmutable: Boolean = true
}
