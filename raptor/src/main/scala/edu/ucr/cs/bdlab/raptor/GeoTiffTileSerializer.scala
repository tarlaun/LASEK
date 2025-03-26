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
import edu.ucr.cs.bdlab.beast.geolite.ITileSerializer.{numberToSparkType, sparkTypeToNumber}
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.beast.io.tiff.{AbstractTiffTile, CompressedTiffTile}
import edu.ucr.cs.bdlab.beast.util.{BitArray, LZWCodec, LZWOutputStream}
import org.apache.commons.io.output.ByteArrayOutputStream

import java.io.DataOutputStream

/**
 * A Kryo serializer for GeoTiffTile. It avoids compressing the tile if it's already compressed and can be
 * more efficient that looping over all pixels.
 */
class GeoTiffTileSerializer[T] extends Serializer[AbstractGeoTiffTile[T]]{
  override def write(kryo: Kryo, output: Output, tile: AbstractGeoTiffTile[T]): Unit = {
    // compress the underlying TiffTile first
    // Write the tiff tile
    kryo.writeObject(output, tile.tiffTile)
    // Write the geotiff information
    output.writeInt(tile.tileID)
    kryo.writeObject(output, tile.rasterMetadata)
    output.writeInt(tile.fillValue)
    if (tile.pixelBitMask != null) {
      output.writeInt(tile.pixelBitMask.length)
      output.writeBytes(tile.pixelBitMask)
    } else {
      output.writeInt(0)
    }
  }

  override def read(kryo: Kryo, input: Input, tileClass: Class[AbstractGeoTiffTile[T]]): AbstractGeoTiffTile[T] = {
    val tiffTile = kryo.readObject(input, classOf[AbstractTiffTile])
    val tileID = input.readInt()
    val metadata = kryo.readObject(input, classOf[RasterMetadata])
    val fillValue = input.readInt()
    val pixelBitMaskLength = input.readInt()
    val pixelBitMask: Array[Byte] = if (pixelBitMaskLength > 0) input.readBytes(pixelBitMaskLength) else null
    val t = if (tiffTile.getNumSamples == 1) {
      if (tiffTile.getSampleFormat(0) == 1 || tiffTile.getSampleFormat(0) == 2)
        new GeoTiffTileInt(tileID, tiffTile, fillValue, pixelBitMask, metadata)
      else if (tiffTile.getSampleFormat(0) == 3 && tiffTile.getBitsPerSample(0) == 32)
        new GeoTiffTileFloat(tileID, tiffTile, fillValue, pixelBitMask, metadata)
      else if (tiffTile.getSampleFormat(0) == 3 && tiffTile.getBitsPerSample(0) == 64)
        new GeoTiffTileDouble(tileID, tiffTile, fillValue, pixelBitMask, metadata)
    } else {
      // Array
      if (tiffTile.getSampleFormat(0) == 1 || tiffTile.getSampleFormat(0) == 2)
        new GeoTiffTileIntArray(tileID, tiffTile, fillValue, pixelBitMask, metadata)
      else if (tiffTile.getSampleFormat(0) == 3 && tiffTile.getBitsPerSample(0) == 32)
        new GeoTiffTileFloatArray(tileID, tiffTile, fillValue, pixelBitMask, metadata)
      else if (tiffTile.getSampleFormat(0) == 3 && tiffTile.getBitsPerSample(0) == 64)
        new GeoTiffTileDoubleArray(tileID, tiffTile, fillValue, pixelBitMask, metadata)
    }
    t.asInstanceOf[AbstractGeoTiffTile[T]]
  }
}

