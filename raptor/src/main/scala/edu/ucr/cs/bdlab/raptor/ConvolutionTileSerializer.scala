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
import edu.ucr.cs.bdlab.beast.util.BitArray

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.{DeflaterOutputStream, InflaterInputStream}

/**
 * A Kryo serialize/deserializer for [[ConvolutionTileSingleBand]] and [[ConvolutionTileMultiBand]]
 */
class ConvolutionTileSerializer extends Serializer[AbstractConvolutionTile[Any]] {

  override def write(kryo: Kryo, output: Output, tile: AbstractConvolutionTile[Any]): Unit = {
    output.writeInt(tile.tileID)
    kryo.writeObject(output, tile.rasterMetadata)
    output.writeInt(tile.numComponents)
    output.writeInt(tile.w)
    output.writeFloats(tile.weights)
    output.writeInt(tile.sourceTileID)
    val compressedOut = new ByteArrayOutputStream()
    val dataOut = new ObjectOutputStream(new DeflaterOutputStream(compressedOut))
    tile.pixelExists.write(dataOut)
    dataOut.writeObject(tile.pixelValues)
    dataOut.close()
    val compressedBytes = compressedOut.toByteArray
    output.writeInt(compressedBytes.length)
    output.write(compressedBytes)
  }

  override def read(kryo: Kryo, input: Input, klass: Class[AbstractConvolutionTile[Any]]): AbstractConvolutionTile[Any] = {
    // Read a tile and do not decompress immediately. Let it lazily decompress when needed
    val tileID: Int = input.readInt()
    val metadata = kryo.readObject(input, classOf[RasterMetadata])
    val numComponents = input.readInt()
    val w = input.readInt()
    val weights: Array[Float] = input.readFloats((2 * w + 1) * (2 * w + 1))
    val sourceTileID: Int = input.readInt()
    val compressedArraySize = input.readInt()
    val compressedData = new Array[Byte](compressedArraySize)
    input.read(compressedData)
    val dataIn = new ObjectInputStream(new InflaterInputStream(new ByteArrayInputStream(compressedData)))
    val pixelExists = new BitArray()
    pixelExists.readFields(dataIn)
    val pixelValues: Array[Float] = dataIn.readObject().asInstanceOf[Array[Float]]
    dataIn.close()
    val t = if (numComponents == 1)
      new ConvolutionTileSingleBand(tileID, metadata, w, weights, sourceTileID, pixelValues, pixelExists)
    else
      new ConvolutionTileMultiBand(tileID, metadata, numComponents, w, weights, sourceTileID, pixelValues, pixelExists)
    t.asInstanceOf[AbstractConvolutionTile[Any]]
  }
}
