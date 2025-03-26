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
import edu.ucr.cs.bdlab.beast.geolite.ITileSerializer.{numberToSparkType, sparkTypeToNumber}
import edu.ucr.cs.bdlab.beast.util.{BitArray, BitOutputStream, LZWCodec, LZWOutputStream}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType}

import java.io.DataOutputStream

/**
 * A Kryo serializer for all tiles.
 */
class ITileSerializer[T] extends Serializer[ITile[T]]{
  override def write(kryo: Kryo, output: Output, tile: ITile[T]): Unit = {
    // A general method to write any tile
    // Write tile identifier and metadata
    output.writeInt(tile.tileID)
    kryo.writeObject(output, tile.rasterMetadata)
    // Write pixel type and number of components
    output.writeInt(tile.numComponents)
    output.writeInt(sparkTypeToNumber(tile.componentType))
    val bytesPerPixel = tile.componentType.defaultSize * tile.numComponents
    // Write pixel data
    val pixelExists = new Array[Byte]((tile.tileWidth * tile.tileHeight + 7) / 8)

    val dummyData: Array[Byte] = Array.fill[Byte](bytesPerPixel)(0)
    val baos2 = new ByteArrayOutputStream()
    val zos2 = new LZWOutputStream(baos2)
    val pixelData = new DataOutputStream(zos2)
    val dataWriter: (DataOutputStream, T) => Unit = if (tile.numComponents == 1)
      ITileSerializer.dataWriters(sparkTypeToNumber(tile.componentType)).asInstanceOf[(DataOutputStream, T) => Unit]
    else
      ITileSerializer.dataWriters(0x10 | sparkTypeToNumber(tile.componentType)).asInstanceOf[(DataOutputStream, T) => Unit]

    var i = 0
    for (y <- tile.y1 to tile.y2; x <- tile.x1 to tile.x2) {
      if (tile.isDefined(x, y)) {
        BitArray.setBit(pixelExists, i)
        dataWriter(pixelData, tile.getPixelValue(x, y))
      } else {
        pixelData.write(dummyData)
      }
      i += 1
    }

    pixelData.close()

    val data1 = LZWCodec.encode(pixelExists)
    output.writeInt(data1.length)
    output.write(data1)
    val data2 = baos2.toByteArray
    output.writeInt(data2.length)
    output.write(data2)
  }

  override def read(kryo: Kryo, input: Input, tileClass: Class[ITile[T]]): ITile[T] = {
    val tileID = input.readInt()
    val metadata = kryo.readObject(input, classOf[RasterMetadata])
    val numComponents = input.readInt()
    val componentType = numberToSparkType(input.readInt())
    val pixelExistsSize = input.readInt()
    val pixelExists = input.readBytes(pixelExistsSize)
    val pixelDataSize = input.readInt()
    val pixelData = input.readBytes(pixelDataSize)
    new DefaultReadOnlyTile[T](tileID, metadata, numComponents, componentType,
      pixelExists, pixelData, DefaultReadOnlyTile.LZW)
  }
}

object ITileSerializer {
  val sparkTypeToNumber: Map[DataType, Int] = Map(
    (ByteType, 1), (ShortType, 2), (IntegerType, 3), (LongType, 4), (FloatType, 5), (DoubleType, 6)
  )

  val numberToSparkType: Array[DataType] = Array(
    NullType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType
  )

  val dataWriters: Array[(DataOutputStream, _<:Any) => Unit] = new Array(32)
  dataWriters(0x01) = (out: DataOutputStream, x: Byte) => out.writeByte(x)
  dataWriters(0x02) = (out: DataOutputStream, x: Short) => out.writeShort(x)
  dataWriters(0x03) = (out: DataOutputStream, x: Int) => out.writeInt(x)
  dataWriters(0x04) = (out: DataOutputStream, x: Long) => out.writeLong(x)
  dataWriters(0x05) = (out: DataOutputStream, x: Float) => out.writeFloat(x)
  dataWriters(0x06) = (out: DataOutputStream, x: Double) => out.writeDouble(x)
  dataWriters(0x11) = (out: DataOutputStream, xs: Array[Byte]) => out.write(xs)
  dataWriters(0x12) = (out: DataOutputStream, xs: Array[Short]) => for (x <- xs) out.writeShort(x)
  dataWriters(0x13) = (out: DataOutputStream, xs: Array[Int]) => for (x <- xs) out.writeInt(x)
  dataWriters(0x14) = (out: DataOutputStream, xs: Array[Long]) => for (x <- xs) out.writeLong(x)
  dataWriters(0x15) = (out: DataOutputStream, xs: Array[Float]) => for (x <- xs) out.writeFloat(x)
  dataWriters(0x16) = (out: DataOutputStream, xs: Array[Double]) => for (x <- xs) out.writeDouble(x)

}