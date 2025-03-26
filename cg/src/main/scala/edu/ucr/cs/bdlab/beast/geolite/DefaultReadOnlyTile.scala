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
import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.DefaultReadOnlyTile.{LZW, sparkTypeToJavaType, valueReaders}
import edu.ucr.cs.bdlab.beast.util.{BitArray, LZWCodec}
import org.apache.spark.sql.types._

import java.util

/**
 * A simple read-only tile that initially has its data compressed.
 * The data is lazily decompressed when accessed for the first time.
 * @param tileID the ID of this tile in the raster metadata
 * @param metadata the metadata of the underlying raster
 * @param numComponents the number of components in each pixel
 * @param pixelType the type of data stored for each pixel
 * @param pixelExists a bit mask for which pixels exist
 * @param pixelData a byte array that contain the data
 * @param compression the type of compression used for [[pixelExists]] and [[pixelData]]
 * @tparam T the type of measurement values in tiles
 */
@DefaultSerializer(classOf[ITileSerializer[Any]])
class DefaultReadOnlyTile[T](tileID: Int, metadata: RasterMetadata, override val numComponents: Int,
                             override val componentType: DataType, var pixelExists: Array[Byte],
                             var pixelData: Array[Byte], compression: Int)
  extends ITile[T](tileID, metadata) {

  // Duplicate compression to store which array is decompressed separately
  var pixelExistsCompression: Int = compression
  var pixelDataCompression: Int = compression

  override def isEmpty(i: Int, j: Int): Boolean = {
    decompressPixelExists
    !BitArray.isBitSet(pixelExists, pixelOffset(i, j))
  }

  private def pixelOffset(i: Int, j: Int) = (j - y1) * tileWidth + (i - x1)

  override def getPixelValue(i: Int, j: Int): T = {
    decompressPixelData
    val offset = pixelOffset(i, j) * bytesPerPixel
    if (numComponents == 1)
      valueReader(pixelData, offset)
    else {
      val value: Array[_] = pixelValueTemplate.asInstanceOf[Array[_]].clone()
      for (i <- 0 until numComponents) {
        java.lang.reflect.Array.set(value, i, valueReader(pixelData, offset + i * componentType.defaultSize))
      }
      value.asInstanceOf[T]
    }
  }

  private def decompressPixelExists: Unit = {
    if (pixelExistsCompression == 0) return
    pixelExistsCompression match {
      case LZW => pixelExists = LZWCodec.decode(pixelExists, (numPixels + 7) / 8, true)
    }
    // Zero the pixel exists side of the compression
    pixelExistsCompression = 0
  }

  private def decompressPixelData: Unit = {
    if (pixelDataCompression == 0)
      return
    pixelDataCompression match {
      case LZW => pixelData = LZWCodec.decode(pixelData, 0, false)
    }
    pixelDataCompression = 0
  }

  lazy val valueReader: (Array[Byte], Int) => T = valueReaders(componentType).asInstanceOf[(Array[Byte], Int) => T]

  /**
   * Number of pixels in the tile including possible empty pixels
   * @return
   */
  private def numPixels: Int = tileWidth * tileHeight

  private lazy val bytesPerPixel: Int = numComponents * componentType.defaultSize

  /**
   * Create and store an array that holds a pixel value.
   * This method should only be called if [[numComponents]] is greater than one
   * @return a primitive array that can store a single pixel value
   */
  private lazy val pixelValueTemplate = {
    assert(numComponents > 1, "The method createPixelValue should only be called when the value is of type array")
    java.lang.reflect.Array.newInstance(componentJavaType, numComponents)
  }

  lazy val componentJavaType: Class[_] = sparkTypeToJavaType(componentType)
}

object DefaultReadOnlyTile {
  val LZW = 1

  val valueReaders: Map[DataType, (Array[Byte], Int) => Any] = Map(
    (ByteType, (data, offset) => data(offset)),
    (ShortType, (data, offset) => {
      (((data(offset).toShort & 0xff) << 8) | (data(offset + 1) & 0xff)).toShort
    }),
    (IntegerType, (data: Array[Byte], offset: Int) => {
      val b1: Int = data(offset).toInt & 0xff
      val b2: Int = data(offset+1).toInt & 0xff
      val b3: Int = data(offset+2).toInt & 0xff
      val b4: Int = data(offset+3).toInt & 0xff
      (b1 << 24) | (b2 << 16) | (b3 << 8) | b4
    }),
    (LongType, (data: Array[Byte], offset: Int) => {
      val b1: Long = data(offset+0) & 0xffL
      val b2: Long = data(offset+1) & 0xffL
      val b3: Long = data(offset+2) & 0xffL
      val b4: Long = data(offset+3) & 0xffL
      val b5: Long = data(offset+4) & 0xffL
      val b6: Long = data(offset+5) & 0xffL
      val b7: Long = data(offset+6) & 0xffL
      val b8: Long = data(offset+7) & 0xffL
      (b1 << 54) | (b2 << 48) | (b3 << 40) | (b4 << 32) | (b5 << 24) | (b6 << 16) | (b7 << 8) | b8
    }),
    (FloatType, (data: Array[Byte], offset: Int) => {
      val b1: Int = data(offset).toInt & 0xff
      val b2: Int = data(offset+1).toInt & 0xff
      val b3: Int = data(offset+2).toInt & 0xff
      val b4: Int = data(offset+3).toInt & 0xff
      java.lang.Float.intBitsToFloat((b1 << 24) | (b2 << 16) | (b3 << 8) | b4)
    }),
    (DoubleType, (data: Array[Byte], offset: Int) => {
      val b1: Long = data(offset+0) & 0xffL
      val b2: Long = data(offset+1) & 0xffL
      val b3: Long = data(offset+2) & 0xffL
      val b4: Long = data(offset+3) & 0xffL
      val b5: Long = data(offset+4) & 0xffL
      val b6: Long = data(offset+5) & 0xffL
      val b7: Long = data(offset+6) & 0xffL
      val b8: Long = data(offset+7) & 0xffL
      java.lang.Double.longBitsToDouble((b1 << 54) | (b2 << 48) | (b3 << 40) | (b4 << 32) | (b5 << 24) | (b6 << 16) | (b7 << 8) | b8)
    }),
  )

  val sparkTypeToJavaType: Map[DataType, Class[_]] = Map(
    (ByteType, classOf[Byte]),
    (ShortType, classOf[Short]),
    (IntegerType, classOf[Int]),
    (LongType, classOf[Long]),
    (FloatType, classOf[Float]),
    (DoubleType, classOf[Double])
  )
}
