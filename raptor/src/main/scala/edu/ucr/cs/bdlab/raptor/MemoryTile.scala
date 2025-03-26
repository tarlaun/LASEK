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
import edu.ucr.cs.bdlab.beast.geolite.{ITile, ITileSerializer, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.{BitArray, LongArrayInputStream, LongArrayOutputStream, MathUtil}
import org.apache.spark.sql.types._

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.reflect.ClassTag

/**
 * Stores tile values in an in-memory array. When serialized, data gets compressed automatically to reduce
 * network size. When deserialized, data is lazily decompressed when first accessed.
 *
 * This memory tile stores all values in a bit-packed array to reduce memory overhead.

 * @param tileID the ID of this tile in the raster metadata
 * @param metadata the raster metadata that defines the underlying raster
 * @param t
 * @tparam T the type of measurement values in tiles
 */
@DefaultSerializer(classOf[MemoryTileSerializer])
class MemoryTile[T](tileID: Int, metadata: RasterMetadata)
                   (implicit @transient t: ClassTag[T]) extends ITile[T](tileID, metadata) {

  /** Number of bands per pixel. For arrays, this variable is only set upon writing the first value */
  var numComponents: Int = if (t.runtimeClass.isArray) 0 else 1

  /**
   * The type of values stored in all components. For that, we assume that all components have the same type.
   *  - 0: Undefined
   *  - 1: 8-bit signed byte
   *  - 2: 16-bit signed short
   *  - 3: 32-bit signed int
   *  - 4: 64-bit signed long int
   *  - 5: 32-bit single-precision floating point
   *  - 6: 64-bit double-precision floating point
   */
  var componentTypeNumber: Int = MemoryTile.inferComponentType(t.runtimeClass)

  /**Number of bits per pixel which is calculated as [[numComponents]] &times; [[bitsPerComponent]] */
  def bitsPerPixel: Int = numComponents * bitsPerComponent

  /** Number of bits needed for each component assuming that all components have the same type */
  private[raptor] var bitsPerComponent: Int = MemoryTile.NumberOfBitsPerType(componentTypeNumber)

  /**
   * If the value type is array, this object holds a temporary object of the pixel type to reduce creation overhead.
   */
  @transient lazy val tempPixelValue: Array[_] = {
    val componentClass: Class[_] = componentTypeNumber match {
      case 1 => classOf[Byte]
      case 2 => classOf[Short]
      case 3 => classOf[Int]
      case 4 => classOf[Long]
      case 5 => classOf[Float]
      case 6 => classOf[Double]
      case _ => throw new RuntimeException(s"Unknown type ${componentType}")
    }
    java.lang.reflect.Array.newInstance(componentClass, numComponents).asInstanceOf[Array[_]]
  }

  /** The internal array that stores all measurement values */
  var data: Array[Long] = _

  /**
   * Compression type.
   *  - (NONE=0)
   *  - (GZIP=1)
   */
  var compression: Int = 0

  // TODO add compression when the tile gets transferred over network

  if (!t.runtimeClass.isArray) {
    // For primitive values, we can determine everything we need
    numComponents = 1
    data = new Array[Long](((this.tileWidth * this.tileHeight) * bitsPerPixel + 63) / 64)
  } else {
    // For arrays, we have to wait until the first value is set to determine the number of components
  }

  /**A bit-packed bit array where 1 means a pixel exists and 0 means pixel does not exist (empty)*/
  val pixelExists: Array[Long] = new Array[Long]((this.tileWidth * this.tileHeight + 63) / 64)

  override def getPixelValue(i: Int, j: Int): T = {
    var pixelOffset = getPixelOffset(i, j) * bitsPerPixel
    if (numComponents == 1) {
      // A single value
      parseComponentValue(pixelOffset).asInstanceOf[T]
    } else {
      val pixelValue: Array[_] = tempPixelValue.clone()
      for (i <- 0 until numComponents) {
        val rawValue: Any = parseComponentValue(pixelOffset)
        java.lang.reflect.Array.set(pixelValue, i, rawValue)
        pixelOffset += bitsPerComponent
      }
      pixelValue.asInstanceOf[T]
    }
  }

  /**
   * Sets the value of a specific component at the given bit offset
   * @param componentOffset the bit offset of the component in the data array
   * @param value the value to set
   * @tparam U the type of the value to set
   */
  private def setComponentValue[U](componentOffset: Long, value: U): Unit = {
    val rawValue: Long = value match {
      case x: Byte => x
      case x: Short => x
      case x: Int => x
      case x: Long => x
      case x: Float => java.lang.Float.floatToIntBits(x)
      case x: Double => java.lang.Double.doubleToLongBits(x)
      case _ => throw new RuntimeException(s"Unrecognized value ${value} of type ${value.getClass}")
    }
    MathUtil.setBits(data, componentOffset, bitsPerComponent, rawValue)
  }

  /**
   * Sets the value of the given pixel
   * @param i the column position of the pixel
   * @param j the row position of the pixel
   * @param value the value to set at the given pixel
   */
  def setPixelValue(i: Int, j:Int, value: T): Unit = {
    if (compression != 0)
      decompress
    var pixelOffset = getPixelOffset(i, j)
    BitArray.setBit(pixelExists, pixelOffset)
    if (numComponents == 1) {
      // A single value
      pixelOffset *= bitsPerPixel
      setComponentValue(pixelOffset, value)
    } else {
      if (numComponents == 0) {
        // First value to set, initialize number of components from it
        numComponents = java.lang.reflect.Array.getLength(value)
        data = new Array[Long](((this.tileWidth * this.tileHeight) * bitsPerPixel + 63) / 64)
      }
      pixelOffset *= bitsPerPixel
      assert(java.lang.reflect.Array.getLength(value) == numComponents, s"Mismatching value length" +
        s" ${java.lang.reflect.Array.getLength(value)} and number of components ${numComponents}")
      value match {
        case xs: Array[_] => for (x <- xs) {
          setComponentValue(pixelOffset, x)
          pixelOffset += bitsPerComponent
        }
        case _ => throw new RuntimeException(s"Unrecognized value type ${value.getClass}")
      }
    }
  }

  override def isEmpty(i: Int, j: Int): Boolean = !BitArray.isBitSet(pixelExists, getPixelOffset(i, j))

  override def componentType: DataType = componentTypeNumber match {
    case 1 => ByteType
    case 2 => ShortType
    case 3 => IntegerType
    case 4 => LongType
    case 5 => FloatType
    case 6 => DoubleType
    case _ => throw new RuntimeException(s"Unrecognized component type $componentType")
  }

  /**
   * Computes the position of the pixel in the row-major order of pixels
   * @param i the column of the pixel
   * @param j the row of the pixel
   * @return the position of this pixel among all pixels in this tile
   */
  @inline
  protected def getPixelOffset(i: Int, j: Int): Long = (j - y1) * this.tileWidth + (i - x1)

  private def parseComponentValue(pixelOffset: Long): Any = {
    if (compression != 0)
      decompress
    val rawValue: Long = MathUtil.getBits(data, pixelOffset, bitsPerComponent)
    componentTypeNumber match {
      case 1 => rawValue.toByte
      case 2 => rawValue.toShort
      case 3 => rawValue.toInt
      case 4 => rawValue.toLong
      case 5 => java.lang.Float.intBitsToFloat(rawValue.toInt)
      case 6 => java.lang.Double.longBitsToDouble(rawValue.toLong)
      case _ => throw new RuntimeException(s"Unrecognized component type ${componentType}")
    }
  }

  protected[raptor] def compress: Unit = {
    if (compression != 0)
      return // Already compressed
    // Compress the tile data in-place
    // TODO apply a value encoder, e.g., delta encoder, to improve compression
    val laos = new LongArrayOutputStream()
    val out = new DataOutputStream(new GZIPOutputStream(laos))
    if (data == null) {
      out.writeInt(0)
    } else {
      out.writeInt(data.length)
      for (l <- data)
        out.writeLong(l)
    }
    out.close()

    this.data = laos.getData
    this.compression = 1
  }

  protected def decompress: Unit = {
    if (compression == 0)
      return
    val in = new DataInputStream(new GZIPInputStream(new LongArrayInputStream(this.data)))
    val size = in.readInt()
    val newData = new Array[Long](size)
    for (i <- 0 until size)
      newData(i) = in.readLong()
    this.data = newData
    this.compression = 0
  }
}

object MemoryTile {
  val NumberOfBitsPerType: Array[Int] = Array[Int](0, 8, 16, 32, 64, 32, 64)


  /**
   * Determine the type of the component for the given runtime class.
   * If the given type is a primitive numeric type, we infer the component type directly from it.
   * If it is an array, we use its component type.
   * @param klass the runtime class that represents the value type
   * @return the type of each component in the given type.
   */
  private[raptor] def inferComponentType(klass: Class[_]): Int = {
    if (klass.isArray) {
      inferComponentType(klass.getComponentType)
    } else if (klass == classOf[Byte]) {
      1
    } else if (klass == classOf[Short]) {
      2
    } else if (klass == classOf[Int]) {
      3
    } else if (klass == classOf[Long]) {
      4
    } else if (klass == classOf[Float]) {
      5
    } else if (klass == classOf[Double]) {
      6
    } else {
      throw new RuntimeException(s"Unrecognized class type '${klass}'")
    }
  }
}