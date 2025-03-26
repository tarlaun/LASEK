package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITile, ITileSerializer, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType}

import scala.annotation.tailrec
import scala.reflect.ClassTag

@DefaultSerializer(classOf[ITileSerializer[Any]])
class MemoryTile2[T](override val tileID: Int, override val rasterMetadata: RasterMetadata)
                    (implicit @transient t: ClassTag[T]) extends ITile[T](tileID, rasterMetadata) {

  /** List of all values stored in row-major order */
  val pixelValue: Array[T] = new Array[T](rasterMetadata.tileWidth * rasterMetadata.tileHeight)

  /** Whether a pixel is defined or not. Same order of pixel values */
  val pixelDefined: BitArray = new BitArray(rasterMetadata.tileWidth * rasterMetadata.tileHeight)

  private def pixelOffset(i: Int, j: Int): Int = (j - y1) * rasterMetadata.tileWidth + (i - x1)

  override def getPixelValue(i: Int, j: Int): T = pixelValue(pixelOffset(i, j))

  /** Number of bands per pixel. For arrays, this variable is only set upon writing the first value */
  private var _numComponents: Int = if (t.runtimeClass.isArray) 0 else 1

  def setPixelValue(i: Int, j: Int, value: T): Unit = {
    if (_numComponents == 0) {
      // First value to set, initialize number of components from it
      _numComponents = java.lang.reflect.Array.getLength(value)
    }
    val offset = pixelOffset(i, j)
    pixelDefined.set(offset, true)
    pixelValue(offset) = value
  }

  override def isEmpty(i: Int, j: Int): Boolean = !pixelDefined.get(pixelOffset(i, j))

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
  private val componentTypeNumber: Int = MemoryTile2.inferComponentType(t.runtimeClass)

  override def componentType: DataType = componentTypeNumber match {
    case 1 => ByteType
    case 2 => ShortType
    case 3 => IntegerType
    case 4 => LongType
    case 5 => FloatType
    case 6 => DoubleType
    case _ => throw new RuntimeException(s"Unrecognized component type $componentType")
  }

  override def numComponents: Int = _numComponents
}

object MemoryTile2 {
  /**
   * Determine the type of the component for the given runtime class.
   * If the given type is a primitive numeric type, we infer the component type directly from it.
   * If it is an array, we use its component type.
   *
   * @param klass the runtime class that represents the value type
   * @return the type of each component in the given type.
   */
  @tailrec
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
      throw new RuntimeException(s"Unrecognized class type '$klass'")
    }
  }
}