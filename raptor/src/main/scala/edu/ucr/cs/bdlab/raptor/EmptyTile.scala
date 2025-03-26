package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITile, ITileSerializer, RasterMetadata}
import org.apache.spark.sql.types.{DataType, IntegerType, NullType}

import scala.reflect.ClassTag

/**
 * A tile that is completely empty with no underlying data. This is used as a placeholder whenever
 * a tile does not exist in a raster layer.
 */
@DefaultSerializer(classOf[ITileSerializer[Any]])
class EmptyTile[T](tileID: Int, rasterMetadata: RasterMetadata,
                   override val numComponents: Int) extends ITile[T](tileID, rasterMetadata) {

  override def isEmpty(i: Int, j: Int): Boolean = true

  // All remaining function should never be called since all pixels are empty
  override def getPixelValue(i: Int, j: Int): T = throw new NullPointerException(s"Pixel ($i, $j) is empty")

  override def componentType: DataType = NullType
}
