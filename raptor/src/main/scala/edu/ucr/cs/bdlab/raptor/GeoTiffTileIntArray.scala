package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITileSerializer, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.AbstractTiffTile
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

import scala.reflect.ClassTag

/**
 * A special class for GeoTiffTiles where each pixel has two or more bands of integer type.
 * @param tileID the ID of the tile within the tile grid
 * @param tiffTile the underlying tiff tile
 * @param fillValue the fill value in the tiff tile that marks empty pixels
 * @param rasterMetadata the metadata of the raster file
 */
@DefaultSerializer(classOf[GeoTiffTileSerializer[Any]])
class GeoTiffTileIntArray(tileID: Int, tiffTile: AbstractTiffTile,
                          fillValue: Int, pixelDefined: Array[Byte], rasterMetadata: RasterMetadata)
  extends AbstractGeoTiffTile[Array[Int]](tileID, tiffTile, fillValue, pixelDefined, rasterMetadata) {

  override def getPixelValue(i: Int, j: Int): Array[Int] = {
    val values = new Array[Int](numComponents)
    for (b <- values.indices)
      values(b) = tiffTile.getSampleValueAsInt(i, j, b)
    values
  }

  override def componentType: DataType = IntegerType
}
