package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITileSerializer, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.AbstractTiffTile
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType}

/**
 * A special class for GeoTiffTiles where each pixel has two or more bands of float type.
 *
 * @param tileID the ID of the tile within the tile grid
 * @param tiffTile the underlying tiff tile
 * @param fillValue the fill value in the tiff tile that marks empty pixels
 * @param metadata the metadata of the raster file
 */
@DefaultSerializer(classOf[GeoTiffTileSerializer[Any]])
class GeoTiffTileFloatArray(tileID: Int, tiffTile: AbstractTiffTile,
                            fillValue: Int, pixelDefined: Array[Byte], metadata: RasterMetadata)
  extends AbstractGeoTiffTile[Array[Float]](tileID, tiffTile, fillValue, pixelDefined, metadata) {

  override def getPixelValue(i: Int, j: Int): Array[Float] = {
    val values = new Array[Float](numComponents)
    for (b <- values.indices)
      values(b) = tiffTile.getSampleValueAsFloat(i, j, b)
    values
  }

  override def componentType: DataType = FloatType
}
