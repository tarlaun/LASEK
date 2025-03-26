package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.beast.io.tiff.AbstractTiffTile
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * A special class for GeoTiffTiles where each pixel has one band of float type.
 * @param tileID the ID of the tile within the tile grid
 * @param tiffTile the underlying tiff tile
 * @param fillValue the fill value in the tiff tile that marks empty pixels
 * @param metadata the metadata of the raster file
 */
@DefaultSerializer(classOf[GeoTiffTileSerializer[Any]])
class GeoTiffTileDouble(tileID: Int, tiffTile: AbstractTiffTile,
                        fillValue: Int, pixelDefined: Array[Byte], metadata: RasterMetadata)
  extends AbstractGeoTiffTile[Double](tileID, tiffTile, fillValue, pixelDefined, metadata) {

  override def getPixelValue(i: Int, j: Int): Double = tiffTile.getSampleValueAsDouble(i, j, 0)

  override def componentType: DataType = DoubleType
}
