/*
 * Copyright 2021 University of California, Riverside
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
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.SpatialReaderMetadata
import edu.ucr.cs.bdlab.jhdf.{DDNumericDataGroup, DDVDataHeader, HDFConstants, HDFFile}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.beast.CRSServer
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.Rectangle
import java.awt.geom.{AffineTransform, Rectangle2D}
import java.nio.ByteBuffer
import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.Pattern

/**
 * A reader for HDF files
 */
@SpatialReaderMetadata(
  description = "Opens Hierarchical Data Format (HDF) files version 4",
  extension = ".hdf",
  shortName = "hdf",
  filter = "*.hdf",
)
class HDF4Reader extends IRasterReader[Float] {
  import HDF4Reader._

  /** The underlying HDF file */
  protected var hdfFile: HDFFile = _

  /** The ID of the tile in the MODIS grid */
  protected var h = 0
  protected var v: Int = 0

  /** The grid size of the raster file */
  protected var resolution: Int = 0

  /** The fill value as listed in the header of the file */
  protected var fillValue: Int = 0

  /**A flag of whether the fill value is valid or not*/
  protected var fillValueValid: Boolean = false

  /** The range of valid values from the header of the HDF file */
  protected var minValue: Int = 0
  protected var maxValue: Int = 0

  /**Metadata of the raster file*/
  override def metadata: RasterMetadata = _metadata
  private var _metadata: RasterMetadata = _

  /** The scale factor listed in the header or 1.0 if not listed in the HDF file */
  protected var scaleFactor = .0

  override def initialize(fileSystem: FileSystem, path: String, layer: String, opts: BeastOptions): Unit = {
    super.initialize(fileSystem, path, layer, opts)
    if (this.hdfFile != null) {
      // In case the reader is reused without properly closing it
      this.close()
    }

    this.hdfFile = new HDFFile(path.toString)
    // Retrieve the h and v values (only for MODIS files from the LP DAAC archive)
    val archiveMetadata = hdfFile.findHeaderByName("ArchiveMetadata.0").getEntryAt(0).asInstanceOf[String]
    val coreMetadata = hdfFile.findHeaderByName("CoreMetadata.0").getEntryAt(0).asInstanceOf[String]
    try {
      this.h = getIntByName(archiveMetadata, "HORIZONTALTILENUMBER")
      this.v = getIntByName(archiveMetadata, "VERTICALTILENUMBER")
    } catch {
      case e: RuntimeException =>
        // For WaterMask (MOD44W), these values are found somewhere else
        try {
          this.h = getIntByName(coreMetadata, "HORIZONTALTILENUMBER")
          this.v = getIntByName(coreMetadata, "VERTICALTILENUMBER")
        } catch {
          case e2: RuntimeException =>
            throw new RuntimeException("Could not getPixel h and v values for an HDF file")
        }
    }
    // DATACOLUMNS is not available in MODIS temperature product MOD11A1.006
    //this.resolution = getIntByName(archiveMetadata, "DATACOLUMNS");
    this.loadRasterMetadata()
    val srid = CRSServer.crsToSRID(SinusoidalCRS)
    // Create an affine transformation for a model space in the sinusoidal projection
    val affineMatrix = Array(TileWidth / resolution, 0, 0, -1 * TileHeight / resolution, MinX + h * TileWidth, MaxY - v * TileHeight)
    val g2m: AffineTransform = new AffineTransform(affineMatrix)
    this._metadata = new RasterMetadata(0, 0, resolution, resolution, resolution, resolution, srid, g2m)
  }

  def loadRasterMetadata(): Unit = {
    // Find the required dataset and read it
    val dataGroup = hdfFile.findGroupByName(getLayerName)
    require(dataGroup != null, s"Could not find the layer '${getLayerName}' in the HDF file")
    this.scaleFactor = 1.0f
    for (dd <- dataGroup.getContents; if dd.isInstanceOf[DDVDataHeader]) {
          val vheader = dd.asInstanceOf[DDVDataHeader]
      vheader.getName match {
        case "_FillValue" => {
          this.fillValue = vheader.getEntryAt(0) match {
            case i: Integer => i
            case s: java.lang.Short => s.toInt
            case b: java.lang.Byte => b.toInt
          }
          fillValueValid = true
        }
        case "valid_range" => {
          this.minValue = vheader.getEntryAt(0) match {
            case i: Integer => i
            case s: java.lang.Short => s.toInt
            case b: java.lang.Byte => b.toInt
          }
          this.maxValue = vheader.getEntryAt(1) match {
            case i: Integer => i
            case s: java.lang.Short => s.toInt
            case b: java.lang.Byte => b.toInt
          }
        }
        case "scale_factor" => {
          this.scaleFactor =  vheader.getEntryAt(0) match {
            case i: Integer => i.toDouble
            case b: java.lang.Byte => b.toDouble
            case f: java.lang.Float => f.toDouble
            case d: java.lang.Double => d
          }
          // Hack: The scale factor in NDVI data is listed as 10,000 but it should be 1/10,000
          // See: https://lpdaac.usgs.gov/products/mod13q1v006/
          if (scaleFactor == 10000.0) scaleFactor = 1 / 10000.0
        }
        case _ => // Ignore
      }
    }
    // Retrieve data size
    for (dd <- dataGroup.getContents; if dd.isInstanceOf[DDNumericDataGroup]) {
      val numericDataGroup = dd.asInstanceOf[DDNumericDataGroup]
      // A bug causes the numericDataGroup to be not loaded. toString will force loading it
      numericDataGroup.toString
      this.resolution = numericDataGroup.getDimensions()(0)
    }
  }

  override def readTile(tileID: Int): ITile[Float] = {
    require(tileID == 0, "Invalid tile number")
    val dataGroup = hdfFile.findGroupByName(getLayerName)
    require(dataGroup != null, s"Could not find the layer '${getLayerName}' in the HDF file")

    // Retrieve data size
    for (dd <- dataGroup.getContents; if dd.isInstanceOf[DDNumericDataGroup]) {
      val numericDataGroup = dd.asInstanceOf[DDNumericDataGroup]
      val valueSize: Int = numericDataGroup.getDataSize
      val unparsedDataArray: Array[Byte] = new Array[Byte](valueSize * resolution * resolution)
      if (fillValueValid) {
        val fillValueBytes = new Array[Byte](valueSize)
        HDFConstants.writeAt(fillValueBytes, 0, this.fillValue, valueSize)
        for (i <- unparsedDataArray.indices)
          unparsedDataArray(i) = fillValueBytes(i % valueSize)
      }
      numericDataGroup.getAsByteArray(unparsedDataArray, 0, unparsedDataArray.length)
      val dataBuffer = ByteBuffer.wrap(unparsedDataArray)
      return new HDFTile(tileID, dataBuffer.array(), valueSize, fillValue, scaleFactor, metadata)
    }
    // Tile not found
    null
  }

  override def getTileOffset(tileID: Int): Long = 0

  override def close(): Unit = hdfFile.close()

  private def getIntByName(metadata: String, name: String) = {
    val strValue = getStringByName(metadata, name)
    if (strValue == null) throw new RuntimeException(String.format("Couldn't find value with name '%s'", name))
    strValue.toInt
  }

  private def getStringByName(metadata: String, name: String): String = {
    var offset = metadata.indexOf(name)
    if (offset == -1) return null
    offset = metadata.indexOf(" VALUE", offset)
    if (offset == -1) return null
    offset = metadata.indexOf('=', offset)
    if (offset == -1) return null
    do offset += 1 while ( {
      offset < metadata.length && metadata.charAt(offset) == ' ' || metadata.charAt(offset) == '"'
    })
    var endOffset = offset
    do endOffset += 1 while ( {
      endOffset < metadata.length && metadata.charAt(endOffset) != ' ' && metadata.charAt(endOffset) != '"' && metadata.charAt(endOffset) != '\n' && metadata.charAt(endOffset) != '\r'
    })
    if (offset < metadata.length) return metadata.substring(offset, endOffset)
    null
  }

}

object HDF4Reader {
  /** Boundaries of the Sinusoidal space assuming angles in radians and multiplying by the scale factor 6371007.181 */
  val Scale: Double = 6371007.181
  val MinX: Double = -Math.PI * Scale
  val MaxX: Double = +Math.PI * Scale
  val MinY: Double = -Math.PI / 2.0 * Scale
  val MaxY: Double = +Math.PI / 2.0 * Scale
  val TileWidth: Double = (MaxX - MinX) / 36
  val TileHeight: Double = (MaxY - MinY) / 18

  /** The coordinate reference system for the sinusoidal space */
  lazy val SinusoidalCRS: CoordinateReferenceSystem = CRS.parseWKT(
"""PROJCS["MODIS Sinusoidal",
  GEOGCS["GCS Name = Unknown datum based upon the custom spheroid",
    DATUM["Datum = Not specified (based on custom spheroid)",
      SPHEROID["Ellipsoid = Custom spheroid", 6371007.181, 0.0]],
    PRIMEM["Greenwich", 0.0],
    UNIT["degree", 0.017453292519943295],
    AXIS["Geodetic longitude", EAST],
    AXIS["Geodetic latitude", NORTH]],
  PROJECTION["Sinusoidal"],
  PARAMETER["central_meridian", 0.0],
  PARAMETER["false_easting", 0.0],
  PARAMETER["false_northing", 0.0],
  UNIT["m", 1.0],
  AXIS["Easting", EAST],
  AXIS["Northing", NORTH]]
""")
//  val SinusoidalCRS: CoordinateReferenceSystem = {
//    // https://spatialreference.org/ref/sr-org/modis-sinusoidal/
//    new DefaultProjectedCRS("MODIS Sinusoidal", new DefaultGeographicCRS(
//      DefaultGeodeticDatum.WGS84,
//      DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
//      new DefaultMathTransformFactory().createFromWKT(
//"""
//PARAM_MT["Sinusoidal",
// PARAMETER["semi_major", 6371007.181], PARAMETER["semi_minor", 6371007.181],
// PARAMETER["central_meridian", 0.0],
// PARAMETER["false_easting", 0.0],  PARAMETER["false_northing", 0.0]
//]
//"""),
//      DefaultCartesianCS.PROJECTED)
//  }

  /** Format of file names in MODIS repository with tile identifier in the name */
  val ModisTileIDRegex: Pattern = Pattern.compile(".*h(\\d\\d)v(\\d\\d).*")

  /**
   * Create a path filter that selects only the tiles that match the given rectangle in the Sinusoidal space.
   *
   * @param rect the extents of the range to compute the filter for in the Sinusoidal space
   * @return a Path filter that will match the tiles based on the file name using the <tt>hxxvyy</tt> part
   */
  def createTileIDFilter(rect: Rectangle2D): PathFilter = {
    val hMin = ((rect.getX - MinX) / TileWidth).toInt
    val hMax = ((rect.getMaxX - MinX) / TileWidth).toInt
    val vMax = ((-rect.getY - MinY) / TileHeight).toInt
    val vMin = ((-rect.getMaxY - MinY) / TileHeight).toInt
    val tileRange = new Rectangle(hMin, vMin, hMax - hMin + 1, vMax - vMin + 1)
    p: Path => {
      val matcher = ModisTileIDRegex.matcher(p.getName)
      if (!matcher.matches)
        false
      else {
        val h = matcher.group(1).toInt
        val v = matcher.group(2).toInt
        tileRange.contains(h, v)
      }
    }
  }

  /** Format of directory names in MODIS with embedded date */
  val DateFormat = new SimpleDateFormat("yyyy.MM.dd")

  /**
   * Creates a filter for paths that match the given range of dates inclusive of both start and end dates.
   * Each date is in the format "yyyy.mm.dd".
   *
   * @param dateStart the start date as a string in the "yyyy.mm.dd" format (inclusive)
   * @param dateEnd   the end date (inclusive)
   * @return a PathFilter that will match all dates in the given range
   */
  def createDateFilter(dateStart: String, dateEnd: String): PathFilter = {
    val startTime = DateFormat.parse(dateStart).getTime
    val endTime = DateFormat.parse(dateEnd).getTime
    (p: Path) => { try {
        val time = DateFormat.parse(p.getName).getTime
        time >= startTime && time <= endTime
      } catch {
        case e: ParseException => false
      }
    }
  }
}