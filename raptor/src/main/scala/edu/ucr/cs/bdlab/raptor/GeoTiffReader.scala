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
import edu.ucr.cs.bdlab.beast.io.tiff.{AbstractTiffTile, ITiffReader, TiffConstants, TiffRaster}
import edu.ucr.cs.bdlab.beast.util.{BitArray, BufferedFSDataInputStream}
import edu.ucr.cs.bdlab.raptor.GeoTiffConstants.{ModelPixelScaleTag, ModelTiepointTag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.geom.AffineTransform
import java.nio.ByteBuffer

/**
 * A reader of GeoTIFF files.
 */
@SpatialReaderMetadata(
  description = "Opens GeoTIFF files",
  extension = ".tif",
  shortName = "geotiff",
  filter = "*.tif\n*.geotiff\n*.tiff",
)
class GeoTiffReader[T] extends IRasterReader[T] with Logging {
  import GeoTiffConstants._
  GeoTiffConstants.initializeTagNames()

  private var tiffReader: ITiffReader = _

  private var tiffMaskReader: ITiffReader = _

  /**The tiff raster that contains the pixel data*/
  private var tiffDataRaster: TiffRaster = _

  /**If the TIFF file has a separate raster that is used as mask layer*/
  private var tiffMaskRaster: TiffRaster = _

  /**The special value that marks empty pixels*/
  private var fillValue: Int = 0

  /**The metadata of this file*/
  private var rasterMetadata: RasterMetadata = _

  def this(filename: String) {
    this()
    val fileSystem = new Path(filename).getFileSystem(new Configuration())
    initialize(fileSystem, filename, "0", new BeastOptions())
  }

  override def initialize(fileSystem: FileSystem, path: String, layer: String, opts: BeastOptions): Unit = {
    super.initialize(fileSystem, path, layer, opts)
    val iLayer = layer.toInt
    this.initialize(fileSystem, new Path(path), iLayer)
    if (opts.contains("fillvalue")) {
      // Override the fill value in case it is not appropriately written in the file
      this.fillValue = opts.getInt("fillvalue", -1)
    }
  }

  private def initialize(fileSystem: FileSystem, path: Path, iLayer: Int): Unit = {
    // Loads the GeoTIFF file from the given path
    tiffReader = ITiffReader.openFile(fileSystem, path)
    tiffDataRaster = tiffReader.getLayer(iLayer)
    if (tiffReader.getNumLayers > 1) {
      // Check if the second layer acts as a mask layer
      tiffMaskReader = ITiffReader.openFile(fileSystem, path)
      val secondLayer = tiffMaskReader.getLayer(1)
      if (secondLayer.getEntry(TiffConstants.TAG_NEW_SUBFILE_TYPE).getOffsetAsInt == 4) {
        if (secondLayer.getNumSamples != 1)
          logWarning(s"Found a mask layer with ${secondLayer.getNumSamples} samples per pixel instead of one")
        if (secondLayer.getBitsPerPixel != 1)
          logWarning(s"Found a mask layer with ${secondLayer.getBitsPerPixel} bits per pixel instead of one")
        tiffMaskRaster = secondLayer
      }
    }
    // Define grid to model transformation (G2M)
    var buffer: ByteBuffer = null
    val g2m: AffineTransform = new AffineTransform
    var entry = tiffDataRaster.getEntry(ModelTiepointTag)
    if (entry != null) { // Translate point
      buffer = tiffReader.readEntry(entry, buffer)
      val dx = buffer.getDouble(3 * 8) - buffer.getDouble(0 * 8)
      val dy = buffer.getDouble(4 * 8) - buffer.getDouble(1 * 8)
      g2m.translate(dx, dy)
    }
    entry = tiffDataRaster.getEntry(ModelPixelScaleTag)
    if (entry != null) { // Scale point
      buffer = tiffReader.readEntry(entry, null)
      val sx = buffer.getDouble(0 * 8)
      val sy = buffer.getDouble(1 * 8)
      g2m.scale(sx, -sy)
    }

    entry = tiffDataRaster.getEntry(GDALNoDataTag)

    if (entry != null) {
      buffer = tiffReader.readEntry(entry, null)
      val prefix = Character.getNumericValue(buffer.get(0))
      if (prefix != -1) fillValue = prefix
      else fillValue = 0
      for (i <- 1 until buffer.capacity - 1) {
        val value = buffer.get(i)
        val temp = Character.getNumericValue(value)
        fillValue = fillValue * 10 + temp
      }
      if (prefix == -1) fillValue *= -1
    } else fillValue = Integer.MIN_VALUE

    // If the user requested to override the raster SRID, use the given value without parsing the file
    val srid: Int = if (opts.contains(IRasterReader.OverrideSRID)) {
      opts.getInt(IRasterReader.OverrideSRID, 0)
    } else {
      // call to class to get metadata.
      val metadata = new GeoTiffMetadata(tiffReader, tiffDataRaster)
      val gtcs = new GeoTiffMetadata2CRSAdapter(null)
      val crs: CoordinateReferenceSystem = try {
        gtcs.createCoordinateSystem(metadata)
      } catch {
        case e: Exception =>
          logWarning(s"Unable to parse GeoTiff CRS from file '$path'. Assuming EPSG:4326")
          logDebug("CRS parse error", e)
          null
      }
      // If CRS is not provided, assume EPSG:4326 instead of throwing an error of unknown geometry
      if (crs == null) 4326 else CRSServer.crsToSRID(crs)
    }
    rasterMetadata = new RasterMetadata(0, 0, tiffDataRaster.getWidth, tiffDataRaster.getHeight,
      tiffDataRaster.getTileWidth, tiffDataRaster. getTileHeight, srid, g2m)
  }

  override def metadata: RasterMetadata = rasterMetadata

  override def readTile(tileID: Int): ITile[T] = {
    val tiffTile: AbstractTiffTile = tiffDataRaster.getTile(tileID)
    val pixelExists: Array[Byte] = if (tiffMaskRaster != null) {
      // We assume that both the data and mask layers are co-tiled
      val tiffMaskTile = tiffMaskRaster.getTile(tileID)
      assert(tiffMaskTile.getTileData.length == 1, "Unsupported PlanarFormat for mask")
      tiffMaskTile.getTileData()(0)
    } else {
      null
    }
    if (tiffTile.getNumSamples == 1) {
      tiffTile.getSampleFormat(0) match {
        case 1 | 2 => new GeoTiffTileInt(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
        case 3 if tiffTile.getBitsPerSample(0) == 32 => new GeoTiffTileFloat(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
        case 3 if tiffTile.getBitsPerSample(0) == 64 => new GeoTiffTileDouble(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
      }
    } else {
      // Assume that all samples have the same type
      tiffTile.getSampleFormat(0) match {
        case 1 | 2 => new GeoTiffTileIntArray(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
        case 3 if tiffTile.getBitsPerSample(0) == 32 => new GeoTiffTileFloatArray(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
        case 3 if tiffTile.getBitsPerSample(0) == 64 => new GeoTiffTileDoubleArray(tileID, tiffTile, fillValue, pixelExists, rasterMetadata).asInstanceOf[ITile[T]]
      }
    }
  }

  /**
   * Whether the given tile contains non-empty data or not. This function does not actually read the tile to
   * determine whether it has valid data or not. Rather, it relies a non-GeoTiff-standard that sets empty
   * tiles to an offset and length of zero
   * @param tileID the ID of the tile to check
   * @return `false` if the tile is known to be empty from the GeoTiff entry or `true` if the tile is not listed
   *         in the entry or if the entry does not exist.
   */
  override def isValidTile(tileID: Int): Boolean = getTileOffset(tileID) != 0

  override def getTileOffset(tileID: Int): Long = tiffDataRaster.getTileOffset(tileID)

  override def close(): Unit = {
    if (tiffMaskReader != null) {
      tiffMaskReader.close()
    }
    if (tiffReader != null)
      tiffReader.close()
  }
}

object GeoTiffReader {
  def getRasterMetadata(fileSystem: FileSystem, path: Path): RasterMetadata = {
    val in = new BufferedFSDataInputStream(fileSystem.open(path), 4096)
    try {
      val tiffReader = ITiffReader.openFile(in)
      val tiffDataRaster = tiffReader.getLayer(0)
      // Define grid to model transformation (G2M)
      var buffer: ByteBuffer = null
      val g2m: AffineTransform = new AffineTransform
      var entry = tiffDataRaster.getEntry(ModelTiepointTag)
      if (entry != null) { // Translate point
        buffer = tiffReader.readEntry(entry, buffer)
        val dx = buffer.getDouble(3 * 8) - buffer.getDouble(0 * 8)
        val dy = buffer.getDouble(4 * 8) - buffer.getDouble(1 * 8)
        g2m.translate(dx, dy)
      }
      entry = tiffDataRaster.getEntry(ModelPixelScaleTag)
      if (entry != null) { // Scale point
        buffer = tiffReader.readEntry(entry, null)
        val sx = buffer.getDouble(0 * 8)
        val sy = buffer.getDouble(1 * 8)
        g2m.scale(sx, -sy)
      }

      // call to class to get metadata.
      val metadata = new GeoTiffMetadata(tiffReader, tiffDataRaster)
      val gtcs = new GeoTiffMetadata2CRSAdapter(null)
      val crs: CoordinateReferenceSystem = try {
        gtcs.createCoordinateSystem(metadata)
      } catch {
        case e: Exception =>
          null
      }
      // If CRS is not provided, assume EPSG:4326 instead of throwing an error of unknown geometry
      val srid = if (crs == null) 4326 else CRSServer.crsToSRID(crs)

      new RasterMetadata(0, 0, tiffDataRaster.getWidth, tiffDataRaster.getHeight,
        tiffDataRaster.getTileWidth, tiffDataRaster. getTileHeight, srid, g2m)
    } finally {
      in.close()
    }

  }
}