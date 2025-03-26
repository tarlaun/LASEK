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
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.geotools.referencing.CRS
import org.geotools.referencing.CRS.AxisOrder
import org.geotools.referencing.operation.projection.ProjectionException
import org.locationtech.jts.geom.{Coordinate, CoordinateXY, Envelope, Geometry}
import org.opengis.metadata.extent.GeographicBoundingBox
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.geom.{AffineTransform, Point2D}

/**
 * A class that holds metadata of a raster layer
 * @param x1 The index of the lowest column in the raster file (inclusive)
 * @param y1 The index of the lowest row in the raster file (inclusive)
 * @param x2 The index of the highest column in the raster file (exclusive)
 * @param y2 The highest index of a row in the raster layer (exclusive)
 * @param tileWidth The width of each tile in pixels
 * @param tileHeight he height of each tile in pixels
 * @param srid The spatial reference identifier of the coordinate reference system of this raster layer
 * @param g2m the grid to model affine transformation
 */
@DefaultSerializer(classOf[RasterMetadataSerializer])
class RasterMetadata(val x1: Int, val y1: Int, val x2: Int, val y2: Int,
                     val tileWidth: Int, val tileHeight: Int,
                     val srid: Int, val g2m: AffineTransform) extends Serializable {
  /**Total number of columns in the raster layer*/
  def rasterWidth: Int = x2 - x1

  /**Total number of rows (scanlines) in the raster layer*/
  def rasterHeight: Int = y2 - y1

  /**Number of tiles per row*/
  def numTilesX: Int = (rasterWidth + tileWidth - 1) / tileWidth

  /**Number of tiles per column*/
  def numTilesY: Int = (rasterHeight + tileHeight - 1) / tileHeight

  /**Total number of tiles in the raster layer*/
  def numTiles: Int = numTilesX * numTilesY

  /**
   * Computes the ID of the tile that contains the given pixel. Tiles are numbered in row-wise ordering.
   * @param iPixel the position of the column of the pixel
   * @param jPixel the position of the row of the pixel
   * @return a unique identifier for the tile that contains this pixel location
   */
  def getTileIDAtPixel(iPixel: Int, jPixel: Int): Int = {
    val iTile = (iPixel - x1) / tileWidth
    val jTile = (jPixel - y1) / tileHeight
    jTile * numTilesX + iTile
  }

  /**
   * Returns the ID of the tile that contains the given point location in model (world) space
   * @param x the x-coordinate of the point, e.g., longitude
   * @param y the y-coordinate of the point, e.g., latitude
   * @return the ID of the tile that contains this pixel or -1 if the point is outside the input space
   */
  def getTileIDAtPoint(x: Double, y: Double): Int = {
    val pixel: Point2D.Double = new Point2D.Double()
    modelToGrid(x, y, pixel)
    getTileIDAtPixel(pixel.x.toInt, pixel.y.toInt)
  }

  /**
   * An iterators that goes over all tile IDs
   * @return an iterator that iterates over all tile IDs in this raster
   */
  def tileIDs: Iterator[Int] = 0 until numTiles iterator

  def getPixelScaleX: Double = {
    val pt = new Point2D.Double()
    gridToModel(0, 0, pt)
    val x1 = pt.x
    gridToModel(1, 0, pt)
    val x2 = pt.x
    (x2 - x1).abs
  }

  /**
   * Computes the low index of a column in the given tile (inclusive)
   * @param tileID the ID of the tile
   * @return
   */
  def getTileX1(tileID: Int): Int = {
    val iTile = tileID % numTilesX
    x1 + iTile * tileWidth
  }

  /**
   * Computes the high index of a column in the given tile (inclusive).
   * If this tile is in the last column, the returned value will be equal to the last column in the raster.
   * @param tileID the ID of the tile
   * @return
   */
  def getTileX2(tileID: Int): Int = {
    val iTile = tileID % numTilesX
    (x1 + (iTile + 1) * tileWidth - 1) min (x2 - 1)
  }

  /**
   * Computes the low index of a row (scanline) in the given tile (inclusive)
   * @param tileID the ID of the tile
   * @return
   */
  def getTileY1(tileID: Int): Int = {
    val iTile = tileID / numTilesX
    y1 + iTile * tileHeight
  }

  /**
   * Computes the high index of a row (scanline) in the given tile (inclusive).
   * If this tile is in the last row, the returned value will be the last row in the entire raster
   * @param tileID the ID of the tile
   * @return
   */
  def getTileY2(tileID: Int): Int = {
    val iTile = tileID / numTilesX
    (y1 + (iTile + 1) * tileHeight - 1) min (y2 - 1)
  }

  /**
   * Converts a point location from the grid (pixel) space to the model (world) space
   * @param i the position of the column
   * @param j the position of the row
   * @param outPoint the output point that contains the model coordinates
   */
  def gridToModel(i: Double, j: Double, outPoint: Point2D.Double): Unit = {
    outPoint.setLocation(i, j)
    g2m.transform(outPoint, outPoint)
  }

  /**
   * Converts a point location from model (world) space to grid (pixel) space
   * @param x the x-coordinate in the model space (e.g., longitude)
   * @param y the y-coordinate in the model space (e.g., latitude)
   * @param outPoint the output point that contains the grid coordinates
   */
  def modelToGrid(x: Double, y: Double, outPoint: Point2D.Double): Unit = {
    outPoint.setLocation(x, y)
    g2m.inverseTransform(outPoint, outPoint)
  }

  /**
   * An envelope that represents the boundaries of the raster data in the model space
   * @return
   */
  def envelope: Envelope = {
    val bounds = Array[Double](x1, y1, x2, y2)
    g2m.transform(bounds, 0, bounds, 0, 2)
    new Envelope(bounds(0) min bounds(2), bounds(0) max bounds(2),
      bounds(1) min bounds(3), bounds(1) max bounds(3))
  }

  /**
   * The extents of the RasterMetadata in model space as a rectangular polygon
   * @return
   */
  def extents: Geometry = {
    val values = Array[Double](x1, y1, x2, y1, x2, y2, x1, y2, x1, y1)
    g2m.transform(values, 0, values, 0, 5)
    val coords = new Array[Coordinate](values.length / 2)
    for (i <- values.indices by 2) {
      coords(i / 2) = new CoordinateXY(values(i), values(i+1))
    }
    val polygon = GeometryReader.DefaultGeometryFactory.createPolygon(coords)
    polygon.setSRID(this.srid)
    polygon
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[RasterMetadata]) return false
    val that = obj.asInstanceOf[RasterMetadata]
    (this eq that) || (this.x1 == that.x1 && this.x2 == that.x2 && this.y1 == that.y1 && this.y2 == that.y2 &&
      this.tileWidth == that.tileWidth && this.tileHeight == that.tileHeight && this.srid == that.srid &&
      this.g2m.equals(that.g2m))
  }

  override def hashCode(): Int = {
    x1.hashCode() + x2.hashCode() + y1.hashCode() + y2.hashCode() + tileWidth.hashCode() + tileHeight.hashCode() +
      srid.hashCode() + g2m.hashCode()
  }

  override def toString: String = {
    s"RasterMetadata ($x1, $y1)-($x2, $y2) tile size ${tileWidth}X${tileHeight} SRID: $srid G2M: $g2m"
  }

  /**
   * Returns a new metadata that covers the same region but with fewer pixels.
   * @param newRasterWidth the new width in pixels, i.e., number of pixels per row (per line)
   * @param newRasterHeight the new height in pixels, i.e., number of rows (lines)
   * @return the new raster metadata
   */
  def rescale(newRasterWidth: Int, newRasterHeight: Int): RasterMetadata = {
    // 1. Set (x1, y1) = (0, 0) for the new raster metadata
    // 2. Set (x2, y2) = (newRasterWidth, newRasterHeight) for the new raster metadata
    // 3. Use the same SRID
    // 4. Adjust grid-to-model to make (x1, y1) point to the first corner and (x2, y2) to the other corner
    val corners = Array[Double](x1, y1, x2, y2)
    this.g2m.transform(corners, 0, corners, 0, 2)
    RasterMetadata.create(corners(0), corners(1), corners(2), corners(3), this.srid, newRasterWidth, newRasterHeight,
      this.tileWidth min newRasterWidth, this.tileHeight min newRasterHeight)
  }

  /**
   * Creates a new [[RasterMetadata]] with the same resolution and the same geographical region
   * but in a difference coordinate reference system
   * @param targetCRS the target [[CoordinateReferenceSystem]]
   * @param sparkConf needed to contact the CRSServer and convert CRS to SRID
   * @return a new raster metadata that is similar to this one except in the CRS
   */
  def reproject(targetCRS: CoordinateReferenceSystem): RasterMetadata = {
    val corners = Array[Double](x1, y1, x2, y2)
    g2m.transform(corners, 0, corners, 0, 2)
    val targetSRID = CRSServer.crsToSRID(targetCRS)
    try {
      Reprojector.reprojectEnvelopeInPlace(corners, this.srid, targetSRID)
    } catch {
      case _: ProjectionException =>
        // This problem might happen if the range of the source goes outside the range of the destination
        // For example, if we are trying to reproject the entire space from EPSG:4326 to web-mercator (EPSG:3857)
        // To fix this problem, we project the entire target space back to the source and compute the intersection
        val targetReversed: Boolean = CRS.getAxisOrder(targetCRS) == AxisOrder.NORTH_EAST
        val targetDomainCorners = if (targetSRID == 3857) {
          // The code in the else block is supposed to work for any CRS but the extents are not accurate
          // At least for the EPSG:3857. So we do it manually since this is a popular one for visualization and
          // we do not want to mess it up
          val maxLatitude = (2 * Math.atan(Math.exp(Math.PI)) - Math.PI / 2) * 180.0 / Math.PI
          if (!targetReversed)
            Array[Double](-180, maxLatitude, 180, -maxLatitude)
          else
            Array[Double](maxLatitude, -180, -maxLatitude, 180)
        } else {
          val geographicExtents = targetCRS.getDomainOfValidity.getGeographicElements.toArray()(0).asInstanceOf[GeographicBoundingBox]
          val targetDomainCorners = new Array[Double](4)
          targetDomainCorners(0) = geographicExtents.getWestBoundLongitude
          targetDomainCorners(1) = geographicExtents.getNorthBoundLatitude
          targetDomainCorners(2) = geographicExtents.getEastBoundLongitude
          targetDomainCorners(3) = geographicExtents.getSouthBoundLatitude
          Reprojector.reprojectEnvelopeInPlace(targetDomainCorners, 4326, this.srid)
          targetDomainCorners
        }
        // Compute the intersection
        corners(0) = x1
        corners(1) = y1
        corners(2) = x2
        corners(3) = y2
        g2m.transform(corners, 0, corners, 0, 2)
        corners(0) = corners(0) max targetDomainCorners(0)
        corners(1) = corners(1) min targetDomainCorners(1)
        corners(2) = corners(2) min targetDomainCorners(2)
        corners(3) = corners(3) max targetDomainCorners(3)
        // Project back the intersected area only
        Reprojector.reprojectEnvelopeInPlace(corners, this.srid, targetSRID)
    }
    RasterMetadata.create(corners(0), corners(1), corners(2), corners(3), targetSRID,
      this.rasterWidth, this.rasterHeight, this.tileWidth, this.tileHeight)
  }

  /**
   * Creates a new raster metadata with the same information as this one but with different tile width and height
   * @param newTileWidth the new tile width in the new raster metadata
   * @param newTileHeight the new tile height in the new raster metadata
   * @return a new raster metadata that is similar to this one except for the tile width and height
   */
  def retile(newTileWidth: Int, newTileHeight: Int): RasterMetadata =
    new RasterMetadata(this.x1, this.y1, this.x2, this.y2, newTileWidth, newTileHeight, this.srid, this.g2m)

  /**
   * Returns a new [[RasterMetadata]] that contains only the given tile ID which will have a tile ID zero in the result
   * @param tileID the tile ID to keep in the resulting metadata
   * @return a new metadata that contains a single tile that points to the same geographical location pointed
   *         by the given tile.
   */
  def forOneTile(tileID: Int): RasterMetadata = {
    val tileBounds = Array[Double](getTileX1(tileID), getTileY1(tileID), getTileX2(tileID) + 1, getTileY2(tileID) + 1)
    this.g2m.transform(tileBounds, 0, tileBounds, 0, 2)
    val tileWidth: Int = getTileX2(tileID) - getTileX1(tileID) + 1
    val tileHeight: Int = getTileY2(tileID) - getTileY1(tileID) + 1
    RasterMetadata.create(tileBounds(0), tileBounds(1), tileBounds(2), tileBounds(3), this.srid,
      tileWidth, tileHeight, tileWidth, tileHeight)
  }
}

object RasterMetadata {
  /**
   * Create a raster metadata that represents a geographical region provided by a rectangle.
   * @param x1 the x-coordinate of the left edge of the pixel at (0, 0)
   * @param y1 the y-coordinate of the top edge of the pixel at (0, 0)
   * @param x2 the x-coordinate of the right edge of the pixel at (rasterWidth - 1, rasterHeight - 1)
   * @param y2 the y-coordinate of the bottom edge of the pixel at (rasterWidth - 1, rasterHeight - 1)
   * @param srid the SRID that represents the coordinate reference system of the raster
   * @param rasterWidth the number of columns in the entire raster
   * @param rasterHeight the number of rows in the entire raster
   * @param tileWidth the width of each tile in pixels
   * @param tileHeight the height of each tile in pixels
   * @return a raster metadata wit the given information
   */
  def create(x1: Double, y1:Double, x2: Double, y2:Double, srid: Int,
             rasterWidth: Int, rasterHeight: Int, tileWidth: Int, tileHeight: Int): RasterMetadata = {
    val g2m = new AffineTransform()
    g2m.translate(x1, y1)
    g2m.scale((x2 - x1) / rasterWidth, (y2 - y1) / rasterHeight)
    new RasterMetadata(0, 0, rasterWidth, rasterHeight, tileWidth, tileHeight, srid, g2m)
  }

  /**
   * Return a list of all distinct RasterMetadata in the input
   * @param raster a set of tiles
   * @return a list of distinct metadata in all input tiles
   */
  def allMetadata[T](raster: RDD[ITile[T]]): Array[RasterMetadata] =
    // For efficiency, we assume that all metadata in each partition is the same
    raster.mapPartitions(tiles => {
        if (tiles.hasNext) {
          Some(tiles.next().rasterMetadata).iterator
        } else {
          Option.empty[RasterMetadata].iterator
        }
      }).distinct()
      .collect()

  /**
   * Creates a new RasterMetadata that scales the input space covered by all input metadata into the given
   * width and height in pixels
   * @param metadata a list of metadata to combine and rescale
   * @return one raster metadata that combines all inputs
   */
  def rescale(metadata: Array[RasterMetadata], rasterWidth: Int, rasterHeight: Int): RasterMetadata = {
    val targetSRID: Int = metadata.head.srid
    var minX: Double = Double.PositiveInfinity
    var minY: Double = Double.PositiveInfinity
    var maxX: Double = Double.NegativeInfinity
    var maxY: Double = Double.NegativeInfinity
    val point1 = new Point2D.Double()
    val point2 = new Point2D.Double()
    for (m <- metadata) {
      m.gridToModel(m.x1, m.y1, point1)
      m.gridToModel(m.x2, m.y2, point2)
      val envelope = Array(point1.x, point1.y, point2.x, point2.y)
      if (m.srid != targetSRID) {
        // Convert the two points to the target SRID
        Reprojector.reprojectEnvelopeInPlace(envelope, m.srid, targetSRID)
      }
      minX = minX min envelope(0) min envelope(2)
      minY = minY min envelope(1) min envelope(3)
      maxX = maxX max envelope(0) max envelope(2)
      maxY = maxY max envelope(1) max envelope(3)
    }
    RasterMetadata.create(minX, maxY, maxX, minY, targetSRID, rasterWidth, rasterHeight,
      metadata.head.tileWidth, metadata.head.tileHeight)
  }

  /**
   * Creates a new [[RasterMetadata]] that reprojects all the given metadata into one with the target CRS
   * @param metadata the list of metadata to combine
   * @param targetCRS the target coordinate reference system (CRS)
   * @return a unified metadata that covers the space of all input metadata with a reasonable resolution
   */
  def reproject(metadata: Array[RasterMetadata], targetCRS: CoordinateReferenceSystem): RasterMetadata = {
    val targetSRID: Int = CRSServer.crsToSRID(targetCRS)
    var minX: Double = Double.PositiveInfinity
    var minY: Double = Double.PositiveInfinity
    var maxX: Double = Double.NegativeInfinity
    var maxY: Double = Double.NegativeInfinity
    // Resolution along the x and y axes is the size of each pixel in model space (model units/pixel)
    var targetResX: Double = 0.0
    var targetResY: Double = 0.0
    val point1 = new Point2D.Double()
    val point2 = new Point2D.Double()
    for (m <- metadata) {
      m.gridToModel(m.x1, m.y1, point1)
      m.gridToModel(m.x2, m.y2, point2)
      val envelope = Array(point1.x, point1.y, point2.x, point2.y)
      if (m.srid != targetSRID) {
        // Convert the two points to the target SRID
        Reprojector.reprojectEnvelopeInPlace(envelope, m.srid, targetSRID)
      }
      targetResX += (envelope(2) - envelope(0)).abs / m.rasterWidth
      targetResY += (envelope(3) - envelope(1)).abs / m.rasterHeight
      minX = minX min envelope(0) min envelope(2)
      minY = minY min envelope(1) min envelope(3)
      maxX = maxX max envelope(0) max envelope(2)
      maxY = maxY max envelope(1) max envelope(3)
    }
    targetResX /= metadata.length
    targetResY /= metadata.length
    val targetRes = (targetResX + targetResY) / 2
    RasterMetadata.create(minX, maxY, maxX, minY, targetSRID,
      ((maxX - minX) / targetRes).toInt, ((maxY - minY) / targetRes).toInt,
      metadata.head.tileWidth, metadata.head.tileHeight)
  }
}
