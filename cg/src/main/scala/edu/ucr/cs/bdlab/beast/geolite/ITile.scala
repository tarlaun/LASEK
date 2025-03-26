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
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.locationtech.jts.geom.{Coordinate, CoordinateXY, Geometry}

import java.awt.geom.Point2D

/**
 * An interface that represents a tile
 * @tparam T the type of measurement values in tiles
 * @param tileID the ID of this tile in the raster metadata
 * @param rasterMetadata the metadata of the underlying raster
 */
@DefaultSerializer(classOf[ITileSerializer[Any]])
abstract class ITile[T](val tileID: Int, val rasterMetadata: RasterMetadata) extends java.io.Serializable {

  // The following values can be functions but we make them as variables for efficiency to avoid calling
  // the corresponding functions over and over

  /** The lowest index of the column of this tile (inclusive)*/
  val x1: Int = rasterMetadata.getTileX1(tileID)

  /**The highest index of the column of this tile (inclusive)*/
  val x2: Int = rasterMetadata.getTileX2(tileID)

  /**Number of columns in the tile*/
  val tileWidth: Int = x2 - x1 + 1

  /** The lowest index of a row of this tile (inclusive) */
  val y1: Int = rasterMetadata.getTileY1(tileID)

  /** The highest index of a row of this tile (inclusive)*/
  val y2: Int = rasterMetadata.getTileY2(tileID)

  /**Total number of scan lines in this tile*/
  val tileHeight: Int = y2 - y1 + 1

  /**
   * The value of the given pixel which depends on the tile data type and number of bands.
   * For example, if the data type is integer with three bands, it will return an integer array of 3.
   * If the data type is float and one band, it will return a single float value.
   * @param i the index of the column
   * @param j the index of the row
   * @return the pixel value
   */
  def getPixelValue(i: Int, j: Int): T

  /**
   * An iterator that goes over all pixels in this tile
   * @return an iterator that goes over all pixels (whether empty or not) in this tile
   */
  def pixelLocations: Iterator[(Int, Int)] =
    for (y <- (y1 to y2).iterator; x <- (x1 to x2).iterator) yield (x, y)

  def pixels: Iterator[(Int, Int, T)] =
    for (y <- (y1 to y2).iterator; x <- (x1 to x2).iterator; if isDefined(x, y)) yield (x, y, getPixelValue(x, y))

  /**
   * Return the value of the pixel that contains the given point at model (world) coordinates.
   * @param x the x-coordinate of the point, e.g., longitude
   * @param y the y-coordinate of the point, e.g., latitude
   * @return the value of all components of the given pixel
   */
  def getPointValue(x: Double, y: Double): T = {
    val pixel: Point2D.Double = new Point2D.Double()
    rasterMetadata.modelToGrid(x, y, pixel)
    getPixelValue(pixel.x.toInt, pixel.y.toInt)
  }

  /**
   * Check whether the given pixel is empty or not. A pixel is empty if it has no data or if its pixel value is
   * equal to a fill value defined by the raster layer.
   * @param i the index of the column
   * @param j the index of the row
   * @return `false` if pixel has a valid value or `true` if it does not.
   */
  def isEmpty(i: Int, j: Int): Boolean

  /**
   * Check if the pixel that contains the given location is empty
   * @param x the x-coordinate of the point, e.g., longitude
   * @param y the y-coordinate of the point, e.g., latitude
   * @return `true` if the pixel at the location is empty, i.e., contains no data
   */
  def isEmptyAt(x: Double, y: Double): Boolean = {
    val pixel: Point2D.Double = new Point2D.Double()
    rasterMetadata.modelToGrid(x, y, pixel)
    isEmpty(pixel.x.toInt, pixel.y.toInt)
  }

  /**
   * Checks if the given pixel is defined (not empty)
   * @param i the index of the column
   * @param j the index of the row
   * @return `true` if pixel has a valid value or `false` if it does not.
   */
  @inline def isDefined(i: Int, j: Int): Boolean = !isEmpty(i, j)

  /**
   * Number of components for each pixel in this raster file.
   * @return the number of components in each pixel, e.g., 3 for RGB or 1 for grayscale
   */
  def numComponents: Int

  /**
   * Returns the type of the pixel as an SQL data type
   * @return the type of the pixel values
   */
  def pixelType: DataType = if (numComponents == 1)
    componentType
  else
    new ArrayType(componentType, false)

  def componentType: DataType

  override def toString: String =
    s"Tile #$tileID ($x1,$y1) - ($x2,$y2)"

  /**
   * Returns a polygon that represents the boundaries of this tile in the model space.
   * @return a polygon (rectangle) that represents the boundaries of this tile
   */
  def extents: Geometry = {
    val values = Array[Double](x1, y1, x2+1, y1, x2+1, y2+1, x1, y2+1, x1, y1)
    rasterMetadata.g2m.transform(values, 0, values, 0, 5)
    val coords = new Array[Coordinate](values.length / 2)
    for (i <- values.indices by 2) {
      coords(i / 2) = new CoordinateXY(values(i), values(i+1))
    }
    val polygon = GeometryReader.DefaultGeometryFactory.createPolygon(coords)
    polygon.setSRID(rasterMetadata.srid)
    polygon
  }
}
