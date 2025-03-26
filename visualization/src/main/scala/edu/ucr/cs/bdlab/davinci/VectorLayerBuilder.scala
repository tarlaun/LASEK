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
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.{GeometryType, IFeature}
import edu.ucr.cs.bdlab.davinci.VectorTile.Tile
import edu.ucr.cs.bdlab.davinci.VectorTile.Tile.{Feature, Layer}
import org.apache.spark.sql.Row
import org.locationtech.jts.geom.{CoordinateSequence, Geometry, LineString, Polygon}

import scala.collection.mutable

/**
 * Creates a layer from a set of features.
 * [[https://docs.mapbox.com/data/tilesets/guides/vector-tiles-standards/]]
 * [[https://github.com/mapbox/vector-tile-spec/tree/master/2.1]]
 * This class does not make any conversion from world space to image space. In other words, it assumes that all
 * geometries are already converted to the correct image space. Also, it does not clip geometries outside the image
 * assuming that they have already been clipped. In short, this class only cares about encoding geometries and
 * other attributes into the correct format for vector tiles.
 * @param mbr the minimum bounding rectangle of the space that encloses this layer
 * @param resolution the resolution of this layer (both width and height are the same)
 * @param name the name of this layer
 */
class VectorLayerBuilder(resolution: Int, name: String)  {
  import VectorLayerBuilder._
  /** A map from all unique names to its index */
  val names = new mutable.HashMap[String, Int]()

  /** A map from each unique value to an index */
  val values = new mutable.HashMap[Any, Int]()

  /** The builder of the layer */
  val layerBuilder: VectorTile.Tile.Layer.Builder = Layer.newBuilder()
    .setVersion(2)
    .setName(name)
    .setExtent(resolution)

  /**
   * Adds a feature to this layer
   * @param feature the feature to add to the layer
   */
  def addFeature(feature: IFeature): Unit = {
    val featureBuilder: Feature.Builder = Feature.newBuilder()
    for (i <- feature.iNonGeom) {
      // Convert the name of the non-geometry attribute to an integer
      val name = feature.getName(i)
      if (name != null && !feature.isNullAt(i)) {
        val iKey = names.getOrElseUpdate(name, {
          layerBuilder.addKeys(name)
          layerBuilder.getKeysCount - 1
        })
        featureBuilder.addTags(iKey)
        // Convert the value of the non-geometry attribute to an integer
        val value: Any = feature.get(i)
        assert(value != null)
        val iValue = values.getOrElseUpdate(value, {
          val valueBuilder = VectorTile.Tile.Value.newBuilder()
          value match {
            case x: Int => valueBuilder.setIntValue(x)
            case x: Float => valueBuilder.setFloatValue(x)
            case x: Double => valueBuilder.setDoubleValue(x)
            case x: Boolean => valueBuilder.setBoolValue(x)
            case x: String => valueBuilder.setStringValue(x)
            case _ => valueBuilder.setStringValue(value.toString)
          }
          layerBuilder.addValues(valueBuilder.build())
          layerBuilder.getValuesCount - 1
        })
        featureBuilder.addTags(iValue)
      }
    }
    // Convert the geometry to MVT format
    val geometry = feature.getGeometry
    encodeGeometry(geometry, featureBuilder)
    layerBuilder.addFeatures(featureBuilder.build())
  }

  def addFeature(feature: Row, geometry: LiteGeometry): Unit = {
    val featureBuilder: Feature.Builder = Feature.newBuilder()
    if (feature != null) {
      for (i <- 0 until feature.size) {
        // Convert the name of the non-geometry attribute to an integer
        val name = feature.schema.fieldNames(i)
        if (name != null && !feature.isNullAt(i)) {
          val iKey = names.getOrElseUpdate(name, {
            layerBuilder.addKeys(name)
            layerBuilder.getKeysCount - 1
          })
          featureBuilder.addTags(iKey)
          // Convert the value of the non-geometry attribute to an integer
          val value: Any = feature.get(i)
          assert(value != null)
          val iValue = values.getOrElseUpdate(value, {
            val valueBuilder = VectorTile.Tile.Value.newBuilder()
            value match {
              case x: Int => valueBuilder.setIntValue(x)
              case x: Float => valueBuilder.setFloatValue(x)
              case x: Double => valueBuilder.setDoubleValue(x)
              case x: Boolean => valueBuilder.setBoolValue(x)
              case x: String => valueBuilder.setStringValue(x)
              case _ => valueBuilder.setStringValue(value.toString)
            }
            layerBuilder.addValues(valueBuilder.build())
            layerBuilder.getValuesCount - 1
          })
          featureBuilder.addTags(iValue)
        }
      }
    }
    // Convert the geometry to MVT format
    encodeGeometry(geometry, featureBuilder)
    layerBuilder.addFeatures(featureBuilder.build())
  }

  /**
   * Finalize the layer and return it
   * @return
   */
  def build(): VectorTile.Tile.Layer = this.layerBuilder.build()
}

object VectorLayerBuilder {
  /** Moves the cursor to the given point */
  val MoveToCommand: Int = 1

  /** Draws a line from the cursor with a given distance on the x and y axes */
  val LineToCommand: Int = 2

  /** Close the current path back to its starting point */
  val ClosePathCommand: Int = 7

  /** Encodes a value using Zigzag encoding */
  def zigzagEncode(x: Int): Int =  (x >> 31) ^ (x << 1)

  /** Decodes a value from Zigzag encoding */
  def zigzagDecode(x: Int): Int = (x >>> 1) ^ -(x & 1)

  /** Encodes the two parts of the command */
  def encodeCommand(command: Int, count: Int): Int = command | (count << 3)

  /**
   * Check if the given coordinate sequence is stored in clock-wise order.
   * https://stackoverflow.com/questions/1165647/how-to-determine-if-a-list-of-polygon-points-are-in-clockwise-order
   * @param cs
   * @return
   */
  private def isClockWiseOrder(cs: CoordinateSequence): Boolean = {
    var sum: Double = 0.0
    var x1 = cs.getX(0)
    var y1 = cs.getY(0)
    for (i <- 1 until cs.size()) {
      val x2 = cs.getX(i)
      val y2 = cs.getY(i)
      sum += (x2 - x1) * (y2 + y1)
      x1 = x2
      y1 = y2
    }
    sum > 0
  }

  /**
   * Reverses the order of the points in a coordinate sequence
   * @param cs
   * @return
   */
  private def reverse(cs: CoordinateSequence): CoordinateSequence = {
    val reversedCS = cs.copy()
    for (i <- 0 until reversedCS.size()) {
      reversedCS.setOrdinate(i, 0, cs.getOrdinate(cs.size() - i - 1, 0))
      reversedCS.setOrdinate(i, 1, cs.getOrdinate(cs.size() - i - 1, 1))
    }
    reversedCS
  }

  /**
   * Encodes a geometry into the given feature
   * @param geometry the geometry to encode
   * @param featureBuilder the feature builder to encode to
   */
  private def encodeGeometry(geometry: Geometry, featureBuilder: Feature.Builder): Unit = {
    // Write the geometry to the output
    geometry.getGeometryType match {
      case GeometryType.PointName =>
        featureBuilder.setType(Tile.GeomType.POINT)
        // Move to command
        featureBuilder.addGeometry(encodeCommand(MoveToCommand, 1))
        // Set the point location as (x, y)
        val coordinate = geometry.getCoordinate
        featureBuilder.addGeometry(zigzagEncode(coordinate.x.round.toInt))
        featureBuilder.addGeometry(zigzagEncode(coordinate.y.round.toInt))
      case GeometryType.LineStringName =>
        featureBuilder.setType(Tile.GeomType.LINESTRING)
        val lineString = geometry.asInstanceOf[LineString]
        // Get the coordinates and convert to image coordinates in pixels
        val coords = lineString.getCoordinateSequence
        encodeCoordinateSequence(coords, featureBuilder, false, 0, 0)
      case GeometryType.PolygonName =>
        featureBuilder.setType(Tile.GeomType.POLYGON)
        val polygon = geometry.asInstanceOf[Polygon]
        encodePolygon(polygon, featureBuilder, 0, 0)
      case GeometryType.MultiPointName =>
        featureBuilder.setType(Tile.GeomType.POINT)
        // MultiPoint is encoded as a list of moveto commands
        featureBuilder.addGeometry(encodeCommand(MoveToCommand, geometry.getNumGeometries))
        for (i <- 0 until geometry.getNumGeometries) {
          val coordinate = geometry.getGeometryN(i).getCoordinate
          featureBuilder.addGeometry(zigzagEncode(coordinate.x.round.toInt))
          featureBuilder.addGeometry(zigzagEncode(coordinate.y.round.toInt))
        }
      case GeometryType.MultiLineStringName =>
        featureBuilder.setType(Tile.GeomType.LINESTRING)
        var lastPoint: (Int, Int) = (0, 0)
        for (i <- 0 until geometry.getNumGeometries) {
          val coordinates = geometry.getGeometryN(i).asInstanceOf[LineString].getCoordinateSequence
          lastPoint = encodeCoordinateSequence(coordinates, featureBuilder, false, lastPoint._1, lastPoint._2)
        }
      case GeometryType.MultiPolygonName =>
        featureBuilder.setType(Tile.GeomType.POLYGON)
        var lastPoint: (Int, Int) = (0, 0)
        for (i <- 0 until geometry.getNumGeometries) {
          val polygon = geometry.getGeometryN(i).asInstanceOf[Polygon]
          lastPoint = encodePolygon(polygon, featureBuilder, lastPoint._1, lastPoint._2)
        }
      case _ => throw new RuntimeException(s"Unsupported geometry '$geometry'")
    }
  }

  private def encodeGeometry(geometry: LiteGeometry, featureBuilder: Feature.Builder): Unit = {
    // Write the geometry to the output
    geometry match {
      case p : LitePoint =>
        featureBuilder.setType(Tile.GeomType.POINT)
        // Move to command
        featureBuilder.addGeometry(encodeCommand(MoveToCommand, 1))
        // Set the point location as (x, y)
        featureBuilder.addGeometry(zigzagEncode(p.x))
        featureBuilder.addGeometry(zigzagEncode(p.y))
      case mp : LiteMultiPoint =>
        featureBuilder.setType(Tile.GeomType.POINT)
        // Move to command
        featureBuilder.addGeometry(encodeCommand(MoveToCommand, mp.numPoints))
        // Set the point location as (x, y)
        var x0: Short = 0
        var y0: Short = 0
        for (i <- 0 until mp.numPoints) {
          featureBuilder.addGeometry(zigzagEncode(mp.xs(i) - x0))
          featureBuilder.addGeometry(zigzagEncode(mp.ys(i) - y0))
          x0 = mp.xs(i)
          y0 = mp.ys(i)
        }
      case lineString : LiteLineString =>
        featureBuilder.setType(Tile.GeomType.LINESTRING)
        // Get the coordinates and convert to image coordinates in pixels
        var lastPoint: (Int, Int) = (0, 0)
        for (part <- lineString.parts)
          lastPoint = encodeCoordinates(part.xs, part.ys, featureBuilder, false, lastPoint._1, lastPoint._2)
      case polygon: LitePolygon =>
        featureBuilder.setType(Tile.GeomType.POLYGON)
        var lastPoint: (Int, Int) = (0, 0)
        for (part <- polygon.parts)
          lastPoint = encodeCoordinates(part.xs, part.ys, featureBuilder, true, lastPoint._1, lastPoint._2)
      case _ => throw new RuntimeException(s"Unsupported geometry '$geometry'")
    }
  }

  /**
   * Encodes a coordinate sequence into a feature.
   * @param coords the coordinate sequence to encode.
   * @param featureBuilder the feature to encode to
   * @param ring whether this is a closed ring or not
   */
  private def encodeCoordinateSequence(coords: CoordinateSequence, featureBuilder: Feature.Builder,
                                       ring: Boolean, x0: Int, y0: Int): (Int, Int) = {
    var numPoints: Int = 0
    val xs = new Array[Int](coords.size())
    val ys = new Array[Int](coords.size())
    for (i <- xs.indices) {
      xs(numPoints) = coords.getX(i).round.toInt
      ys(numPoints) = coords.getY(i).round.toInt
      // Skip the point if it ends up at the same pixel location
      if (i == 0 || xs(numPoints) != xs(numPoints - 1) || ys(numPoints) != ys(numPoints - 1))
        numPoints += 1
    }
    // Move to the first point
    featureBuilder.addGeometry(encodeCommand(MoveToCommand, 1))
    featureBuilder.addGeometry(zigzagEncode(xs(0) - x0))
    featureBuilder.addGeometry(zigzagEncode(ys(0) - y0))
    if (numPoints > 1) {
      // If the coordinate sequence represents a closed ring, ignore the last point and close the path instead
      if (ring)
        numPoints -= 1
      // Lineto remaining points
      featureBuilder.addGeometry(encodeCommand(LineToCommand, numPoints - 1))
      for (i <- 1 until numPoints) {
        val dx = xs(i) - xs(i - 1)
        val dy = ys(i) - ys(i - 1)
        featureBuilder.addGeometry(zigzagEncode(dx))
        featureBuilder.addGeometry(zigzagEncode(dy))
      }
      if (ring) {
        // Close the ring
        featureBuilder.addGeometry(encodeCommand(ClosePathCommand, 1))
      }
    }
    (xs(numPoints - 1), ys(numPoints - 1))
  }

  private def encodeCoordinates(xs: Array[Short], ys: Array[Short], featureBuilder: Feature.Builder,
                                       ring: Boolean, x0: Int, y0: Int): (Int, Int) = {
    // Move to the first point
    featureBuilder.addGeometry(encodeCommand(MoveToCommand, 1))
    featureBuilder.addGeometry(zigzagEncode(xs(0) - x0))
    featureBuilder.addGeometry(zigzagEncode(ys(0) - y0))
    var numPoints = xs.length
    if (xs.length > 1) {
      // If the coordinate sequence represents a closed ring, ignore the last point and close the path instead
      if (ring)
        numPoints -= 1
      // Lineto remaining points
      featureBuilder.addGeometry(encodeCommand(LineToCommand, numPoints - 1))
      for (i <- 1 until numPoints) {
        val dx = xs(i) - xs(i - 1)
        val dy = ys(i) - ys(i - 1)
        featureBuilder.addGeometry(zigzagEncode(dx))
        featureBuilder.addGeometry(zigzagEncode(dy))
      }
      if (ring) {
        // Close the ring
        featureBuilder.addGeometry(encodeCommand(ClosePathCommand, 1))
      }
    }
    (xs(numPoints - 1), ys(numPoints - 1))
  }

  /**
   * Encodes the geometry part of the polygon
   * @param polygon the polygon to encode
   * @param featureBuilder the feature to encode to
   * @param x0 the x-coordinate of the last point to make the first move to command relative to it
   * @param y0 the y-coordinate of the last point to make the first move to command relative to it
   * @return the coordinates of the last point
   */
  def encodePolygon(polygon: Polygon, featureBuilder: Feature.Builder, x0: Int, y0: Int): (Int, Int) = {
    val outerShell = polygon.getExteriorRing
    var coords = outerShell.getCoordinateSequence
    if (!isClockWiseOrder(coords))
      coords = reverse(coords)
    var lastPoint: (Int, Int) = encodeCoordinateSequence(coords, featureBuilder, true, x0, y0)
    for (n <- 0 until polygon.getNumInteriorRing) {
      val hole = polygon.getInteriorRingN(n)
      coords = hole.getCoordinateSequence
      if (isClockWiseOrder(coords))
        coords = reverse(coords)
      lastPoint = encodeCoordinateSequence(coords, featureBuilder, true, lastPoint._1, lastPoint._2)
    }
    lastPoint
  }
}