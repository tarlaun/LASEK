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

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{Feature, GeometryType, IFeature}
import edu.ucr.cs.bdlab.beast.util.BitArray
import edu.ucr.cs.bdlab.davinci.IntermediateVectorTile._
import org.apache.spark.sql.Row
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.opengis.referencing.operation.MathTransform

import scala.collection.mutable.ArrayBuffer

/**
 * A class that stores intermediate data to build vector tiles.
 *
 * @param resolution the resolution of the tile in pixels.
 * @param buffer the additional pixels around the tile that should be tracked.
 * @param dataToImage a math transform that transforms images. If `null`, no transformation is applied
 */
@DefaultSerializer(classOf[IntermediateVectorTileSerializer])
class IntermediateVectorTile(private[davinci] val resolution: Int, private[davinci] val buffer: Int,
                             @transient private[davinci] val dataToImage: MathTransform = null) extends Serializable {

  /** A list of non-spatial attributes for features that are stored in full */
  val features: collection.mutable.ArrayBuffer[Row] = new collection.mutable.ArrayBuffer[Row]()

  /** A list of geometries that are stored in full */
  val geometries: collection.mutable.ArrayBuffer[LiteGeometry] = new collection.mutable.ArrayBuffer[LiteGeometry]()

  /** Counts total number of points of all features in the list [[geometries]] */
  var numPoints: Int = 0

  /** A bit map that marks which pixels are occupied by the interior of geometries */
  var interiorPixels: BitArray = _

  /** A bit map that marks which pixels are at the boundaries of geometries */
  var boundaryPixels: BitArray = _

  @transient private var _tileMBR: PreparedGeometry = _

  def tileMBR(factory: GeometryFactory): PreparedGeometry = {
    if (_tileMBR == null) {
      val mbr = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
      if (dataToImage != null) {
        val imageToData: MathTransform = dataToImage.inverse()
        imageToData.transform(mbr, 0, mbr, 0, 2)
      }
      val envelope = new Envelope(mbr(0), mbr(2), mbr(1), mbr(3))
      _tileMBR = PreparedGeometryFactory.prepare(factory.toGeometry(envelope))
    }
    _tileMBR
  }

  // TODO store additional aggregated attribute, i.e., non-spatial attributes

  def isEmpty: Boolean = numPoints == 0 && interiorPixels == null

  def nonEmpty: Boolean = !isEmpty

  /**
   * Add the given feature to this tile.
   * The feature is either added as-is, or rasterized and aggregated, depending on the state of this tile.
   * @param feature the feature to add
   * @return this tile to allow chaining other operations
   */
  def addFeature(feature: IFeature): IntermediateVectorTile = {
    val mbr = tileMBR(feature.getGeometry.getFactory)
    if (mbr.disjoint(feature.getGeometry))
      return this
    val simplifiedGeometry = simplifyGeometry(feature.getGeometry)
    if (simplifiedGeometry == null)
      return this
    if (!this.isRasterized) {
      // Still collecting features in full
      geometries.append(simplifiedGeometry)
      features.append(Feature.create(feature, null))
      numPoints += simplifiedGeometry.numPoints

      if (shouldRasterize) {
        interiorPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
        boundaryPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
        for (g <- geometries)
          rasterizeGeometry(g)
        features.clear()
        geometries.clear()
      }
    } else {
      rasterizeGeometry(simplifiedGeometry)
    }
    this
  }

  /** Whether this tile is in the rasterized space or not */
  @inline private[davinci] def isRasterized: Boolean = this.interiorPixels != null

  /** Returns ture if this tile should be rasterized */
  @inline private[davinci] def shouldRasterize: Boolean =
    numPoints > maxPointsPerTile || geometries.length > maxFeaturesPerTile

  @inline private def isVisible(x: Short, y: Short): Boolean = x >= -buffer && x <= resolution + buffer &&
    y >= -buffer && y <= resolution + buffer

  @inline private def isVisible(x: Double, y: Double): Boolean = x >= -buffer && x <= resolution + buffer &&
    y >= -buffer && y <= resolution + buffer

  /**
   * Tests if the given point is at one of the corners of the tile + buffer.
   * That is, it returns true if the point is one of the four points:
   *  1. (-buffer, -buffer)
   *  1. (-buffer, buffer+resolution)
   *  1. (buffer+resolution, -buffer)
   *  1. (buffer+resolution, buffer+resolution)
   * This function is used mainly for debugging.
   * @param x the x-coordinate of the point to test
   * @param y the y-coordinate of the point to test
   * @return `true` if the given point is at one of the four corners.
   */
  @inline private def isCorner(x: Short, y: Short): Boolean =
    (x - resolution / 2).abs + (y - resolution / 2).abs == (resolution + 2 * buffer)

  /**
   * Trim a line segment with the boundaries of this tile,
   * i.e., the box (-buffer, -buffer, resolution + buffer, resolution + buffer)
   * @param x1 the x-coordinate of the first point
   * @param y1 the y-coordinate of the first point
   * @param x2 the x-coordinate of the second point
   * @param y2 the y-coordinate of the second point
   * @return the given line segment trimmed to the tile boundaries or null if the line segment is completely outside
   */
  private[davinci] def trimLineSegment(x1: Double, y1: Double, x2: Double, y2: Double):
  (Double, Double, Double, Double) = {
    // For efficiency, handle the easy case directly where both end points are within the range
    if (isVisible(x1, y1) && isVisible(x2, y2))
      return (x1, y1, x2, y2)
    var (_x1: Double, _y1: Double, _x2: Double, _y2: Double) = (x1, y1, x2, y2)
    var reversed: Boolean = false
    // Reverse the two end points
    def reverse(): Unit = {
      var t: Double = 0
      t = _x1; _x1 = _x2; _x2 = t
      t = _y1; _y1 = _y2; _y2 = t
      reversed = !reversed
    }
    // Find the intersections to the two vertical lines x=-buffer and x=resolution + buffer
    if (_x1 > _x2)
      reverse()
    if (_x2 < -buffer) return null
    if (_x1 > resolution + buffer) return null
    if (_x1 < -buffer) {
      // Compute intersection with left line, x=-buffer
      _y1 = ((-buffer - _x1) * _y2 + (_x2 + buffer) * _y1) / (_x2 - _x1)
      _x1 = -buffer
    }
    if (_x2 > resolution + buffer) {
      // Compute intersection with left line, x=resolution + buffer
      _y2 = ((resolution + buffer - _x1) * _y2 + (_x2 - resolution - buffer) * _y1) / (_x2 - _x1)
      _x2 = resolution + buffer
    }
    // Find the intersections to the two horizontal lines y=-buffer and y=resolution + buffer
    if (_y1 > _y2)
      reverse()
    if (_y2 < -buffer) return null
    if (_y1 > resolution + buffer) return null
    if (_y1 < -buffer) {
      // Compute intersection with left line, x=-buffer
      _x1 = ((-buffer - _y1) * _x2 + (_y2 + buffer) * _x1) / (_y2 - _y1)
      _y1 = -buffer
    }
    if (_y2 > resolution + buffer) {
      // Compute intersection with left line, x=resolution + buffer
      _x2 = ((resolution + buffer - _y1) * _x2 + (_y2 - resolution - buffer) * _x1) / (_y2 - _y1)
      _y2 = resolution + buffer
    }
    // Revert to the original ordering before returning the result
    if (reversed)
      reverse()
    (_x1, _y1, _x2, _y2)
  }

  /**
   * Takes a geometry that is already projected to the image space of this tile and returns a simplified lite geometry
   * that satisfies the following:
   *  - If the geometry does not overlap with the tile boundaries, null is returned
   *  - Coordinates are snapped to the nearest integer
   *  - Parts of the geometry that are outside the tile boundaries can be simplified without affecting the portion
   *    that is within the tile boundaries.
   *  - If there are consecutive coordinates that snap to the same pixel, only one can be kept
   * @param geometry the input geometry
   * @return the simplified geometry or null if empty
   */
  private[davinci] def simplifyGeometry(geometry: Geometry): LiteGeometry = geometry.getGeometryType match {
    case GeometryType.PointName =>
      val coord: Coordinate = geometry.getCoordinate
      val pt = Array[Double](coord.x, coord.y)
      if (dataToImage != null)
        dataToImage.transform(pt, 0, pt, 0, 1)
      val x = pt(0).round.toShort
      val y = pt(1).round.toShort
      if (isVisible(x, y)) new LitePoint(x, y) else null
    case GeometryType.LineStringName =>
      val coords: CoordinateSequence = geometry.asInstanceOf[LineString].getCoordinateSequence
      val parts = new ArrayBuffer[LiteGeometry]()
      val currentPartX = new ArrayBuffer[Short]()
      val currentPartY = new ArrayBuffer[Short]()
      def addPoint(_x: Double, _y: Double): Unit = {
        val x = _x.round.toShort
        val y = _y.round.toShort
        if (currentPartX.isEmpty || currentPartX.last != x || currentPartY.last != y) {
          currentPartX.append(x)
          currentPartY.append(y)
        }
      }
      val pt = Array[Double](coords.getX(0), coords.getY(0))
      if (dataToImage != null)
        dataToImage.transform(pt, 0, pt, 0, 1)
      var x1: Double = pt(0)
      var y1: Double = pt(1)
      var startPointVisible: Boolean = isVisible(x1, y1)
      if (startPointVisible) // Add the first end point
        addPoint(x1, y1)
      for (i <- 1 until coords.size()) {
        val pt = Array[Double](coords.getX(i), coords.getY(i))
        if (dataToImage != null)
          dataToImage.transform(pt, 0, pt, 0, 1)
        val x2: Double = pt(0)
        val y2: Double = pt(1)
        val endPointVisible: Boolean = isVisible(x2, y2)
        if (i == 1 || x2.round != x1.round || y2.round != y1.round) {
          if (startPointVisible && endPointVisible) {
            // The entire line segment is visible. Append this point to the current part
            addPoint(x2, y2)
          } else if (startPointVisible && !endPointVisible) {
            // Trim the line segment and end the current part
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            assert(trimmedLine != null)
            assert(trimmedLine._1 == x1 && trimmedLine._2 == y1, s"Start point should not change ($x1,$y1)-($x2,$y2)")
            addPoint(trimmedLine._3, trimmedLine._4)
            parts.append(if (currentPartX.length == 1) new LitePoint(currentPartX.head, currentPartY.head)
                else new LiteList(currentPartX.toArray, currentPartY.toArray))
            currentPartX.clear()
            currentPartY.clear()
          } else if (!startPointVisible && endPointVisible) {
            // Trim the line segment and start a new part
            assert(currentPartX.isEmpty, "Current part should be empty")
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            assert(trimmedLine != null, "The entire line cannot be outside the tile")
            assert(x2 == trimmedLine._3 && y2 == trimmedLine._4, s"End point should not change ($x1,$y1)-($x2,$y2)")
            addPoint(trimmedLine._1, trimmedLine._2)
            addPoint(trimmedLine._3, trimmedLine._4)
          } else if (!startPointVisible && !endPointVisible) {
            // Both end-points are invisible. Either zero or two intersections with boundaries
            assert(currentPartX.isEmpty, "Current part should be empty")
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            // If zero intersections, skip this line segment
            // If two intersections, create a part that contains this segment only
            if (trimmedLine != null)
              parts.append(new LiteList(Array(trimmedLine._1.round.toShort, trimmedLine._3.round.toShort),
                Array(trimmedLine._2.round.toShort, trimmedLine._4.round.toShort)))
          }
          x1 = x2
          y1 = y2
          startPointVisible = endPointVisible
        }
      }
      if (currentPartX.nonEmpty)
        parts.append(if (currentPartX.size > 1) new LiteList(currentPartX.toArray, currentPartY.toArray)
        else new LitePoint(currentPartX.head, currentPartY.head))
      // If no parts were generated, return null
      if (parts.isEmpty)
        null
      else {
        val allPoints: Boolean = parts.forall(_.numPoints == 1)
        if (parts.length == 1 && allPoints) {
          // Return a single point
          parts.head
        } else if (allPoints) {
          // All parts consist of at most one point, return a multipoint
          val xs: Array[Short] = parts.map(_.asInstanceOf[LitePoint].x).toArray
          val ys: Array[Short] = parts.map(_.asInstanceOf[LitePoint].y).toArray
          new LiteMultiPoint(xs, ys)
        } else {
          // At least one part contains more than one point, ignore all parts that contain a single point
          new LiteLineString(parts.filter(_.numPoints > 1).map(_.asInstanceOf[LiteList]).toArray)
        }
      }
    case GeometryType.LinearRingName =>
      val coords: CoordinateSequence = geometry.asInstanceOf[LineString].getCoordinateSequence
      val pointsX = new ArrayBuffer[Short]()
      val pointsY = new ArrayBuffer[Short]()
      // Add a point to the output if it differs from the last point added
      def addPoint(_x: Double, _y: Double): Unit = {
        val x: Short = _x.round.toShort
        val y: Short = _y.round.toShort
        if (pointsX.isEmpty || pointsX.last != x || pointsY.last != y) {
          pointsX.append(x)
          pointsY.append(y)
        }
      }
      // Trace the border of the tile from (x1, y1) to (x2, y2) going in CW order or CCW order
      def traceTileEdge(x1: Short, y1: Short, x2: Short, y2: Short, cwOrdering: Boolean): Unit = {
        var currentPoint: (Short, Short) = (x1, y1)
        val endPoint: (Short, Short) = (x2, y2)
        do {
          currentPoint = if (cwOrdering) nextPointCWOrdering(currentPoint._1, currentPoint._2, endPoint._1, endPoint._2)
          else nextPointCCWOrdering(currentPoint._1, currentPoint._2, endPoint._1, endPoint._2)
          addPoint(currentPoint._1, currentPoint._2)
        } while (currentPoint != endPoint)
      }

      val pt = Array[Double](coords.getX(0), coords.getY(0))
      if (dataToImage != null)
        dataToImage.transform(pt, 0, pt, 0, 1)
      var x1: Double = pt(0)
      var y1: Double = pt(1)
      var startPointVisible: Boolean = isVisible(x1, y1)
      var sumForOrdering: Double = 0.0
      var sumForOrderingFirstPart: Double = 0.0
      if (startPointVisible) // Add the first end point
        addPoint(x1, y1)
      // Keep track of the sum to determine CW Vs CCW order. If the sum is positive, it is CCW ordering
      for (i <- 1 until coords.size()) {
        val pt = Array[Double](coords.getX(i), coords.getY(i))
        try {
          if (dataToImage != null)
            dataToImage.transform(pt, 0, pt, 0, 1)
          val x2: Double = pt(0)
          val y2: Double = pt(1)
          val endPointVisible: Boolean = isVisible(x2, y2)
          if (startPointVisible && endPointVisible) {
            // The entire line segment is visible. Append this point to the current part
            addPoint(x2, y2)
          } else if (startPointVisible && !endPointVisible) {
            // Trim the line segment and start the tracking the order (CW/CCW)
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            assert(trimmedLine != null)
            assert(trimmedLine._1 == x1 && trimmedLine._2 == y1, s"Start point should not change ($x1,$y1)-($x2,$y2)")
            addPoint(trimmedLine._3, trimmedLine._4)
            // Start tracking the cycle.
            // First segment goes from the end of the trimmed line to the end of the untrimmed line
            // We round the intersection point to make sure the calculations are correct because we will later
            //   use the same point to close the ring and we will only be able to retrieve it from the list of points
            sumForOrdering = (x2 - trimmedLine._3.round.toShort) * (y2 + trimmedLine._4.round.toShort)
          } else if (!startPointVisible && endPointVisible) {
            // Was outside and went back in.
            // Trim the last line segment to determine the entry point.
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            assert(trimmedLine != null, "The entire line cannot be outside the tile")
            assert(x2 == trimmedLine._3 && y2 == trimmedLine._4, s"End point should not change ($x1,$y1)-($x2,$y2)")
            // Complete the cycle with last two segments
            // Next to last segment is the part that was taken out of the trimmed line. Be
            sumForOrdering += (trimmedLine._1.round - x1) * (trimmedLine._2.round + y1)
            if (pointsX.nonEmpty) {
              // Last segment goes from the entry point back to the point that exited the tile
              sumForOrdering += (pointsX.last - trimmedLine._1.round) * (pointsY.last + trimmedLine._2.round)
              // Was outside and went back in. Trace the tile boundaries from the exit point ot the entry point
              val cwOrdering = sumForOrdering > 0
              traceTileEdge(pointsX.last, pointsY.last,
                trimmedLine._1.round.toShort, trimmedLine._2.round.toShort, cwOrdering)
            } else {
              // Add the entry point
              addPoint(trimmedLine._1, trimmedLine._2)
              sumForOrderingFirstPart = sumForOrdering
            }
            // Finally, append the end point of the trimmed line
            addPoint(x2, y2)
          } else if (!startPointVisible && !endPointVisible) {
            // Both end-points are invisible. Either the entire line segment remains outside or it crosses the tile
            val trimmedLine = trimLineSegment(x1, y1, x2, y2)
            // If the line does not intersect tile boundaries, continue tracking the ordering using the original points
            if (trimmedLine == null) {
              // The cycle continues with the entire segment that is outside the tile
              sumForOrdering += (x2 - x1) * (y2 + y1)
            } else {
              // If two intersections, trace the boundaries, go back inside, and start tracking the ordering again
              if (pointsX.nonEmpty) {
                // Was outside and went back in. Trace the tile boundaries from the exit point to the entry point
                // Add the last segment that completes the cycle, the part of the line that is outside the tile
                sumForOrdering += (trimmedLine._1.round - x1) * (trimmedLine._2.round + y1)
                val cwOrdering = sumForOrdering > 0
                traceTileEdge(pointsX.last, pointsY.last,
                  trimmedLine._1.round.toShort, trimmedLine._2.round.toShort, cwOrdering)
              } else {
                // First time going into the tile
                // Track the sum for ordering for the part outside and keep it until the end
                sumForOrdering += (trimmedLine._1.round - x1) * (trimmedLine._2.round + y1)
                sumForOrderingFirstPart = sumForOrdering
                // Add the first point of the trimmed line; the second point is added shortly after
                addPoint(trimmedLine._1, trimmedLine._2)
              }
              // Add the other end point of the line segment
              addPoint(trimmedLine._3, trimmedLine._4)
              // Start tracking a new cycle ordering
              // The first segment goes from the exit point (end of trimmed line) to end of untrimmed line
              sumForOrdering = (x2 - trimmedLine._3.round) * (y2 + trimmedLine._4.round)
            }
          }
          x1 = x2
          y1 = y2
          startPointVisible = endPointVisible
        } catch {
          case e: org.geotools.referencing.operation.projection.ProjectionException =>
            None
        }
      }
      // Complete the calculation for the ordering in case of needed
      if (!startPointVisible && pointsX.size == 1) {
        // Intersection is one point. Consider no intersections.
        pointsX.clear()
        pointsY.clear()
        sumForOrdering += sumForOrderingFirstPart
      } else if (!startPointVisible && pointsX.size > 1) {
        // Ended outside. Close the ring by connecting the last point to the first point
        // Add the last segment that connects the entry point (first point) the exit point (last point)
        sumForOrdering += (pointsX.last - pointsX.head) * (pointsY.last + pointsY.head)
        val cwOrdering = (sumForOrdering + sumForOrderingFirstPart) > 0
        traceTileEdge(pointsX.last, pointsY.last, pointsX.head, pointsY.head, cwOrdering)
      }
      if (pointsX.isEmpty) {
        // The ring is either completely outside the tile or completely contains it
        // Check the point at (0,0) if it is inside the ring. If so, the entire tile is inside
        pt(0) = coords.getX(0)
        pt(1) = coords.getY(0)
        if (dataToImage != null)
          dataToImage.transform(pt, 0, pt, 0, 1)
        var x1: Double = pt(0)
        var y1: Double = pt(1)
        var numIntersection: Int = 0
        for (i <- 1 until coords.size()) {
          pt(0) = coords.getX(i)
          pt(1) = coords.getY(i)
          if (dataToImage != null)
            dataToImage.transform(pt, 0, pt, 0, 1)
          val x2: Double = pt(0)
          val y2: Double = pt(1)
          // Check if there is an intersection between the ray that goes from (0,0) to the left
          if (y1 < 0 && y2 > 0 || y2 < 0 && y1 > 0) {
            val xIntersection = x1 - y1 * (x2 - x1) / (y2 - y1)
            if (xIntersection < 0)
              numIntersection += 1
          }
          x1 = x2
          y1 = y2
        }
        if (numIntersection % 2 == 1) // The ring completely contains the the point (0,) and hence th entire tile
          new LiteList(Array(-buffer, -buffer, resolution + buffer, resolution + buffer, -buffer).map(_.toShort),
            Array(-buffer, resolution + buffer, resolution + buffer, -buffer, -buffer).map(_.toShort))
        else // The ring is completely outside the tile
          null
      } else if (pointsX.size == 1) {
        // One point, return a single point
        new LitePoint(pointsX.head, pointsY.head)
      } else {
        // Return a ring
        new LiteList(pointsX.toArray, pointsY.toArray)
      }
    case GeometryType.PolygonName =>
      val polygon: Polygon = geometry.asInstanceOf[Polygon]
      val parts: Array[LiteGeometry] = new Array[LiteGeometry](polygon.getNumInteriorRing + 1)
      parts(0) = simplifyGeometry(polygon.getExteriorRing)
      var numParts: Int = 1
      // Make sure the exterior ring is big enough to actually be a ring
      if (parts(0) == null) // Empty geometry
        null
      else if (parts(0).numPoints == 1) // Single point
        parts(0)
      else if (parts(0).numPoints <= 3) // Line string
        new LiteLineString(Array(parts(0).asInstanceOf[LiteList]))
      else {
        assert(parts(0).asInstanceOf[LiteList].isClosed, s"Non-closed exterior ring ${parts(0)}")
        parts(0) match {
          case p: LiteList if p.isCCW => p.reverse()
          case _ => None
        }

        for (iRing <- 0 until polygon.getNumInteriorRing) {
          parts(numParts) = simplifyGeometry(polygon.getInteriorRingN(iRing))
          if (parts(numParts) != null && parts(numParts).numPoints > 3) {
            assert(parts(numParts).asInstanceOf[LiteList].isClosed, s"Non-closed ring ${parts(numParts)}")
            parts(numParts) match {
              case p: LiteList if p.isCW => p.reverse()
              case _ => None
            }
            numParts += 1
          }
        }
        new LitePolygon(parts.slice(0, numParts).map(_.asInstanceOf[LiteList]))
      }
    case GeometryType.MultiPointName =>
      var points: Array[(Short, Short)] = new Array[(Short, Short)](geometry.getNumPoints)
      val pt = new Array[Double](2)
      for (i <- points.indices) {
        val coord: Coordinate = geometry.getGeometryN(i).getCoordinate
        pt(0) = coord.x
        pt(1) = coord.y
        if (dataToImage != null)
          dataToImage.transform(pt, 0, pt, 0, 1)
        points(i) = (pt(0).round.toShort, pt(1).round.toShort)
      }
      points = points.filter(p => isVisible(p._1, p._2)).sorted
      var numPoints: Int = 0
      for (i <- 1 until points.length) {
        if (points(i) != points(numPoints - 1)) {
          points(numPoints) = points(i)
          numPoints += 1
        }
      }
      if (numPoints == 1)
        new LitePoint(points(0)._1, points(0)._2)
      else {
        val xs: Array[Short] = points.slice(0, numPoints).map(_._1)
        val ys: Array[Short] = points.slice(0, numPoints).map(_._2)
        new LiteMultiPoint(xs, ys)
      }
    case GeometryType.MultiLineStringName =>
      // Handle this similar to LineString and combine all parts into one
      val parts: Array[LiteGeometry] = (0 until geometry.getNumGeometries)
        .map(i => simplifyGeometry(geometry.getGeometryN(i))).filter(_ != null)
        .toArray
      LiteGeometry.combineGeometries(parts)
    case GeometryType.MultiPolygonName | GeometryType.GeometryCollectionName =>
      val parts: Array[LiteGeometry] = (0 until geometry.getNumGeometries)
        .map(i => simplifyGeometry(geometry.getGeometryN(i))).filter(_ != null)
        .toArray
      LiteGeometry.combineGeometries(parts)
  }

  /**
   * Rasterize the boundary of a line string using the mid-point line rasterization algorithm
   * @param linestring the list of points that make the line string
   */
  private def rasterizeLineStringBoundary(linestring: LiteList): Unit = {
    for (i <- 1 until linestring.numPoints) {
      var (x1: Int, y1: Int) = (linestring.xs(i - 1).toInt, linestring.ys(i - 1).toInt)
      val (x2: Int, y2: Int) = (linestring.xs(i).toInt, linestring.ys(i).toInt)
      val dx = (x2 - x1).abs
      val dy = (y2 - y1).abs
      val incx = if (x1 < x2) 1 else -1
      val incy = if (y1 < y2) 1 else -1
      if (dx > dy) { // Slope < 1
        var p = dy - dx / 2
        while (x1 != x2) {
          setPixelBoundary(x1, y1)
          if (p > 0) {
            y1 += incy
            p += dy - dx
          } else {
            p += dy
          }
          x1 += incx
        }
      } else { // Slope >= 1
        var p = dx - dy / 2
        while (y1 != y2) {
          setPixelBoundary(x1, y1)
          if (p > 0) {
            x1 += incx
            p += dx - dy
          } else {
            p += dx
          }
          y1 += incy
        }
      }
      setPixelBoundary(x2, y2)
    }
  }

  private def rasterizeGeometry(geometry: LiteGeometry): Unit = geometry match {
    case p : LitePoint =>
      setPixelBoundary(p.x, p.y)
    case mp: LiteMultiPoint =>
      for (i <- 0 until mp.numPoints)
        setPixelBoundary(mp.xs(i), mp.ys(i))
    case linestring : LiteLineString =>
      linestring.parts.foreach(rasterizeLineStringBoundary)
    case polygon: LitePolygon =>
      val mbr: java.awt.Rectangle = polygon.envelope
      // Mark all pixels that have centers inside the polygon
      for (y <- (mbr.getMinY.toInt max -buffer) until (mbr.getMaxY.toInt max resolution + buffer)) {
        val xs = findPixelsInside(y, polygon)
        for (x <- xs)
          setPixelInterior(x, y)
      }
      polygon.parts.foreach(rasterizeLineStringBoundary)
  }

  /**
   * Merge another tile into this tile
   * @param other the other tile to merge into this one
   * @return this tile after merging with the other one
   */
  def merge(other: IntermediateVectorTile): IntermediateVectorTile = {
    if (this.isRasterized && other.isRasterized) {
      // Both are rasterized, just combine the two sets of occupied pixels
      this.interiorPixels.inplaceOr(other.interiorPixels)
      this.boundaryPixels.inplaceOr(other.boundaryPixels)
    } else if (this.isRasterized && !other.isRasterized) {
      // Rasterize all incoming features into this
      for (g <- other.geometries)
        rasterizeGeometry(g)
    } else if (!this.isRasterized && other.isRasterized) {
      // Rasterize current one and then merge with the other one
      interiorPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
      boundaryPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
      for (g <- geometries)
        rasterizeGeometry(g)
      features.clear()
      geometries.clear()
      this.interiorPixels.inplaceOr(other.interiorPixels)
      this.boundaryPixels.inplaceOr(other.boundaryPixels)
    } else {
      // None is rasterized. Add incoming features and rasterize if needed
      this.features.appendAll(other.features)
      this.geometries.appendAll(other.geometries)
      this.numPoints += other.numPoints
      if (shouldRasterize) {
        interiorPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
        boundaryPixels = new BitArray((resolution + (2 * buffer)) * (resolution + (2 * buffer)))
        for (g <- geometries)
          rasterizeGeometry(g)
        features.clear()
        geometries.clear()
      }
    }
    // Return this tile to chain merge operations if needed
    this
  }

  private[davinci] def nextPointCWOrdering(x: Short, y: Short, xEnd: Short, yEnd: Short): (Short, Short) = {
    if (x == xEnd && y == yEnd)
      return (xEnd, yEnd)
    if (x == -buffer) { // Along the left edge
      if (y == resolution + buffer) // Move right
        if (y == yEnd) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
      else // Move up
        if (x == xEnd && yEnd >= y) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
    } else if (x == resolution + buffer) { // Along the right edge
      if (y == -buffer) // Move left
        if (y == yEnd) (xEnd, yEnd) else ((-buffer).toShort, y)
      else // Move down
        if (x == xEnd && yEnd <= y) (xEnd, yEnd) else (x, (-buffer).toShort)
    } else if (y == -buffer) { // Along the bottom edge
      if (x == -buffer) // Move up
        if (x == xEnd) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
      else // Move left
        if (y == yEnd && xEnd <= x) (xEnd, yEnd) else ((-buffer).toShort, y)
    } else { // Must be along the top edge
      assert(y == resolution + buffer)
      if (x == resolution + buffer) // Move down
        if (x == xEnd) (xEnd, yEnd) else (x, (-buffer).toShort)
      else // Move right
        if (y == yEnd && xEnd >= x) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
    }
  }

  private[davinci] def nextPointCCWOrdering(x: Short, y: Short, xEnd: Short, yEnd: Short): (Short, Short) = {
    if (x == xEnd && y == yEnd)
      return (xEnd, yEnd)
    if (x == -buffer) { // Along the left edge
      if (y == -buffer) // Move right
        if (y == yEnd) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
      else // Move down
        if (x == xEnd && yEnd <= y) (xEnd, yEnd) else (x, (-buffer).toShort)
    } else if (x == resolution + buffer) { // Along the right edge
      if (y == resolution + buffer) // Move left
        if (y == yEnd) (xEnd, yEnd) else ((-buffer).toShort, y)
      else // Move up
        if (x == xEnd && yEnd >= y) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
    } else if (y == -buffer) { // Along the bottom edge
      if (x == resolution + buffer) // Move up
        if (x == xEnd) (xEnd, yEnd) else (x, (resolution + buffer).toShort)
      else // Move right
        if (y == yEnd && xEnd >= x) (xEnd, yEnd) else ((resolution + buffer).toShort, y)
    } else { // Must be along the top edge
      assert(y == resolution + buffer)
      if (x == -buffer) // Move down
        if (x == xEnd) (xEnd, yEnd) else (x, (-buffer).toShort)
      else // Move Left
        if (y == yEnd && xEnd <= x) (xEnd, yEnd) else ((-buffer).toShort, y)
    }
  }


  /**
   * Converts this intermediate tile into a final [[VectorTile.Tile]]
   * @return the vector tile that represents all features in this tile
   */
  def vectorTile: VectorTile.Tile = {
    val vectorTileBuilder = VectorTile.Tile.newBuilder()
    if (!this.isRasterized) {
      val vectorLayerBuilder = new VectorLayerBuilder(resolution, "features")
      // Add all features one-by-one
      for (iFeature <- features.indices)
        vectorLayerBuilder.addFeature(features(iFeature), geometries(iFeature))
      vectorTileBuilder.addLayers(vectorLayerBuilder.build())

    } else {
      Seq(("interior", interiorPixels), ("boundary", boundaryPixels)).foreach(p => {
        if (p._2.countOnes() > 0) {
          // Add occupied pixels
          val layerBuilder = new VectorLayerBuilder(resolution, p._1)
          // Run the object delineation algorithm to aggregate occupied pixels
          val ringsList: Array[LiteList] = aggregateOccupiedPixels(p._2)
          val polygon = new LitePolygon(ringsList)
          layerBuilder.addFeature(null, polygon)
          vectorTileBuilder.addLayers(layerBuilder.build())
        }
      })
    }
    vectorTileBuilder.build()
  }

  /**
   * Find all pixels in the given row that have a center inside the polygon boundary.
   * @param y the y coordinate of the line of pixels to consider
   * @param polygon the polygon to consider its segments
   * @return the list of x-coordinates of all contained pixels
   */
  private def findPixelsInside(y: Int, polygon: LitePolygon): Iterator[Int] = {
    var intersections = new ArrayBuffer[Int]
    val yCenter: Double = y + 0.5
    for (iRing <- 0 to polygon.numInteriorRings) {
      val ring: LiteList = if (iRing == 0) polygon.exteriorRing else polygon.interiorRing(iRing - 1)
      for (iPoint <- 1 until ring.numPoints) {
        val (x1: Short, y1: Short) = (ring.xs(iPoint - 1), ring.ys(iPoint - 1))
        val (x2: Short, y2: Short) = (ring.xs(iPoint), ring.ys(iPoint))
        if ((y1 <= yCenter && y2 >= yCenter) || (y2 <= yCenter && y1 >= yCenter)) {
          // Ignore horizontal lines for consistency
          if (y2 != y1) {
            val xIntersection: Double = x1 + (x2.toDouble - x1) / (y2.toDouble - y1) * (yCenter - y1)
            intersections.append((xIntersection.round.toInt max (-buffer - 1)) min (resolution + buffer))
          }
        }
      }
      assert(intersections.length % 2 == 0, s"Expected even number of intersections but got ${intersections.length}")
    }

    intersections = intersections.sorted
    (intersections.sorted.indices by 2).iterator.flatMap { i => intersections(i) to intersections(i+1) }
  }

  private def aggregateOccupiedPixels(occupiedPixels: BitArray): Array[LiteList] = {
    // Row width is used to advance the offset more efficiently
    val scanlineSize: Int = resolution + (2 * buffer)
    // Draw the blocked regions
    // Vertices at the top starting at each column, or null if there is no vertical line
    val topVertices: Array[Vertex] = new Array[Vertex](scanlineSize + 1)
    // The location of the left vertex or null if there is no left vertex
    var leftVertex: Vertex = null
    // Store a list of top-left corners to be able to trace all rings afterward
    val corners = new ArrayBuffer[Vertex]()
    // Initial offset of the occupied pixel in the occupiedPixels bit array
    var offset: Int = pointOffset(-buffer, -buffer)
    for (y <- -buffer to resolution + buffer) {
      assert(offset == pointOffset(-buffer, y) || y == resolution + buffer)
      for (x <- -buffer to resolution + buffer) {
        val blocked0: Int =
          if (x > -buffer && y > -buffer && occupiedPixels.get(offset - scanlineSize - 1)) 1
          else 0
        val blocked1: Int =
          if (x < resolution + buffer && y > -buffer && occupiedPixels.get(offset - scanlineSize)) 2
          else 0
        assert((blocked0 == 0) == (blocked1 == 0) || topVertices(x + buffer) != null)
        val blocked2: Int =
          if (x > -buffer && y < resolution + buffer && occupiedPixels.get(offset - 1)) 4
          else 0
        val blocked3: Int =
          if (x < resolution + buffer && y < resolution + buffer && occupiedPixels.get(offset)) 8
          else 0
        assert((blocked0 == 0) == (blocked2 == 0) || leftVertex != null)
        val pixelType: Int = blocked0 | blocked1 | blocked2 | blocked3

        pixelType match {
          case 0 | 3 | 5 | 10 | 12 | 15 => // Do nothing
          case 1 =>
            // Add a new vertex that connects top to left
            val newVertex = Vertex(x, y, leftVertex)
            topVertices(x + buffer).next = newVertex
            topVertices(x + buffer) = null
            leftVertex = null
          case 2 =>
            // Add a new vertex that connects right vertex (unknown) to top (known)
            val newVertex = Vertex(x, y, topVertices(x + buffer))
            leftVertex = newVertex
            topVertices(x + buffer) = null
          case 4 =>
            // Add a new vertex that connects the left vertex (known) to the bottom vertex (unknown)
            val newVertex = Vertex(x, y, null)
            leftVertex.next = newVertex
            topVertices(x + buffer) = newVertex
            leftVertex = null
          case 6 =>
            // Create two coinciding vertices at the center.
            // One connects to the top and is left open to find the right vertex
            // The second follows the one on the left and becomes the new top vertex
            val newVertex1 = Vertex(x, y, topVertices(x + buffer))
            val newVertex2 = Vertex(x, y, null)
            leftVertex.next = newVertex2
            leftVertex = newVertex1
            topVertices(x + buffer) = newVertex2
          case 7 =>
            // Create a new vertex that connects the right (not known yet) to the bottom (not known yet)
            val newVertex = Vertex(x, y, null)
            topVertices(x + buffer) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 8 =>
            // Create a new vertex that connects the bottom vertex (not known yet) to the right vertex(not known yet)
            val newVertex = Vertex(x, y, null)
            topVertices(x + buffer) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 9 =>
            // Create two coinciding vertices at the center
            // 1- First vertex connects the top (known) to the left (known)
            // 2- Second vertex connects the bottom (unknown) to the right (unknown)
            val newVertex1 = Vertex(x, y, leftVertex)
            val newVertex2 = Vertex(x, y, null)
            topVertices(x + buffer).next = newVertex1
            leftVertex = newVertex2
            topVertices(x + buffer) = newVertex2
            corners.append(newVertex2)
          case 11 =>
            // Create a vertex that connects the bottom (unknown) to the left (known)
            val newVertex = Vertex(x, y, leftVertex)
            topVertices(x + buffer) = newVertex
            leftVertex = null
          case 13 =>
            // Create a new vertex that connects the top vertex (known) to the right (unknown)
            val newVertex = Vertex(x, y, null)
            topVertices(x + buffer).next = newVertex
            leftVertex = newVertex
            topVertices(x + buffer) = null
          case 14 =>
            // Create a new vertex that connects left (known) to top (known)
            val newVertex = Vertex(x, y, topVertices(x + buffer))
            leftVertex.next = newVertex
            leftVertex = null
            topVertices(x + buffer) = null
        }
        offset += 1
      }
      offset = offset - (resolution + (2 * buffer) + 1) + scanlineSize
    }
    // Phase II: Trace all linear rings to produce the final output. Always start with a non-visited corner
    val rings = new ArrayBuffer[LiteList]()
    for (corner <- corners; if !corner.visited) {
      // First, count the number of corners to prepare a CoordinateSequence of the right size
      var iCorner = 0
      var p = corner
      do {
        p = p.next
        iCorner += 1
      } while (p != corner)
      val xs: Array[Short] = new Array[Short](iCorner + 1)
      val ys: Array[Short] = new Array[Short](iCorner + 1)
      p = corner
      iCorner = 0
      do {
        xs(iCorner) = p.x.toShort
        ys(iCorner) = p.y.toShort
        p.visited = true
        p = p.next
        iCorner += 1
      } while (p != corner)
      // Make last coordinate similar to the first one
      xs(iCorner) = p.x.toShort
      ys(iCorner) = p.y.toShort
      // Create the linear ring
      rings.append(new LiteList(xs, ys))
    }
    rings.toArray
  }

  /**
   * marks the pixel corresponding to the point coordinate as occupied by the interior of a geometry
   * @param coordinateX x coordinate of the point
   * @param coordinateY y coordinate of the point
   */
  def setPixelInterior(coordinateX: Double, coordinateY: Double): Unit = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    val index = pointOffset(x, y)
    if (index != -1)
      interiorPixels.set(index, true)
  }

  def setPixelBoundary(coordinateX: Double, coordinateY: Double): Unit = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    val index = pointOffset(x, y)
    if (index != -1)
      boundaryPixels.set(index, true)
  }

  def pointOffset(x: Int, y: Int): Int =
    if (!isWithinBoundary(x, y)) -1
    else ((y + buffer) * (resolution + (2 * buffer))) + (x + buffer)

  private def isWithinBoundary(coordinateX: Double, coordinateY: Double) : Boolean = {
    val x = coordinateX.round.toInt
    val y = coordinateY.round.toInt
    x >= -buffer && x < resolution + buffer && y >= -buffer && y < resolution + buffer
  }
}

object IntermediateVectorTile {
  /** The maximum number of points in one vector tile before we decide to rasterize */
  val maxPointsPerTile: Long = 20000L

  /** The maximum number of distinct features to (geometries) to keep in one tile before rasterization */
  val maxFeaturesPerTile: Long = 1000L
}
