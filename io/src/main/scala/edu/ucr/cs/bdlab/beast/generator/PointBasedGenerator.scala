/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.generator
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, GeometryType, PointND}
import org.locationtech.jts.geom.{Coordinate, CoordinateXY, Geometry}

import scala.util.matching.Regex

/**
 * A generator that generates either points directly or boxes around these points
 * @param partition information about the partition to generate
 */
abstract class PointBasedGenerator(partition: RandomSpatialPartition)
  extends SpatialGenerator(partition) {

  /**The index of the record that will be returned when next is called*/
  var iRecord: Long = 0

  /**The maximum size for boxes to generate*/
  lazy val maxSize: Array[Double] = partition.opts.getString(UniformDistribution.MaxSize).split(",").map(_.toDouble)

  private val intRegexp: Regex = raw"(\d+)".r
  private val rangeRegexp: Regex = raw"(\d+)\.\.(\d+)".r
  lazy val numSegments: Range = partition.opts.getString(UniformDistribution.NumSegments) match {
    case intRegexp(maxSize) => 3 to maxSize.toInt
    case rangeRegexp(minimum, maximum) => minimum.toInt to maximum.toInt
  }

  /**Return the geometry type to generate*/
  lazy val geometryType: GeometryType = partition.opts
    .getString(UniformDistribution.GeometryType, "point").toLowerCase match {
    case "box" => GeometryType.ENVELOPE
    case "point" => GeometryType.POINT
    case "polygon" => GeometryType.POLYGON
    case other => throw new RuntimeException(s"Unrecognized geometry type '$other'")
  }

  /**Unit square is the input domain*/
  val UnitSquare: EnvelopeNDLite = new EnvelopeNDLite(2, 0, 0, 1, 1)

  /**
   * Generates a point
   * @return the point generated
   */
  def generatePoint: PointND

  /**
   * Generates a box by first generating a point and building a box around it
   * @return
   */
  def generateBox: EnvelopeND = {
    val center: PointND = generatePoint
    val box: EnvelopeND = new EnvelopeND(center.getFactory, center.getCoordinateDimension)
    for (d <- 0 until center.getCoordinateDimension) {
      // Adjust the coordinate to ensure that the box will remain in the unit square
      val adjustedCoord = center.getCoordinate(d) * (1 - maxSize(d)) + maxSize(d) / 2
      val size = uniform(0, maxSize(d))
      box.setMinCoord(d, adjustedCoord - size / 2)
      box.setMaxCoord(d, adjustedCoord + size / 2)
    }
    box
  }

  /**
   * Generates and returns the next geometry
   *
   * @return the generated geometry
   */
  override def nextGeometry: Geometry = {
    iRecord += 1
    geometryType match {
      case GeometryType.ENVELOPE =>
        // Generate box and ensure it is contained in the input domain
        var box: EnvelopeND = null
        do {
          box = generateBox
        } while (!UnitSquare.containsEnvelope(box.getEnvelopeInternal))
        box
      case GeometryType.POLYGON =>
        require(partition.dimensions == 2, s"Polygon generation only works with 2 dimensions, found ${partition.dimensions}")
        // Generate a polygon around the point as follows
        // Picture a clock with one hand making a complete rotation of 2 PI
        // The hand makes n stops to create n points on the polygon
        // At each stop, we choose a point on the hand at random to create one point
        // We connect all points to create the n line segments
        // This way, we can generate an arbitrary polygon (convex or concave) that does not intersect itself.
        val center: PointND = generatePoint
        for (d <- 0 until center.getCoordinateDimension)
          center.setCoordinate(d, center.getCoordinate(d) * (1 - maxSize(0)) + maxSize(0) / 2)
        val numSegments: Int = dice(this.numSegments)
        // The sorted angle stops of the hand
        val points: Array[Coordinate] = new Array[Int](numSegments + 1).map(_ => {uniform(0, Math.PI * 2)})
          .sorted
          .map(angle => {
            val distance = uniform(0, maxSize(0) / 2)
            val x = center.getCoordinate(0) + distance * Math.cos(angle)
            val y = center.getCoordinate(1) + distance * Math.sin(angle)
            new CoordinateXY(x, y)
          })
        // To ensure that the polygon is closed, we override the value of the last point to be the same as the first
        points(numSegments).setCoordinate(points(0))
        geometryFactory.createPolygon(points)
      case GeometryType.POINT =>
        // Generate a point and ensure it is contained in the input domain
        var point: PointND = null
        do {
          point = generatePoint
        } while (!UnitSquare.containsPoint(point))
        point
    }
  }

  override def hasNext: Boolean = iRecord < partition.cardinality
}
