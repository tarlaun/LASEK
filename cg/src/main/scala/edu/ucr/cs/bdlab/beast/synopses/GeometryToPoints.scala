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
package edu.ucr.cs.bdlab.beast.synopses

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, LinearRing, Point, Polygon}

import scala.collection.mutable

/**
 * An iterator that breaks down a geometry into points.
 */
class GeometryToPoints(geometry: Geometry) extends Iterator[Geometry] {
  /**A stack of geometries to break. The tail is the geometry currently being processed.*/
  val geometriesToBreak: mutable.ArrayBuffer[Geometry] = new mutable.ArrayBuffer[Geometry]()

  if (!geometry.isEmpty)
    geometriesToBreak.append(geometry)

  /**The current geometry being broken down*/
  var currentGeometry: Geometry = _

  /**The index of the point that will be extracted from the current geometry when next is called*/
  var iPoint: Int = -1

  var numPointsForCurrentGeometry: Int = 0

  // Move to the first record
  moveToNext()

  private def moveToNext(): Unit = {
    iPoint += 1
    if (iPoint >= numPointsForCurrentGeometry) {
      // Done with the current geometry, move to next
      currentGeometry = null
      while (currentGeometry == null && geometriesToBreak.nonEmpty) {
        currentGeometry = geometriesToBreak.remove(geometriesToBreak.length - 1)
        currentGeometry match {
          case _: Point => numPointsForCurrentGeometry = 1
          case _: EnvelopeND => numPointsForCurrentGeometry = 2
          case lr: LinearRing => numPointsForCurrentGeometry = lr.getNumPoints - 1
          case ls: LineString => numPointsForCurrentGeometry = ls.getNumPoints
          case p: Polygon =>
            geometriesToBreak.append(p.getExteriorRing)
            for (i <- 0 until p.getNumInteriorRing)
              geometriesToBreak.append(p.getInteriorRingN(i))
            currentGeometry = null
          case gc: GeometryCollection =>
            for (i <- 0 until gc.getNumGeometries) {
              if (!gc.getGeometryN(i).isEmpty)
                geometriesToBreak.append(gc.getGeometryN(i))
            }
            currentGeometry = null
        }
      }
      iPoint = 0
      numPointsForCurrentGeometry = currentGeometry match {
        case null => 0
        case _: Point | _: PointND => 1
        case _: EnvelopeND => 2
        case lr: LinearRing => lr.getNumPoints - 1
        case ls: LineString => ls.getNumPoints
      }
    }
  }

  override def hasNext: Boolean = iPoint < numPointsForCurrentGeometry

  override def next(): Geometry = {
    val point: Geometry = currentGeometry match {
      case _: Point | _: PointND =>
        geometry
      case e: EnvelopeND =>
        val p = new PointND(e.getFactory, e.getCoordinateDimension)
        for (d <- 0 until e.getCoordinateDimension)
          p.setCoordinate(d, if (iPoint == 0) e.getMinCoord(d) else e.getMaxCoord(d))
        p
      case ls: LineString =>
        ls.getFactory.createPoint(ls.getCoordinateN(iPoint))
    }
    // Advance the iterator to the next point
    moveToNext()
    point
  }

}
