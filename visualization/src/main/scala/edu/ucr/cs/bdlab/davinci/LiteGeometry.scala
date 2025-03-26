/*
 * Copyright 2023 University of California, Riverside
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

/**
 * A light-weight geometry that uses short as a data type.
 */
trait LiteGeometry extends Serializable {
  def numPoints: Int

  def envelope: java.awt.Rectangle
}

object LiteGeometry {
  /**
   * Combines multiple geometries into one. Since GeometryCollections are not supports, this method tries to create
   * a single geometry with the highest dimension possible.
   *
   *  1. If all geometries are points or multipoints, a point or a multipoint is created with all of them.
   *  2. If the list contains no polygons, a line string is created and all points are dropped.
   *  3. If at least one polygon exists, a polygon is created and all points and line strings are dropped.
   * @param parts a list of geometries that could contain, Point, MultiPoint, LineString, or Polygon
   * @return a single geomettry that combines all (most) geometries into one
   */
  def combineGeometries(parts: Array[LiteGeometry]): LiteGeometry = {
    if (parts.isEmpty)
      return null
    if (parts.length == 1)
      return parts.head
    val maxDimension: Int = parts.map({
      case _: LitePoint | _: LiteMultiPoint => 0
      case _: LiteLineString => 1
      case _: LitePolygon => 2
    }).max
    maxDimension match {
      case 0 =>
        // Return a multipoint
        val xs: Array[Short] = parts.filter(_.isInstanceOf[LitePoint]).map(_.asInstanceOf[LitePoint].x) ++
          parts.filter(_.isInstanceOf[LiteMultiPoint]).flatMap(_.asInstanceOf[LiteMultiPoint].xs)
        val ys: Array[Short] = parts.filter(_.isInstanceOf[LitePoint]).map(_.asInstanceOf[LitePoint].y) ++
          parts.filter(_.isInstanceOf[LiteMultiPoint]).flatMap(_.asInstanceOf[LiteMultiPoint].ys)
        new LiteMultiPoint(xs, ys)
      case 1 =>
        // Return a line string
        new LiteLineString(parts.filter(_.isInstanceOf[LiteLineString]).flatMap(_.asInstanceOf[LiteLineString].parts))
      case 2 =>
        // Return a polygon
        new LitePolygon(parts.filter(_.isInstanceOf[LitePolygon]).flatMap(_.asInstanceOf[LitePolygon].parts))
    }
  }
}

class LitePoint(val x: Short, val y: Short) extends LiteGeometry {
  override def numPoints: Int = 1

  override def envelope: java.awt.Rectangle = new java.awt.Rectangle(x, y, 0, 0)

  override def toString: String = s"POINT($x $y)"
}

class LiteList(val xs: Array[Short], val ys: Array[Short]) extends LiteGeometry {
  require(xs.length == ys.length, s"Incompatible coordinate lengths ${xs.length} != ${ys.length}")
  require(xs.length > 1, s"Cannot create a LiteList with ${xs.length} coordinates.")

  override def numPoints: Int = xs.length.toShort

  override def envelope: java.awt.Rectangle = {
    val x1 = xs.min
    val y1 = ys.min
    val x2 = xs.max
    val y2 = ys.max
    new java.awt.Rectangle(x1, y1, x2 - x1, y2 - y1)
  }

  override def toString: String = xs.zip(ys).mkString(",")

  def isClosed: Boolean = xs(0) == xs(numPoints - 1) && ys(0) == ys(numPoints - 1)

  /**
   * Checks whether this list of points form a closed ring stored in CW order
   *
   * @return `true` if the points form a ring and the ring is stored in clock-wise order
   */
  def isCW: Boolean = {
    if (!isClosed)
      return false
    var sum: Double = 0
    var prevX: Short = xs.last
    var prevY: Short = ys.last
    for (i <- 0 until numPoints) {
      val x: Short = xs(i)
      val y: Short = ys(i)
      sum += (x.toDouble - prevX) * (y.toDouble + prevY)
      prevX = x
      prevY = y
    }
    sum > 0
  }

  def isCCW: Boolean = isClosed && !isCW

  def area: Double = {
    require(numPoints >= 3)
    var a = 0.0
    for (i <- 1 until numPoints)
      a += (xs(i - 1) * ys(i)) - (xs(i) * ys(i - 1))
    a.abs / 2.0
  }

  /** Reverse the order of points in this list */
  def reverse(): Unit = {
    for (i <- 0 until numPoints / 2) {
      var t: Short = 0
      t = xs(i); xs(i) = xs(numPoints - i - 1); xs(numPoints - i - 1) = t
      t = ys(i); ys(i) = ys(numPoints - i - 1); ys(numPoints - i - 1) = t
    }
  }

}

class LiteMultiPoint(override val xs: Array[Short], override val ys: Array[Short]) extends LiteList(xs, ys) {
  require(xs.length > 1, s"A MultiPoint must contain more than one point but contained ${xs.length} points")
}

abstract class LiteMultiList(val parts: Array[LiteList]) extends LiteGeometry {
  require(parts.nonEmpty, "Cannot initialize a multilist with empty parts")
  require(!parts.contains(null), "Cannot initialize a multilist with a null part")

  override def numPoints: Int = parts.map(_.numPoints).sum

  def numParts: Int = parts.length

  def part(i: Int): LiteList = parts(i)

  override def envelope: java.awt.Rectangle = {
    val mbrs = parts.map(_.envelope)
    val x1: Int = mbrs.map(_.x).min
    val y1: Int = mbrs.map(_.y).min
    val x2: Int = mbrs.map(_.getMaxX.toInt).max
    val y2: Int = mbrs.map(_.getMaxY.toInt).max
    new java.awt.Rectangle(x1, y1, x2 - x1, y2 - y1)
  }
}

class LiteLineString(parts: Array[LiteList]) extends LiteMultiList(parts) {
  override def toString: String = {
    val str = new StringBuilder()
    str.append(if (numParts > 1) "MULTILINESTRING(" else "LINESTRING")
    for (part <- parts) {
      str.append("(")
      for (i <- 0 until part.numPoints) {
        if (i > 0)
          str.append(",")
        str.append(part.xs(i))
        str.append(" ")
        str.append(part.xs(i))
      }
      str.append(")")
    }
    if (numParts > 1)
      str.append(")")
    str.toString()
  }
}

class LitePolygon(parts: Array[LiteList]) extends LiteMultiList(parts) {
  for (part <- parts) {
    require(part.numPoints >= 3, s"Ring contained only ${part.numPoints} points")
    require(part.isClosed, s"Non-closed ring ${part.toString}")
  }

  def exteriorRing: LiteList = parts(0)

  def numInteriorRings: Int = numParts - 1

  def interiorRing(i: Int): LiteList = parts(i + 1)

  override def toString: String = {
    val str = new StringBuilder()
    str.append("POLYGON(")
    for (iPart <- 0 until numParts) {
      if (iPart > 0)
        str.append(", ")
      str.append("(")
      val part = parts(iPart)
      for (i <- 0 until part.numPoints) {
        if (i > 0)
          str.append(", ")
        str.append(part.xs(i))
        str.append(" ")
        str.append(part.xs(i))
      }
      str.append(")")
    }
    str.append(")")
    str.toString()
  }
}