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
package edu.ucr.cs.bdlab.beast.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, CoordinateXY, CoordinateXYM, CoordinateXYZM, Geometry, GeometryCollection, GeometryFactory, LineString, LinearRing, MultiPoint, Point, Polygon}

object SpatialParquetHelper {
  val SpatialParquetGeometryType: DataType = StructType(Seq(
    StructField("srid", IntegerType, nullable = false),
    StructField("subgeometries", ArrayType.apply(StructType(Seq(
      StructField("geomtype", IntegerType, nullable = false),
      StructField("xs", ArrayType.apply(DoubleType)),
      StructField("ys", ArrayType.apply(DoubleType)),
      StructField("zs", ArrayType.apply(DoubleType)),
      StructField("ms", ArrayType.apply(DoubleType)),
    )))
    )))

  val PointType: Int = 1
  val LineStringType: Int = 2
  val PolygonType: Int = 3

  def encodeGeometry(geometry: Geometry): Seq[InternalRow] = geometry match {
    case p: Point =>
      val cs = p.getCoordinateSequence
      val x = cs.getX(0)
      val y = cs.getY(0)
      val z = if (cs.hasZ) cs.getZ(0) else Double.NaN
      val m = if (cs.hasM) cs.getM(0) else Double.NaN
      Seq(InternalRow.apply(PointType, new GenericArrayData(Seq(x)), new GenericArrayData(Seq(y)),
        if (z.isNaN) null else new GenericArrayData(Seq(z)), if (m.isNaN) null else new GenericArrayData(Seq(m))))
    case l: LineString =>
      val cs = l.getCoordinateSequence
      val xs = new Array[Double](cs.size())
      val ys = new Array[Double](cs.size())
      var zs = if (cs.hasZ) new Array[Double](cs.size()) else null
      var ms = if (cs.hasM) new Array[Double](cs.size()) else null
      for (i <- 0 until cs.size()) {
        xs(i) = cs.getX(i)
        ys(i) = cs.getY(i)
        if (cs.hasZ)
          zs(i) = cs.getZ(i)
        if (cs.hasM)
          ms(i) = cs.getM(i)
      }
      if (zs != null && zs.forall(_.isNaN))
        zs = null
      if (ms != null && ms.forall(_.isNaN))
        ms = null
      Seq(InternalRow.apply(LineStringType, new GenericArrayData(xs), new GenericArrayData(ys),
        if (zs == null) null else new GenericArrayData(zs),
        if (ms == null) null else new GenericArrayData(ms)))
    case p: Polygon =>
      val totalNumPoints = p.getNumPoints
      val firstCS = p.getExteriorRing.getCoordinateSequence
      val xs = new Array[Double](totalNumPoints)
      val ys = new Array[Double](totalNumPoints)
      val zs = if (firstCS.hasZ) new Array[Double](totalNumPoints) else null
      val ms = if (firstCS.hasM) new Array[Double](totalNumPoints) else null
      var iPoint = 0
      for (iRing <- 0 to p.getNumInteriorRing) {
        val ring = if (iRing == 0) p.getExteriorRing else p.getInteriorRingN(iRing - 1)
        val cs = ring.getCoordinateSequence
        for (i <- 0 until cs.size) {
          xs(iPoint) = cs.getX(i)
          ys(iPoint) = cs.getY(i)
          if (cs.hasZ)
            zs(iPoint) = cs.getZ(i)
          if (cs.hasM)
            ms(iPoint) = cs.getM(i)
          iPoint += 1
        }
      }
      assert(iPoint == xs.length)
      Seq(InternalRow.apply(PolygonType, new GenericArrayData(xs), new GenericArrayData(ys),
        if (zs == null) null else new GenericArrayData(zs),
        if (ms == null) null else new GenericArrayData(ms)))
    case mp: MultiPoint =>
      val totalNumPoints = mp.getNumPoints
      val firstCS = mp.getGeometryN(0).asInstanceOf[Point].getCoordinateSequence
      val xs = new Array[Double](totalNumPoints)
      val ys = new Array[Double](totalNumPoints)
      val zs = if (firstCS.hasZ) new Array[Double](totalNumPoints) else null
      val ms = if (firstCS.hasM) new Array[Double](totalNumPoints) else null
      for (iPoint <- 0 until totalNumPoints) {
        val cs = mp.getGeometryN(iPoint).asInstanceOf[Point].getCoordinateSequence
        xs(iPoint) = cs.getX(0)
        ys(iPoint) = cs.getY(0)
        if (cs.hasZ)
          zs(iPoint) = cs.getZ(0)
        if (cs.hasM)
          ms(iPoint) = cs.getM(0)
      }
      Seq(InternalRow.apply(PointType, new GenericArrayData(xs), new GenericArrayData(ys),
        if (zs == null) null else new GenericArrayData(zs),
        if (ms == null) null else new GenericArrayData(ms)))
    case gc: GeometryCollection =>
      // This case handles, MultiLineString, MultiPolygons, or any GeometryCollection
      val subgeoms = new Array[Seq[InternalRow]](gc.getNumGeometries)
      for (iGeom <- 0 until gc.getNumGeometries)
        subgeoms(iGeom) = encodeGeometry(gc.getGeometryN(iGeom))
      subgeoms.flatten.toSeq
  }

  def decodeGeometry(encodedGeometry: InternalRow, factory: GeometryFactory): Geometry = {
    val geomType: Int = encodedGeometry.getInt(0)
    val xs = encodedGeometry.getArray(1)
    val ys = encodedGeometry.getArray(2)
    val zs = encodedGeometry.getArray(3)
    val ms = encodedGeometry.getArray(4)
    val numDimensions = if (zs == null) 2 else 3
    val numMeasures = if (ms == null) 0 else 1
    val mIndex = if (zs == null) 2 else 3
    geomType match {
      case PointType if xs.numElements() == 1 =>
        // Create a single point
        val coord = if (zs == null && ms == null)
          new CoordinateXY(xs.getDouble(0), ys.getDouble(0))
        else if (zs == null)
          new CoordinateXYM(xs.getDouble(0), ys.getDouble(0), ms.getDouble(0))
        else if (ms == null)
          new CoordinateXYZM(xs.getDouble(0), ys.getDouble(0), Double.NaN, zs.getDouble(0))
        else
          new CoordinateXYZM(xs.getDouble(0), ys.getDouble(0), zs.getDouble(0), ms.getDouble(0))
        factory.createPoint(coord)
      case PointType if xs.numElements() > 1 =>
        // Create a MultiPoint
        val points = new Array[Point](xs.numElements())
        for (iPoint <- 0 until xs.numElements()) {
          val coord = if (zs == null && ms == null)
            new CoordinateXY(xs.getDouble(iPoint), ys.getDouble(iPoint))
          else if (zs == null)
            new CoordinateXYM(xs.getDouble(iPoint), ys.getDouble(iPoint), ms.getDouble(iPoint))
          else if (ms == null)
            new CoordinateXYZM(xs.getDouble(iPoint), ys.getDouble(iPoint), Double.NaN, zs.getDouble(iPoint))
          else
            new CoordinateXYZM(xs.getDouble(iPoint), ys.getDouble(iPoint), zs.getDouble(iPoint), ms.getDouble(iPoint))
          points(iPoint) = factory.createPoint(coord)
        }
        factory.createMultiPoint(points)
      case LineStringType =>
        // Single LineString
        val cs = factory.getCoordinateSequenceFactory.create(xs.numElements(), numDimensions, numMeasures)
        for (iPoint <- 0 until xs.numElements()) {
          cs.setOrdinate(iPoint, 0, xs.getDouble(iPoint))
          cs.setOrdinate(iPoint, 1, ys.getDouble(iPoint))
          if (zs != null)
            cs.setOrdinate(iPoint, 2, zs.getDouble(iPoint))
          if (ms != null)
            cs.setOrdinate(iPoint, mIndex, ms.getDouble(iPoint))
        }
        factory.createLineString(cs)
      case PolygonType =>
        val numDimensions = if (zs == null) 2 else 3
        val numMeasures = if (ms == null) 0 else 1
        val mIndex = if (zs == null) 2 else 3
        var i1 = 0
        val rings = collection.mutable.ArrayBuffer[LinearRing]()
        while (i1 < xs.numElements()) {
          // Find the last point in this ring
          var i2 = i1 + 1
          var notFound = false
          do {
            notFound = xs.getDouble(i2).toFloat != xs.getDouble(i1).toFloat ||
              ys.getDouble(i2).toFloat != ys.getDouble(i1).toFloat
            i2 += 1
          } while (i2 < xs.numElements() && notFound)
          val numPointsInRing = i2 - i1
          val cs = factory.getCoordinateSequenceFactory.create(numPointsInRing, numDimensions, numMeasures)
          for (iPoint <- 0 until numPointsInRing) {
            cs.setOrdinate(iPoint, 0, xs.getDouble(iPoint + i1))
            cs.setOrdinate(iPoint, 1, ys.getDouble(iPoint + i1))
            if (zs != null)
              cs.setOrdinate(iPoint, 2, zs.getDouble(iPoint + i1))
            if (ms != null)
              cs.setOrdinate(iPoint, mIndex, ms.getDouble(iPoint + i1))
          }
          rings.append(factory.createLinearRing(cs))
          i1 = i2
        }
        // Create the polygon
        factory.createPolygon(rings(0), rings.slice(1, rings.length).toArray)
    }
  }
}
