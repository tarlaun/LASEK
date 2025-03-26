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
package edu.ucr.cs.bdlab.beast.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialParquetHelperTest extends AnyFunSuite with ScalaSparkTest {
  val geometryFactory = new GeometryFactory()

  def createCS(coords: Double*): CoordinateSequence = {
    val numPoints = coords.length / 2
    val cs = geometryFactory.getCoordinateSequenceFactory.create(numPoints, 2)
    for (i <- 0 until numPoints) {
      cs.setOrdinate(i, 0, coords(2 * i))
      cs.setOrdinate(i, 1, coords(2 * i + 1))
    }
    cs
  }

  test("encode point") {
    val point = geometryFactory.createPoint(new Coordinate(5.5, 3.4))
    val encoded: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(point)
    assertResult(1)(encoded.length)
    val firstGeom: InternalRow = encoded.head
    assertResult(SpatialParquetHelper.PointType)(firstGeom.getInt(0))
    val xs = firstGeom.getArray(1)
    assertResult(5.5)(xs.get(0, DoubleType))
    val ys = firstGeom.getArray(2)
    assertResult(3.4)(ys.get(0, DoubleType))
    assert(firstGeom.isNullAt(3))
    assert(firstGeom.isNullAt(4))
  }

  test("encode linestring") {
    val linestring = geometryFactory.createLineString(createCS(5.5, 3.4, 10.2, 5.1, 1.2, 2.1))
    val encoded: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(linestring)
    assertResult(1)(encoded.length)
    val firstGeom: InternalRow = encoded.head
    assertResult(SpatialParquetHelper.LineStringType)(firstGeom.getInt(0))
    val xs = firstGeom.getArray(1)
    assertResult(Array[Double](5.5, 10.2, 1.2))(xs.toArray[Double](DoubleType))
    val ys = firstGeom.getArray(2)
    assertResult(Array[Double](3.4, 5.1, 2.1))(ys.toArray[Double](DoubleType))
    assert(firstGeom.isNullAt(3))
    assert(firstGeom.isNullAt(4))
  }

  test("encode polygon") {
    val geometryFactory = new GeometryFactory()
    val polygon = geometryFactory.createPolygon(
      geometryFactory.createLinearRing(createCS(0, 0, 5, 0, 5, 5, 0, 5, 0, 0)),
      Array(geometryFactory.createLinearRing(createCS(1, 1, 2, 1, 1, 2, 1, 1))),
    )
    val encoded: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(polygon)
    assertResult(1)(encoded.length)
    val firstGeom: InternalRow = encoded.head
    assertResult(SpatialParquetHelper.PolygonType)(firstGeom.getInt(0))
    val xs = firstGeom.getArray(1)
    assertResult(Array[Double](0, 5, 5, 0, 0, 1, 2, 1, 1))(xs.toArray[Double](DoubleType))
    val ys = firstGeom.getArray(2)
    assertResult(Array[Double](0, 0, 5, 5, 0, 1, 1, 2, 1))(ys.toArray[Double](DoubleType))
    assert(firstGeom.isNullAt(3))
    assert(firstGeom.isNullAt(4))
  }

  test("encode multipoint") {
    val multipoint = geometryFactory.createMultiPoint(
      createCS(0, 5, 2, 2, 4, 3)
    )
    val encoded: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(multipoint)
    assertResult(1)(encoded.length)
    val firstGeom: InternalRow = encoded.head
    assertResult(SpatialParquetHelper.PointType)(firstGeom.getInt(0))
    val xs = firstGeom.getArray(1)
    assertResult(Array[Double](0, 2, 4))(xs.toArray[Double](DoubleType))
    val ys = firstGeom.getArray(2)
    assertResult(Array[Double](5, 2, 3))(ys.toArray[Double](DoubleType))
    assert(firstGeom.isNullAt(3))
    assert(firstGeom.isNullAt(4))
  }

  test("encode multilinestring") {
    val multipoint = geometryFactory.createMultiLineString(Array(
      geometryFactory.createLineString(createCS(0, 5, 2, 2, 4, 3)),
      geometryFactory.createLineString(createCS(10, 15, 12, 12, 14, 13)),
    ))
    val encoded: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(multipoint)
    assertResult(2)(encoded.length)
    val firstGeom: InternalRow = encoded.head
    assertResult(SpatialParquetHelper.LineStringType)(firstGeom.getInt(0))
    assertResult(Array[Double](0, 2, 4))(firstGeom.getArray(1).toArray[Double](DoubleType))
    assertResult(Array[Double](5, 2, 3))(firstGeom.getArray(2).toArray[Double](DoubleType))
    assertResult(null)(firstGeom.getArray(3))
    assertResult(null)(firstGeom.getArray(4))
  }

  test("encode/decode geometries") {
    val geometries = Array(
      geometryFactory.createPoint(new Coordinate(5.5, 3.4)),
      geometryFactory.createLineString(createCS(5.5, 3.4, 10.2, 5.1, 1.2, 2.1)),
      geometryFactory.createPolygon(
        geometryFactory.createLinearRing(createCS(0, 0, 5, 0, 5, 5, 0, 5, 0, 0)),
        Array(geometryFactory.createLinearRing(createCS(1, 1, 2, 1, 1, 2, 1, 1))),
      ),
      geometryFactory.createMultiPoint(createCS(0, 5, 2, 2, 4, 3))
    )

    val encoded: Array[Seq[InternalRow]] = geometries.map(g => SpatialParquetHelper.encodeGeometry(g))
    val decoded: Array[Geometry] = encoded.map(e => SpatialParquetHelper.decodeGeometry(e.head, geometryFactory))
    assertResult(decoded)(geometries)
  }
}
