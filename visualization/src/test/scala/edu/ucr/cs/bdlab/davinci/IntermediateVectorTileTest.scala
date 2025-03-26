package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, LinearRing, Polygon}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntermediateVectorTileTest extends AnyFunSuite with ScalaSparkTest {

  test("Simplify geometry should handle points") {
    val interTile = new IntermediateVectorTile(10, 0)
    var point = GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(5, 5))
    var simplifiedPoint = interTile.simplifyGeometry(point)
    assert(simplifiedPoint.isInstanceOf[LitePoint])
    assertResult(5)(simplifiedPoint.asInstanceOf[LitePoint].x)

    point = GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(-5, 5))
    simplifiedPoint = interTile.simplifyGeometry(point)
    assertResult(null)(simplifiedPoint)
  }

  test("Simplify geometry should handle line string") {
    val interTile = new IntermediateVectorTile(10, 0)
    var line = GeometryReader.DefaultGeometryFactory.createLineString(Array(
      new Coordinate(5, 5), new Coordinate(-5, 5), new Coordinate(-5, 6), new Coordinate(-5, 7),
      new Coordinate(-5, 15), new Coordinate(5, 8),
    ))
    var simplifiedLine = interTile.simplifyGeometry(line)
    assert(simplifiedLine.isInstanceOf[LiteLineString])
    assertResult(4)(simplifiedLine.numPoints)

    line = GeometryReader.DefaultGeometryFactory.createLineString(Array(
      new Coordinate(5, 5), new Coordinate(2, 3), new Coordinate(-5, 5), new Coordinate(-5, 6), new Coordinate(-5, 7)
    ))
    simplifiedLine = interTile.simplifyGeometry(line)
    assertResult(3)(simplifiedLine.numPoints)

    line = GeometryReader.DefaultGeometryFactory.createLineString(Array(
      new Coordinate(-5, 5), new Coordinate(-5, 6), new Coordinate(-5, 7)
    ))
    simplifiedLine = interTile.simplifyGeometry(line)
    assertResult(null)(simplifiedLine)

    line = GeometryReader.DefaultGeometryFactory.createLineString(Array(
      new Coordinate(-5, 5), new Coordinate(-5, 6), new Coordinate(-5, 20)
    ))
    simplifiedLine = interTile.simplifyGeometry(line)
    assertResult(null)(simplifiedLine)
  }

  test("Simplify geometry should handle linear ring") {
    var interTile = new IntermediateVectorTile(10, 0)
    var ring: LinearRing = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(-5, 5), new Coordinate(-5, 6), new Coordinate(-5, 7),
      new Coordinate(-5, 15), new Coordinate(5, 5)
    ))
    var simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(4)(simplifiedRing.numPoints)

    // Same ring but goes in the other direction
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(-5, 15), new Coordinate(-5, 5), new Coordinate(5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(4)(simplifiedRing.numPoints)

    // A ring that is completely contained
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(1, 5), new Coordinate(1, 8), new Coordinate(5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(4)(simplifiedRing.numPoints)

    // A ring that is completely outside and does not contain the tile
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-15, -15), new Coordinate(-15, 15), new Coordinate(15, 15), new Coordinate(15, -15),
      new Coordinate(-14, -15), new Coordinate(-14, -14), new Coordinate(14, -14), new Coordinate(14, 14),
      new Coordinate(-14, 14), new Coordinate(-14, -14), new Coordinate(-15, -15)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(null)(simplifiedRing)

    // A ring that completely contains the tile
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-15, -15),new Coordinate(-15, 15),new Coordinate(15, 15),
      new Coordinate(15, -15),new Coordinate(-15, -15),
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(5)(simplifiedRing.numPoints)

    // A ring that becomes one point
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(5.1, 5.3), new Coordinate(5.1, 5.4), new Coordinate(5, 5),
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LitePoint])

    // A ring that starts and ends outside
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-5, 5), new Coordinate(-5, 15), new Coordinate(5, 5), new Coordinate(-5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(4)(simplifiedRing.numPoints)

    // A ring that starts and ends outside going in the different direction
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-5, 5), new Coordinate(5, 5), new Coordinate(-5, 15), new Coordinate(-5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(4)(simplifiedRing.numPoints)

    // A ring that starts and ends outside
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-5, 5), new Coordinate(-5, -15), new Coordinate(15, -15), new Coordinate(15, 15),
      new Coordinate(-5, 15), new Coordinate(5, 5), new Coordinate(-5, 5),
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(7)(simplifiedRing.numPoints)

    // A ring with very close intersections with boundaries
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-5, 5), new Coordinate(-5, 5.4), new Coordinate(5.4, 5), new Coordinate(4.6, 5),
      new Coordinate(-5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.isInstanceOf[LiteList])
    assertResult(3)(simplifiedRing.numPoints)

    ring = new WKTReader().read("LINEARRING (11 4, 10 5, 9 5, 9 4, 10 4, 10 3, 11 4)").asInstanceOf[LinearRing]
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(6)(simplifiedRing.numPoints)

    // Test a ring that goes outside and inside twice
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(-5, 12), new Coordinate(5, 8), new Coordinate(5, 15),
      new Coordinate(-5, 15), new Coordinate(-5, 5), new Coordinate(5, 5),
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(8)(simplifiedRing.numPoints)

    // Test a ring with a long cutting line
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(12, 5), new Coordinate(10000, 8), new Coordinate(-10000, 8),
      new Coordinate(5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(6)(simplifiedRing.numPoints)

    // Ring with extreme values that go beyond Short
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(12, 5), new Coordinate(100000, 8), new Coordinate(-100000, 8),
      new Coordinate(5, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(6)(simplifiedRing.numPoints)

    // Test a ring that has two nearby points, one inside and one outside
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(10.1, 5), new Coordinate(10.1, 7), new Coordinate(8, 7),
      new Coordinate(9.9, 5), new Coordinate(10.1, 5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assert(simplifiedRing.asInstanceOf[LiteList].isClosed, s"Ring ${simplifiedRing} should be closed")

    // A ring that starts outside and crosses the tile
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 12), new Coordinate(-2, 5), new Coordinate(5, 5), new Coordinate(5, 12)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(6)(simplifiedRing.numPoints)

    // Test a ring that touches the boundary
    interTile = new IntermediateVectorTile(256, 5)
    ring = new WKTReader().read("LINEARRING (264.1 51.3, 264.2 51.6, 264.2 51.9, 264.1 52.1, 264.0 52.3, " +
      "263.9 52.2, 263.7 52.1, 263.6 51.7, 263.4 51.2, 263.2 50.9, 263.1 50.8, 262.6 50.0, 262.2 49.7, 261.8 48.5, " +
      "261.2 47.9, 261.1 47.6, 261.2 47.5, 261.2 47.4, 261.1 46.73, 260.7 45.8, 260.7 45.1, 260.8 44.9, 260.7 44.5, " +
      "263.6 50.4, 263.7 50.5, 263.9 50.7, 264.0 50.8, 264.0 50.9, 264.1 51.2, 264.1 51.3)").asInstanceOf[LinearRing]
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(0)(simplifiedRing.asInstanceOf[LiteList].area)

    // Test a ring that is very close to the boundary
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(249.8, -3.5), new Coordinate(259.0, -5.3), new Coordinate(260.1, -2.1), new Coordinate(249.8, -3.5)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(5)(simplifiedRing.numPoints)

    // Test a ring that crosses the tile twice
    ring = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(-49.7, 71.9), new Coordinate(52.5, 4.1), new Coordinate(44.3, -8.3),
      new Coordinate(-57.1, 60.7), new Coordinate(-49.7, 71.9)
    ))
    simplifiedRing = interTile.simplifyGeometry(ring)
    assertResult(6)(simplifiedRing.numPoints)
  }

  test("Simplify geometry should handle polygons") {
    // Ensure that the outer shell is stored in CW order and holes are in CCW order
    var interTile = new IntermediateVectorTile(10, 0)
    var shell: LinearRing = GeometryReader.DefaultGeometryFactory.createLinearRing(Array(
      new Coordinate(5, 5), new Coordinate(6, 5), new Coordinate(6, 6), new Coordinate(5, 5)
    ))
    var polygon = GeometryReader.DefaultGeometryFactory.createPolygon(shell)
    var simplifiedPolygon = interTile.simplifyGeometry(polygon)
    assert(simplifiedPolygon.isInstanceOf[LitePolygon])
    assert(simplifiedPolygon.asInstanceOf[LitePolygon].parts(0).isCW, "Outer ring was not in CW order")

    // Test a polygon that becomes one line because it is too small
    polygon = new WKTReader().read("POLYGON ((1 1, 3 1.1, 1 1.1, 1 1))").asInstanceOf[Polygon]
    simplifiedPolygon = interTile.simplifyGeometry(polygon)
    assertResult(classOf[LiteLineString])(simplifiedPolygon.getClass)
    assertResult(3)(simplifiedPolygon.numPoints)
  }

    test("Trim line segment") {
    val tile = new IntermediateVectorTile(10, 2)
    assertResult((1, 2, 3, 4))(tile.trimLineSegment(1, 2, 3, 4))
    assertResult(null)(tile.trimLineSegment(-4, 2, -3, 5))
    assertResult((-2, 3, 2, 5))(tile.trimLineSegment(-4, 2, 2, 5))
    assertResult((6, 8, 4, 12))(tile.trimLineSegment(6, 8, 3, 14))
  }

  test("Next Point CW") {
    val tile = new IntermediateVectorTile(10, 2)
    assertResult((-2, 12))(tile.nextPointCWOrdering(-2, -2, 12, 5))
    assertResult((12, 12))(tile.nextPointCWOrdering(-2, 12, 12, 5))
    assertResult((12, 5))(tile.nextPointCWOrdering(12, 12, 12, 5))

    assertResult((-2, 12))(tile.nextPointCWOrdering(-2, 5, -2, 3))
    assertResult((-2, 6))(tile.nextPointCWOrdering(-2, 5, -2, 6))
    assertResult((5, 12))(tile.nextPointCWOrdering(3, 12, 5, 12))
    assertResult((12, 12))(tile.nextPointCWOrdering(3, 12, 1, 12))
  }
}
