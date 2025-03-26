package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateSequence, GeometryFactory, LinearRing, Polygon}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PixelsInsideTest extends AnyFunSuite with ScalaSparkTest {
  var factory: GeometryFactory = new GeometryFactory()

  def createPolygon(rings: CoordinateSequence*): Polygon = {
    val shell: LinearRing = factory.createLinearRing(rings(0))
    val holes: Array[LinearRing] = new Array[LinearRing](rings.length - 1)
    for ($i <- 1 until rings.length) {
      holes($i - 1) = factory.createLinearRing(rings($i))
    }
    factory.createPolygon(shell, holes)
  }

  def createCoordinateSequence(coordinates: Double*): CoordinateSequence = {
    val size: Int = coordinates.length / 2
    val cs: CoordinateSequence = factory.getCoordinateSequenceFactory.create(coordinates.length / 2, 2)
    for (i <- 0 until size) {
      cs.setOrdinate(i, 0, coordinates(2 * i))
      cs.setOrdinate(i, 1, coordinates(2 * i + 1))
    }
    cs
  }

  val p1: Polygon = createPolygon(createCoordinateSequence(
    1.0, 1.0,
    9.0, 3.0,
    3.0, 5.0,
    1.0, 1.0))

  val p2: Polygon = createPolygon(createCoordinateSequence(
    12.0, 3.0,
    18.0, 5.0,
    15.0, 7.0,
    12.0, 3.0))

  val p3: Polygon = createPolygon(createCoordinateSequence(
    -3.0, 6.0,
    12.0, 9.0,
    3.0, 11.0,
    -3.0, 6.0))

  val p4: Polygon = createPolygon(createCoordinateSequence(
    5.0, 1.0,
    8.0, 2.0,
    6.0, 5.0,
    5.0, 1.0))

  val p5: Polygon = createPolygon(createCoordinateSequence(
    6.0, 11.0,
    14.0, 13.0,
    8.0, 15.0,
    6.0, 11.0))

  val p9: Polygon = createPolygon(createCoordinateSequence(
    0.0, 11.0,
    5.0, 11.0,
    5.0, 14.1,
    2.0, 14.1,
    1.0, 12.0,
    0.0, 14.1,
    0.0, 11.0))

  test("Polygon with no holes") {
    val rasterMetadata = RasterMetadata.create(0, 0, 20, 20, 0, 20, 20, 10, 10)
    val pixelsInside = new PixelsInside(Array(p3), rasterMetadata).toArray
    assertResult(13)(pixelsInside.length)
  }

  test("Multiple non-overlapping polygons with no holes") {
    val rasterMetadata = RasterMetadata.create(0, 0, 20, 20, 0, 20, 20, 10, 10)
    val pixelsInside = new PixelsInside(Array(p2, p3, p5), rasterMetadata).toArray
    assertResult(20)(pixelsInside.length)
  }

  test("Multiple overlapping polygons with no holes") {
    val rasterMetadata = RasterMetadata.create(0, 0, 20, 20, 0, 20, 20, 10, 10)
    val pixelsInside = new PixelsInside(Array(p1, p4), rasterMetadata).toArray
    assertResult(6)(pixelsInside.length)
  }

  test("Polygon with a notch and no holes") {
    val rasterMetadata = RasterMetadata.create(0, 0, 20, 20, 0, 20, 20, 10, 10)
    val pixelsInside = new PixelsInside(Array(p9), rasterMetadata).toArray
    assert(pixelsInside.contains((0, 3, 13)))
  }
}
