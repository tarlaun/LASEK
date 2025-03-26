package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{GeometryReader, RasterMetadata}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform

@RunWith(classOf[JUnitRunner])
class IntersectionIteratorTest extends AnyFunSuite with ScalaSparkTest {
  private def createSimpleGrid(numTilesX: Int, numTilesY: Int, tileWidth: Int, tileHeight: Int): RasterMetadata =
    new RasterMetadata(0, 0, numTilesX * tileWidth, numTilesY * tileHeight,
      tileWidth, tileHeight, 0, new AffineTransform)

  test("Iterate over two intersections") {
    val metadata: RasterMetadata = createSimpleGrid(1, 1, 10, 10)
    val geometries1 = Array[Geometry](
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(1, 1)),
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(3, 3)),
    )
    val intersections1 = new Intersections
    intersections1.compute(geometries1, metadata)

    val geometries2 = Array[Geometry](
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(2, 2)),
      GeometryReader.DefaultGeometryFactory.createPoint(new Coordinate(4, 4)),
    )
    val intersections2 = new Intersections
    intersections2.compute(geometries2, metadata)

    val rasterIDs = Array(0, 1)
    val intersectionsList = Array(intersections1, intersections2)
    val intersectionsIterator = new IntersectionsIterator(rasterIDs, intersectionsList)
    assert(intersectionsIterator.size == 4)
  }
}
