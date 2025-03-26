package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.Feature
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import edu.ucr.cs.bdlab.davinci.VectorTile.Tile
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.CoordinateXY
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VectorLayerBuilderTest extends AnyFunSuite with ScalaSparkTest {

  test("Write feature with attributes") {
    val factory = FeatureReader.DefaultGeometryFactory
    val builder = new VectorLayerBuilder(100, "test")
    builder.addFeature(Feature.create(factory.createPoint(new CoordinateXY(50, 50)), Array("id", "name"),
      null, Array(10, "pt")))
    // Now, read the data back
    val layer = builder.build()

    assertResult(1)(layer.getFeaturesCount)
    assertResult(2)(layer.getKeysCount)
    assertResult(2)(layer.getValuesCount)
  }

  test("Skip null attributes") {
    val factory = FeatureReader.DefaultGeometryFactory
    val builder = new VectorLayerBuilder(100, "test")
    builder.addFeature(Feature.create(factory.createPoint(new CoordinateXY(50, 50)), Array("id", "name"),
      null, Array(10, null)))
    // Now, read the data back
    val layer = builder.build()

    assertResult(1)(layer.getFeaturesCount)
    val f1 = layer.getFeatures(0)
    assertResult(2)(f1.getTagsCount)
  }

  test("Write mixed geometry types") {
    val factory = FeatureReader.DefaultGeometryFactory
    val wktReader = new WKTReader(factory)
    val geometries = Array(
      "POINT(10 5)",
      "LINESTRING(10 5, 3 3)",
      "POLYGON((13 5, 20 6, 19 15, 17 14, 13 5), (14 6, 15 6, 14 7, 14 6))",
      "MULTIPOINT(50 5, 53 3, 60 6, 63 3)",
      "MULTILINESTRING((20 5, 23 3), (24 4, 25 5))",
      "MULTIPOLYGON(((13 15, 20 16, 19 25, 17 24, 13 15), (14 16, 15 16, 14 17, 14 16))," +
        "((23 15, 30 16, 29 25, 27 24, 23 15), (24 16, 25 16, 24 17, 24 16)))"
    ).map(wkt => wktReader.read(wkt))
    val builder = new VectorLayerBuilder(100, "test")
    for (geometry <- geometries)
      builder.addFeature(Feature.create(null, geometry))

    // Now, read the data back
    val layer = builder.build()
    assertResult(6)(layer.getFeaturesCount)
    assertResult(VectorTile.Tile.GeomType.POINT)(layer.getFeatures(0).getType)
    assertResult(VectorTile.Tile.GeomType.LINESTRING)(layer.getFeatures(1).getType)
    assertResult(VectorTile.Tile.GeomType.POLYGON)(layer.getFeatures(2).getType)
    assertResult(VectorTile.Tile.GeomType.POINT)(layer.getFeatures(3).getType)
    assertResult(VectorTile.Tile.GeomType.LINESTRING)(layer.getFeatures(4).getType)
    assertResult(VectorTile.Tile.GeomType.POLYGON)(layer.getFeatures(5).getType)
    assertResult(0)(layer.getKeysCount)
    assertResult(0)(layer.getValuesCount)
  }
}
