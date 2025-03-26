package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VectorTilePlotterTest extends AnyFunSuite with ScalaSparkTest {

  test("Write mixed geometry types") {
    val factory = FeatureReader.DefaultGeometryFactory
    val wktReader = new WKTReader(factory)
    val geometries = Array(
      "LINESTRING(-160 40, -150 20)",
    ).map(wkt => wktReader.read(wkt))
    val features: RDD[IFeature] = sparkContext.parallelize(geometries).map(g => Feature.create(null, g))

    val tile = MVTDataVisualizer.plotSingleTileParallel(features, 128, TileIndex.encode(0, 0, 0), 0)

    assertResult(1)(tile.getLayersCount)
    val layer = tile.getLayers(0)
    assertResult(1)(layer.getFeaturesCount)
    val feature = layer.getFeatures(0)
    assertResult(VectorTile.Tile.GeomType.LINESTRING)(feature.getType)
  }

  test("Project into the image space") {
    val factory = FeatureReader.DefaultGeometryFactory
    val wktReader = new WKTReader(factory)
    val geometries = Array("POINT(-90 0)").map(wkt => wktReader.read(wkt))
    val features: RDD[IFeature] = sparkContext.parallelize(geometries).map(g => Feature.create(null, g))

    val tile = MVTDataVisualizer.plotSingleTileParallel(features, 100, TileIndex.encode(0, 0, 0), 0)

    assertResult(1)(tile.getLayersCount)
    val layer = tile.getLayers(0)
    assertResult(1)(layer.getFeaturesCount)
    val feature = layer.getFeatures(0)
    assertResult(VectorTile.Tile.GeomType.POINT)(feature.getType)
    assertResult(3)(feature.getGeometryCount)
    val x = VectorLayerBuilder.zigzagDecode(feature.getGeometry(1))
    val y = VectorLayerBuilder.zigzagDecode(feature.getGeometry(2))
    assertResult(25)(x)
    assertResult(50)(y)
  }
}
