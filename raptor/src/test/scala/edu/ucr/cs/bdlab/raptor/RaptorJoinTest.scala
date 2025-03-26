package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateSequence, Envelope, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RaptorJoinTest extends AnyFunSuite with ScalaSparkTest {

  val factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)
  def createSequence(points: (Double, Double)*): CoordinateSequence = {
    val cs = factory.getCoordinateSequenceFactory.create(points.length, 2)
    for (i <- points.indices) {
      cs.setOrdinate(i, 0, points(i)._1)
      cs.setOrdinate(i, 1, points(i)._2)
    }
    cs
  }

  test("RaptorJoinZS with RDD") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(-82.76, -80.25, 31.91, 35.17))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.length == 6)
    assert(finalValues(0).x == 69)
    assert(finalValues(0).y == 48)
  }

  test("RaptorJoin with multiband") {
    val rasterFile = makeFileCopy("/rasters/FRClouds.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(0, 3, 45, 47))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Array[Int]] = new RasterFileRDD[Array[Int]](sparkContext, rasterFile, IRasterReader.OverrideSRID -> 4326)

    val values: RDD[RaptorJoinResult[Array[Int]]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    assertResult(classOf[Array[Int]])(values.first().m.getClass)
  }

  test("RaptorJoinZS with RDD and reproject") {
    val rasterFile = makeFileCopy("/rasters/glc2000_small_EPSG3857.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(-109, -106, 36, 40))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.length == 6)
    assert(finalValues(0).m == 12.0f)
  }

  test("EmptyResult with RDD") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.createPolygon()
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.isEmpty)
  }
}
