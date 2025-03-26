package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform

@RunWith(classOf[JUnitRunner])
class RasterOperationsLocalTest extends AnyFunSuite with ScalaSparkTest {
  test("threshold operation") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val inputTile: MemoryTile[Short] = new MemoryTile(0, metadata)
    inputTile.setPixelValue(10, 0, 123)
    inputTile.setPixelValue(0, 1, 125)
    inputTile.setPixelValue(50, 3, 44)

    val inputRaster: RDD[ITile[Short]] = sparkContext.parallelize(Seq(inputTile))

    val outputRaster: RDD[ITile[Int]] = RasterOperationsLocal.mapPixels(inputRaster, (x: Short) => Math.max(x, 50))
    assertResult(1)(outputRaster.count())
    val outputTile = outputRaster.first()
    assertResult(123)(outputTile.getPixelValue(10, 0))
    assertResult(125)(outputTile.getPixelValue(0, 1))
    assertResult(50)(outputTile.getPixelValue(50, 3))

    var pixelCount = 0
    for (y <- outputTile.y1 to outputTile.y2; x <- outputTile.x1 to outputTile.x2; if outputTile.isDefined(x, y))
      pixelCount += 1
    assertResult(3)(pixelCount)
  }

  test("filter operation") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val inputTile: MemoryTile[Short] = new MemoryTile(0, metadata)
    inputTile.setPixelValue(10, 0, 123)
    inputTile.setPixelValue(0, 1, 125)
    inputTile.setPixelValue(50, 3, 44)

    val inputRaster: RDD[ITile[Short]] = sparkContext.parallelize(Seq(inputTile))

    val outputRaster: RDD[ITile[Short]] = RasterOperationsLocal.filterPixels(inputRaster, (x: Short) => x < 50)
    assertResult(1)(outputRaster.count())
    val outputTile = outputRaster.first()
    assert(outputTile.isEmpty(10, 0))
    assert(outputTile.isEmpty(0, 1))
    assert(outputTile.isDefined(50, 3))

    var pixelCount = 0
    for (y <- outputTile.y1 to outputTile.y2; x <- outputTile.x1 to outputTile.x2; if outputTile.isDefined(x, y))
      pixelCount += 1
    assertResult(1)(pixelCount)
  }

  test("overlay operation") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val inputTile1: MemoryTile[Short] = new MemoryTile(0, metadata)
    inputTile1.setPixelValue(10, 0, 123)
    inputTile1.setPixelValue(0, 1, 125)
    val raster1: RDD[ITile[Short]] = sparkContext.parallelize(Seq(inputTile1))
    val inputTile2: MemoryTile[Short] = new MemoryTile(0, metadata)
    inputTile2.setPixelValue(10, 0, 55)
    inputTile2.setPixelValue(0, 5, 125)
    inputTile2.setPixelValue(50, 3, 44)
    val raster2: RDD[ITile[Short]] = sparkContext.parallelize(Seq(inputTile2))

    val raster3: RasterRDD[Array[Short]] = RasterOperationsLocal.overlay(raster1, raster2)
    assertResult(1)(raster3.count())
    val tile3 = raster3.first()
    var pixelCount = 0
    for (y <- tile3.y1 to tile3.y2; x <- tile3.x1 to tile3.x2; if tile3.isDefined(x, y))
      pixelCount += 1
    assertResult(1)(pixelCount)
    assertResult(Array[Short](123, 55))(tile3.getPixelValue(10, 0))
  }

  test("explode operation") {
    val metadata = new RasterMetadata(0, 0, 20, 20, 10, 10, 4326,
      new AffineTransform())
    val pixels = sparkContext.parallelize(Seq(
      (0, 0, 100),
      (1, 0, 200),
      (0, 1, 50),
      (2, 0, 300),
      (9, 9, 10),
      (10, 9, 16),
      (10, 10, 18),
      (11, 9, 25),
    ))
    val raster: RasterRDD[Int] = RasterOperationsGlobal.rasterizePixels(pixels, metadata)

    assertResult(1)(raster.map(_.rasterMetadata).distinct().count)
    assertResult(3)(raster.count)
    val outputRaster = RasterOperationsLocal.explode(raster)
    assertResult(3)(outputRaster.map(_.rasterMetadata).distinct().count)
    val tile = outputRaster.first()
    val corners = Array[Double](tile.x1, tile.y1, tile.x2 + 1, tile.y2 + 1)
    tile.rasterMetadata.g2m.transform(corners, 0, corners, 0, 2)
    assertResult(10)(corners(2) - corners(0))
  }
}
