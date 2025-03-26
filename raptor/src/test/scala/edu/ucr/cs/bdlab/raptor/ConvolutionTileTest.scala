package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.SpatialReader
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureReader
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform

@RunWith(classOf[JUnitRunner])
class ConvolutionTileTest extends AnyFunSuite with ScalaSparkTest {

  test("Single band convolution") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())

    val tile1 = new MemoryTile[Float](0, metadata)
    tile1.setPixelValue(0, 0, 0.5f)
    tile1.setPixelValue(1, 0, 0.25f)
    tile1.setPixelValue(0, 1, 3.00f)
    tile1.setPixelValue(1, 1, 1.00f)
    tile1.setPixelValue(9, 9, 4.00f)
    val convWindow1 = new ConvolutionTileSingleBand(0, metadata, 1, Array.fill(9)(0.11f), tile1.tileID)
    convWindow1.addTile(tile1)
    val tile2 = new MemoryTile[Float](1, metadata)
    tile2.setPixelValue(10, 0, 5.0f)
    tile2.setPixelValue(10, 9, 5.0f)
    val convWindow2 = new ConvolutionTileSingleBand(0, metadata, 1, Array.fill(9)(0.11f), tile2.tileID)
    convWindow2.addTile(tile2)

    val convWindow = convWindow1.merge(convWindow2)

    assertResult(0.99f)(convWindow.getPixelValue(9, 9))
    assert(convWindow.isDefined(9, 8))
    assertResult(0.99f)(convWindow.getPixelValue(9, 8))
    assert(convWindow.isEmpty(9, 7))
  }


  test("Multi band convolution") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())

    val tile1 = new MemoryTile[Array[Float]](0, metadata)
    tile1.setPixelValue(0, 0, Array(0.5f, 0.1f))
    tile1.setPixelValue(1, 0, Array(0.25f, 0.4f))
    tile1.setPixelValue(0, 1, Array(3.00f, 2.1f))
    tile1.setPixelValue(1, 1, Array(1.00f, 3.0f))
    tile1.setPixelValue(9, 9, Array(4.00f, 0.5f))
    val convWindow1 = new ConvolutionTileMultiBand(0, metadata, 2, 1, Array.fill(9)(0.11f), tile1.tileID)
    convWindow1.addTile(tile1)
    val tile2 = new MemoryTile[Array[Float]](1, metadata)
    tile2.setPixelValue(10, 0, Array(5.0f, 10.0f))
    tile2.setPixelValue(10, 9, Array(5.0f, 20.0f))
    val convWindow2 = new ConvolutionTileMultiBand(0, metadata, 2, 1, Array.fill(9)(0.11f), tile2.tileID)
    convWindow2.addTile(tile2)

    val convWindow = convWindow2.merge(convWindow1)

    assertResult(Array(0.99f, 2.255f))(convWindow.getPixelValue(9, 9))
    assert(convWindow.isDefined(9, 8))
    assertResult(Array(0.99f, 2.255f))(convWindow.getPixelValue(9, 8))
    assert(convWindow.isEmpty(9, 7))
  }
}
