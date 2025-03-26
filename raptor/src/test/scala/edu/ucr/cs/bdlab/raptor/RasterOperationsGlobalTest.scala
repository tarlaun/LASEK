package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform

@RunWith(classOf[JUnitRunner])
class RasterOperationsGlobalTest extends AnyFunSuite with ScalaSparkTest {
  test("create raster from pixels") {
    val rasterMetadata = new RasterMetadata(0, 0, 10, 10, 5, 5, 4326, new AffineTransform())
    val pixels = sparkContext.parallelize(Seq(
      (0, 0, 10),
      (2, 0, 5),
      (7, 3, 12),
      (8, 9, 50)
    ))
    val raster = RasterOperationsGlobal.rasterizePixels(pixels, rasterMetadata)
    assertResult(3)(raster.count())
    val tile1 = raster.filter(_.tileID == 0).first()
    assertResult(10)(tile1.getPixelValue(0, 0))
    assertResult(5)(tile1.getPixelValue(2, 0))
  }

}
