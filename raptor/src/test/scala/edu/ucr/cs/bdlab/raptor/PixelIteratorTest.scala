package edu.ucr.cs.bdlab.raptor

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PixelIteratorTest extends AnyFunSuite with ScalaSparkTest {

  test("EmptyResults") {
    val rasterFile = makeFileCopy("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf").getPath
    val i = Array(
      (0.toLong, PixelRange(0.toLong, 0, 2, 5)),
      (0.toLong, PixelRange(0.toLong, 1, 5, 9))
    )
    val pixelIterator = new PixelIterator(i.iterator, Array(rasterFile), "LST_Day_1km")
    assert(pixelIterator.isEmpty)
  }

  test("NonEmptyResults") {
    val rasterFile = makeFileCopy("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf").getPath
    val i = Array(
      (0.toLong, PixelRange(0.toLong, 0, 0, 7)),
      (0.toLong, PixelRange(0.toLong, 2, 0, 3))
    )
    val pixelIterator = new PixelIterator[Float](i.iterator, Array(rasterFile), "LST_Day_1km")
    val values: Array[(Long, Float)] = pixelIterator.map(x => (x.featureID, x.m)).toArray
    assert(values.length == 4)
    assert(values(0)._2 == (15240 / 50.0).toFloat)
    assert(values(1)._2 == (15240 / 50.0).toFloat)
    assert(values(2)._2 == (15349 / 50.0).toFloat)
    assert(values(3)._2 == (15383 / 50.0).toFloat)
  }
}
