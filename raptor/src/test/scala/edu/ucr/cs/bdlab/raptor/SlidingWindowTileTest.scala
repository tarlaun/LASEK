package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform
import java.io.ByteArrayInputStream

@RunWith(classOf[JUnitRunner])
class SlidingWindowTileTest extends AnyFunSuite with ScalaSparkTest {
  test("corner tile") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())
    val windowTile = new SlidingWindowTile(0, metadata, 1, (values: Array[Int], defined: Array[Boolean]) => {
      // Sum all the defined values
      values.zip(defined).filter(_._2).map(_._1).sum
    })
    val tile1 = new MemoryTile[Int](0, metadata)
    tile1.setPixelValue(0, 0, 10)
    tile1.setPixelValue(1, 0, 5)
    tile1.setPixelValue(0, 1, 7)
    tile1.setPixelValue(5, 5, 25)
    tile1.setPixelValue(5, 6, 71)
    tile1.setPixelValue(6, 6, 1)
    tile1.setPixelValue(9, 9, 100)
    windowTile.addTile(tile1)
    val tile2 = new MemoryTile[Int](1, metadata)
    tile2.setPixelValue(10, 9, 3)
    windowTile.addTile(tile2)
    val tile3 = new MemoryTile[Int](10, metadata)
    tile3.setPixelValue(9, 10, 15)
    windowTile.addTile(tile3)
    val tile4 = new MemoryTile[Int](11, metadata)
    tile4.setPixelValue(10, 10, 1000)
    windowTile.addTile(tile4)

    val finalTile = windowTile.resultTile
    assertResult(0)(finalTile.tileID)
    assertResult(22)(finalTile.getPixelValue(0, 0))
    assertResult(1118)(finalTile.getPixelValue(9, 9))
    assertResult(97)(finalTile.getPixelValue(5, 5))
  }

  test("corner tile with merge") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())
    val windowTile = new SlidingWindowTile(0, metadata, 1, (values: Array[Int], defined: Array[Boolean]) => {
      // Sum all the defined values
      values.zip(defined).filter(_._2).map(_._1).sum
    })
    val tile1 = new MemoryTile[Int](0, metadata)
    tile1.setPixelValue(0, 0, 10)
    tile1.setPixelValue(1, 0, 5)
    tile1.setPixelValue(0, 1, 7)
    tile1.setPixelValue(5, 5, 25)
    tile1.setPixelValue(5, 6, 71)
    tile1.setPixelValue(6, 6, 1)
    tile1.setPixelValue(9, 9, 100)
    windowTile.addTile(tile1)
    val tile2 = new MemoryTile[Int](1, metadata)
    tile2.setPixelValue(10, 9, 3)
    windowTile.addTile(tile2)

    val windowTile2 = new SlidingWindowTile(0, metadata, 1, (values: Array[Int], defined: Array[Boolean]) => {
      // Sum all the defined values
      values.zip(defined).filter(_._2).map(_._1).sum
    })

    val tile3 = new MemoryTile[Int](10, metadata)
    tile3.setPixelValue(9, 10, 15)
    windowTile2.addTile(tile3)
    val tile4 = new MemoryTile[Int](11, metadata)
    tile4.setPixelValue(10, 10, 1000)
    windowTile2.addTile(tile4)

    windowTile.merge(windowTile2)

    val finalTile = windowTile.resultTile
    assertResult(0)(finalTile.tileID)
    assertResult(22)(finalTile.getPixelValue(0, 0))
    assertResult(1118)(finalTile.getPixelValue(9, 9))
    assertResult(97)(finalTile.getPixelValue(5, 5))
  }
}
