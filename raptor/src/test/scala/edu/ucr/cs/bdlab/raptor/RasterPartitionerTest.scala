package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform

@RunWith(classOf[JUnitRunner])
class RasterPartitionerTest extends AnyFunSuite with ScalaSparkTest {

  test("Simple square raster") {
    val rasterMetadata = new RasterMetadata(0, 0, 1000, 1000,
      100, 100, 4326, new AffineTransform())
    val partitioner = new RasterPartitioner(rasterMetadata, 25)
    assertResult(0)(partitioner.getPartition(0))
    assertResult(0)(partitioner.getPartition(11))
    assertResult(24)(partitioner.getPartition(99))
  }
}
