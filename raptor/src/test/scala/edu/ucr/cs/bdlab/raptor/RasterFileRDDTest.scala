package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.test.ScalaSparkTest
import org.geotools.coverage.grid.io.GridFormatFinder
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.Point2D
import java.io.File

@RunWith(classOf[JUnitRunner])
class RasterFileRDDTest extends AnyFunSuite with ScalaSparkTest {

  test("Single Partition and number of tiles check"){
    val file = makeResourceCopy("/rasters/glc2000_small.tif")
    val fileTileRDD = new RasterFileRDD(sparkContext, file.toString, new BeastOptions())
    // assert no. of partition
    assertResult(fileTileRDD.getNumPartitions)(1)
    //assert no. of tiles
    assertResult(fileTileRDD.count)(8)
  }

  test("Two Partition and number of tiles check"){
    val file = makeResourceCopy("/rasters/glc2000_small.tif")
    val fileTileRDD = new RasterFileRDD(sparkContext, file.toString, SpatialFileRDD.MaxSplitSize -> 4096)
    assertResult(2)(fileTileRDD.getNumPartitions)
    assertResult(8)(fileTileRDD.count)
  }

  test("Verify Metadata and Read pixel values"){
    val file = locateResource("/rasters/glc2000_small.tif")
    val fileTileRDD = new RasterFileRDD[Int](sparkContext, file.toString, new BeastOptions())

    assertResult(128)(fileTileRDD.first.rasterMetadata.rasterHeight)
    assertResult(256)(fileTileRDD.first.rasterMetadata.rasterWidth)

    assertResult(8)(fileTileRDD.filter(t => t.rasterMetadata.getTileIDAtPoint(23.224, 32.415) == t.tileID).map(x => {
      x.getPointValue(23.224, 32.415)
    }).collect()(0))

    assertResult(22)(fileTileRDD.filter(t => t.rasterMetadata.getTileIDAtPoint(33.694, 14.761) == t.tileID).map(x => {
      x.getPointValue(33.694, 14.761)
    }).collect()(0))
  }

  test("Verify number of partitions and number of tiles for multiple files in a directory") {
    val dir = new File(scratchDir, "dir")
    dir.mkdirs()
    val file = new File(dir, "test.tif")
    val file1 = new File(dir, "test1.tif")
    copyResource("/rasters/glc2000_small.tif", file)
    copyResource("/rasters/glc2000_small_EPSG3857.tif", file1)
    val tileCount = 9
    val fileTileRDD = new RasterFileRDD(sparkContext, dir.getPath, new BeastOptions())

    assertResult(2)(fileTileRDD.getNumPartitions)
    assertResult(tileCount)(fileTileRDD.count)
  }

  test("Verify metadata and pixel values for multiple files in a directory") {
    val dir = new File(scratchDir, "dir")
    dir.mkdirs()
    val file = new File(dir, "test.tif")
    val file1 = new File(dir, "test1.tif")
    copyResource("/rasters/glc2000_small.tif", file)
    copyResource("/rasters/glc2000_small_EPSG3857.tif", file1)

    val rasterHeight:Array[Int] = Array(4,128)
    val rasterWidth:Array[Int] = Array(5,256)
    val pixelValues:Array[Int] = Array(12,20)
    val fileTileRDD = new RasterFileRDD[Int](sparkContext, dir.getPath, new BeastOptions())

    assertArrayEquals(fileTileRDD.map(x => x.rasterMetadata.rasterHeight).distinct().sortBy(identity).collect(),rasterHeight)
    assertArrayEquals(fileTileRDD.map(x => x.rasterMetadata.rasterWidth).distinct().sortBy(identity).collect(),rasterWidth)
    assertResult(pixelValues)(fileTileRDD.filter(t => t.rasterMetadata.getTileIDAtPixel(0, 0) == t.tileID)
      .map(x => x.getPixelValue(0, 0)).distinct()
      .sortBy(identity)
      .collect())
  }

  test("Override CRS"){
    val file = makeResourceCopy("/rasters/FRClouds.tif")
    val fileTileRDD = new RasterFileRDD(sparkContext, file.toString, new BeastOptions(IRasterReader.OverrideSRID -> 4326))
    assertResult(4326)(fileTileRDD.first().rasterMetadata.srid)
  }
}
