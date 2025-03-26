package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.{Point2D, Rectangle2D}

@RunWith(classOf[JUnitRunner])
class HDF4ReaderTest extends AnyFunSuite with ScalaSparkTest {

  test("Metadata") {
    val hdfFile = new Path(makeFileCopy("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf").getPath)
    val fileSystem = hdfFile.getFileSystem(new Configuration)
    val reader = new HDF4Reader
    try {
      reader.initialize(fileSystem, hdfFile.toString, "water_mask", new BeastOptions())
      val tile = reader.readTile(0)
      assert(1 == tile.numComponents)
      assert(4800 == tile.tileWidth)
      val value = tile.getPixelValue(0, 0)
      assertResult(1.0)(value)
      assertResult(1)(reader.metadata.numTiles)
    } finally reader.close()
  }

  test("Projection") {
    val hdfFile = new Path(makeFileCopy("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf").getPath)
    val fileSystem = hdfFile.getFileSystem(new Configuration)
    val reader = new HDF4Reader
    try {
      reader.initialize(fileSystem, hdfFile.toString, "water_mask", new BeastOptions())
      val pt = new Point2D.Double
      reader.metadata.gridToModel(0.5, 0.5, pt)
      assert(((Math.toRadians(-110 + 10 * 0.5 / 4800) * HDF4Reader.Scale) - pt.x).abs < 1E-3)
      assert(((Math.toRadians(30.0 - 10 * 0.5 / 4800) * HDF4Reader.Scale) - pt.y).abs < 1E-3)

      reader.metadata.modelToGrid(Math.toRadians(-110.0) * HDF4Reader.Scale, Math.toRadians(30.0) * HDF4Reader.Scale, pt)
      assert((0.0 - pt.x).abs < 1E-3)
      assert((0.0 - pt.y).abs < 1E-3)

      reader.metadata.modelToGrid(Math.toRadians(-110.0 + 10.0 / 4800) * HDF4Reader.Scale, Math.toRadians(30.0 - 10.0 / 4800) * HDF4Reader.Scale, pt)
      assert((1.0 - pt.x).abs < 1E-3)
      assert((1.0 - pt.y).abs < 1E-3)

      reader.metadata.gridToModel(50.5, 122.5, pt)
      reader.metadata.modelToGrid(pt.x, pt.y, pt)
      assert((50.5 - pt.x).abs < 1E-3)
      assert((122.5 - pt.y).abs < 1E-3)

      reader.metadata.gridToModel(4799.5, 4799.5, pt)
      assert(((Math.toRadians(-100 - 10 * 0.5 / 4800) * HDF4Reader.Scale) - pt.x).abs < 1E-3)
      assert(((Math.toRadians(20.0 + 10 * 0.5 / 4800) * HDF4Reader.Scale) - pt.y).abs < 1E-3)
    } finally reader.close()
  }

  test("Select tiles") {
    val tileIDFilter = HDF4Reader.createTileIDFilter(new Rectangle2D.Double(Math.toRadians(-145.0) * HDF4Reader.Scale,
      Math.toRadians(5.0) * HDF4Reader.Scale, Math.toRadians(29.0) * HDF4Reader.Scale, Math.toRadians(49.0) * HDF4Reader.Scale))
    assert(tileIDFilter.accept(new Path("tile-h03v03.hdf")))
    assert(tileIDFilter.accept(new Path("tile-h06v07.hdf")))
    assert(!tileIDFilter.accept(new Path("tile-h02v09.hdf")))
    assert(!tileIDFilter.accept(new Path("tile-h07v06.hdf")))
  }

  test("Select dates") {
    val dateFilter = HDF4Reader.createDateFilter("2001.02.15", "2005.02.11")
    assert(dateFilter.accept(new Path("2001.02.15")))
    assert(dateFilter.accept(new Path("2005.02.11")))
    assert(dateFilter.accept(new Path("2003.07.15")))
    assert(!dateFilter.accept(new Path("2005.02.12")))
    assert(!dateFilter.accept(new Path("2001.01.31")))
  }

  test("Load another file") {
    val hdfFile = new Path(makeFileCopy("/rasters/MOD44W.A2000055.h07v06.005.2009212172956.hdf").getPath)
    val fileSystem = hdfFile.getFileSystem(new Configuration)
    val reader = new HDF4Reader
    try {
      reader.initialize(fileSystem, hdfFile.toString, "water_mask", new BeastOptions())
      val value = reader.readTile(0).getPixelValue(0, 0)
      assertResult(1)(value)
      reader.close()
    } finally reader.close()

    val hdfFile2 = new Path(makeFileCopy("/rasters/MYD11A1.A2002185.h09v06.006.2015146150958.hdf").getPath)
    try {
      reader.initialize(fileSystem, hdfFile2.toString, "LST_Day_1km", new BeastOptions())
      val value = reader.readTile(0).getPixelValue(0, 0)
      assertResult(15240 / 50.0f)(value)
      reader.close()
    } finally reader.close()
  }
}
