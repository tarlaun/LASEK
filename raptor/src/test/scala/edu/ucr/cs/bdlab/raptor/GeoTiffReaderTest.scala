package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.test.ScalaSparkTest
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.GridFormatFinder
import org.junit.runner.RunWith
import org.opengis.parameter.GeneralParameterValue
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.Point2D
import java.io.File

@RunWith(classOf[JUnitRunner])
class GeoTiffReaderTest extends AnyFunSuite with ScalaSparkTest {

  test("Read Stripped file") {
    val rasterPath = new Path(makeFileCopy("/rasters/FRClouds.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Int]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      assert(reader.metadata.rasterWidth == 99)
      assert(reader.metadata.rasterHeight == 72)
      assert(reader.metadata.getPixelScaleX == 0.17578125)
      // Test conversion
      // Transform origin point from raster to vector
      val outPoint = new Point2D.Double
      reader.metadata.gridToModel(0, 0, outPoint)
      assert(-6.679688 - outPoint.x < 1E-3)
      assert(53.613281 - outPoint.y < 1E-3)
      // Transform the other corner point
      reader.metadata.gridToModel(reader.metadata.rasterWidth, reader.metadata.rasterHeight, outPoint)
      assert(10.7226 - outPoint.x < 1E-3)
      assert(40.957 - outPoint.y < 1E-3)
      // Test the inverse transformation on one corner
      reader.metadata.modelToGrid(-6.679688, 53.613281, outPoint)
      assert(outPoint.getX.toInt == 0)
      assert(outPoint.getY.toInt == 0)
      // Test the inverse transformation
      reader.metadata.modelToGrid(-0.06, 49.28, outPoint)
      assert(outPoint.getX.toInt == 37)
      assert(outPoint.getY.toInt == 24)
      val tileID = reader.metadata.getTileIDAtPixel(37, 24)
      val tile = reader.readTile(tileID)
      val pixel = tile.getPixelValue(37, 24)
      assertResult(Array(0x45, 0x9c, 0x8b))(pixel)
    } finally {
      reader.close()
    }
  }

  test("Read tiled GeoTIFF") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_small.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)

      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(23.224, 32.415))
      val tile2 = reader.readTile(reader.metadata.getTileIDAtPoint(33.694, 14.761))
      assertResult(8)(tile1.getPointValue(23.224, 32.415))
      assertResult(22)(tile2.getPointValue(33.694, 14.761))
    } finally {
      reader.close()
    }
  }

  test("Read banded GeoTIFF") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_banded_small.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Float]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)
      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(31.277, 26.954))
      assertArrayEquals(Array[Float](880664.8f, 16.0f), tile1.getPointValue(31.277, 26.954), 1E-3F)
    } finally {
      reader.close()
    }
  }

  test("Read projected GeoTIFF file") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_small_EPSG3857.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(5 == reader.metadata.rasterWidth)
      assert(4 == reader.metadata.rasterHeight)
      assert(3857 == reader.metadata.srid)
    } finally {
      reader.close()
    }
  }

  test("Read projected GeoTIFF Landsat") {
    val rasterPath = new Path(locateResource("/rasters/sample.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val srid = reader.metadata.srid
      assert(srid != 0)

      //GeoTools
      val format = GridFormatFinder.findFormat(new File(rasterPath.toString))
      val Greader = format.getReader(new File(rasterPath.toString))
      val coverage: GridCoverage2D = Greader.read(Array[GeneralParameterValue]())
      val crsGeotools = coverage.getCoordinateReferenceSystem
      val sridGeotools = CRSServer.crsToSRID(crsGeotools)

      assert(sridGeotools == srid)
    } finally {
      reader.close()
    }
  }

  test("Use default planar configuration if not set") {
    val rasterPath = new Path(locateResource("/rasters/glc2000_small_noplanar.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val tile = reader.readTile(0)
      assertResult(20)(tile.getPixelValue(5, 0))
    } finally {
      reader.close()
    }
  }

  test("Read a file with some incomplete tiles") {
    val rasterPath = new Path(locateResource("/rasters/FRClouds.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      for (tileID <- reader.metadata.tileIDs) {
        val tile = reader.readTile(tileID)
        for ((x, y) <- tile.pixelLocations)
          tile.isDefined(x, y)
      }

    } finally {
      reader.close()
    }
  }

  test("Read a file a separate mask layer") {
    val rasterPath = new Path(locateResource("/rasters/masked.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val tile = reader.readTile(0)
      assertResult(32)(tile.tileWidth)
      assert(!tile.isDefined(0, 0))
      assert(tile.isDefined(6, 0))
      assert(!tile.isDefined(9, 0))
    } finally {
      reader.close()
    }
  }

  test("Read tiled Big GeoTiff Big Endian") {
    val rasterPath = new Path(locateResource("/rasters/glc2000_bigtiff2.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)
      reader.readTile(0)
    } finally {
      reader.close()
    }
  }

  test("Override fill value") {
    val rasterPath = locateResource("/rasters/glc2000_small.tif")
    val fileSystem = new Path(rasterPath.getPath).getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.getPath, "0", "fillvalue" -> 8)
      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(23.224, 32.415))
      assert(tile1.isEmptyAt(23.224, 32.415))
    } finally {
      reader.close()
    }
  }

  test("Read GeoTIFF with 64-bit numbers") {
    val rasterPath = new Path(makeFileCopy("/rasters/NLDAS-64.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Double]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      val tile = reader.readTile(0)
      assertResult(DoubleType)(tile.componentType)
      assertResult(11)(tile.numComponents)
      var pixelValue: Array[Double] = tile.getPixelValue(0, 0)
      assert((pixelValue(0) - -0.96).abs < 1E-3)
      assert((pixelValue(5) - 261.43).abs < 1E-3)
      pixelValue = tile.getPixelValue(1, 2)
      assert((pixelValue(0) - -1.51).abs < 1E-3)
      assert((pixelValue(4) - 5.36).abs < 1E-3)
    } finally {
      reader.close()
    }
  }
}
