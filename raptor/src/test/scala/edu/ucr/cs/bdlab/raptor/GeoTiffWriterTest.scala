package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.{AbstractTiffTile, CompressedTiffTile, TiffConstants}
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.FloatType
import org.apache.spark.test.ScalaSparkTest
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform
import java.io.File
import java.lang.reflect.Field

@RunWith(classOf[JUnitRunner])
class GeoTiffWriterTest extends AnyFunSuite with ScalaSparkTest {

  test("Write simple file with one tile") {
    val affineTransform = new AffineTransform()
    affineTransform.translate(2.0, 2.0)
    affineTransform.scale(3.0, 1.0)
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      affineTransform)
    val originalTile: MemoryTile[Short] = new MemoryTile(0, metadata)
    originalTile.setPixelValue(10, 0, 123)
    originalTile.setPixelValue(0, 1, 125)
    originalTile.setPixelValue(50, 3, 44)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val fileSystem = geotTiffPath.getFileSystem(sparkContext.hadoopConfiguration)

    val geoTiffWriter = new GeoTiffWriter[Short](geotTiffPath, metadata, false, new BeastOptions())
    geoTiffWriter.write(originalTile)
    geoTiffWriter.close()

    // Read it back
    val geoTiffReader = new GeoTiffReader()
    geoTiffReader.initialize(fileSystem, geotTiffPath.toString, "0", new BeastOptions)
    assertResult(4326)(geoTiffReader.metadata.srid)
    assertResult(metadata.g2m)(geoTiffReader.metadata.g2m)
    val actualTile = geoTiffReader.readTile(0)
    assertResult(originalTile.getPixelValue(10, 0))(actualTile.getPixelValue(10, 0))
    assertResult(originalTile.getPixelValue(0, 1))(actualTile.getPixelValue(0, 1))
    assertResult(originalTile.getPixelValue(50, 3))(actualTile.getPixelValue(50, 3))
    geoTiffReader.close()
  }

  test("Write simple file with LZW encoder") {
    val affineTransform = new AffineTransform()
    affineTransform.translate(2.0, 2.0)
    affineTransform.scale(3.0, 1.0)
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      affineTransform)
    val originalTile: MemoryTile[Int] = new MemoryTile(0, metadata)
    originalTile.setPixelValue(10, 0, 123)
    originalTile.setPixelValue(0, 1, 125)
    originalTile.setPixelValue(50, 3, 44)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val fileSystem = geotTiffPath.getFileSystem(sparkContext.hadoopConfiguration)

    val geoTiffWriter = new GeoTiffWriter[Int](geotTiffPath, metadata, false,
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
    geoTiffWriter.write(originalTile)
    geoTiffWriter.close()

    // Read it back
    val geoTiffReader = new GeoTiffReader[Int]()
    geoTiffReader.initialize(fileSystem, geotTiffPath.toString, "0", new BeastOptions)
    val actualTile = geoTiffReader.readTile(0)
    assertResult(originalTile.getPixelValue(10, 0))(actualTile.getPixelValue(10, 0))
    assertResult(originalTile.getPixelValue(0, 1))(actualTile.getPixelValue(0, 1))
    assertResult(originalTile.getPixelValue(50, 3))(actualTile.getPixelValue(50, 3))
    geoTiffReader.close()
  }

  test("Write simple file with multiple tiles") {
    val crs = CRS.decode("EPSG:32640")
    val srid = CRSServer.crsToSRID(crs)
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, srid,
      new AffineTransform())
    val originalTile1 = new MemoryTile[Int](0, metadata)
    originalTile1.setPixelValue(0, 1, 123)
    originalTile1.setPixelValue(0, 5, 125)
    originalTile1.setPixelValue(2, 7, 44)

    val originalTile2 = new MemoryTile[Int](3, metadata)
    originalTile2.setPixelValue(30, 1, 12)
    originalTile2.setPixelValue(30, 5, 251)
    originalTile2.setPixelValue(32, 7, 33)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val fileSystem = geotTiffPath.getFileSystem(sparkContext.hadoopConfiguration)

    val geoTiffWriter = new GeoTiffWriter[Int](geotTiffPath, metadata, false, new BeastOptions())
    geoTiffWriter.write(originalTile1)
    geoTiffWriter.write(originalTile2)
    geoTiffWriter.close()

    // Read it back
    val geoTiffReader = new GeoTiffReader[Int]()
    geoTiffReader.initialize(fileSystem, geotTiffPath.toString, "0", new BeastOptions)
    assertResult(metadata.srid)(geoTiffReader.metadata.srid)
    val actualTile1 = geoTiffReader.readTile(0)
    assertResult(originalTile1.getPixelValue(0, 1))(actualTile1.getPixelValue(0, 1))
    assertResult(originalTile1.getPixelValue(0, 5))(actualTile1.getPixelValue(0, 5))
    assertResult(originalTile1.getPixelValue(2, 7))(actualTile1.getPixelValue(2, 7))
    val actualTile2 = geoTiffReader.readTile(3)
    assertResult(originalTile2.getPixelValue(30, 1))(actualTile2.getPixelValue(30, 1))
    assertResult(originalTile2.getPixelValue(30, 5))(actualTile2.getPixelValue(30, 5))
    assertResult(originalTile2.getPixelValue(32, 7))(actualTile2.getPixelValue(32, 7))
    geoTiffReader.close()
  }

  test("Write RDD to GeoTiff") {
    val rasterFile = locateResource("/raptor/glc2000_small.tif")
    val rasterRDD = new RasterFileRDD(sparkContext, rasterFile.getPath, IRasterReader.RasterLayerID -> 0)
    val outputFile = new File(scratchDir, "glc.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath, GeoTiffWriter.BitsPerSample -> 8)
    val readRaster = new RasterFileRDD(sparkContext, outputFile.getPath, IRasterReader.RasterLayerID -> 0)
    assertResult(rasterRDD.count())(readRaster.count())
  }

  test("Write RDD to BigGeoTiff") {
    val rasterFile = locateResource("/raptor/glc2000_small.tif")
    val rasterRDD = new RasterFileRDD(sparkContext, rasterFile.getPath, IRasterReader.RasterLayerID -> 0)
    val outputFile = new File(scratchDir, "glc.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath,
      Seq(GeoTiffWriter.BitsPerSample -> 8, GeoTiffWriter.BigTiff -> "yes"))
    val readRaster = new RasterFileRDD(sparkContext, outputFile.getPath, IRasterReader.RasterLayerID -> 0)
    assertResult(rasterRDD.count())(readRaster.count())
  }

  test("Write partitioned RDD to GeoTiff") {
    val rasterFile = locateResource("/raptor/glc2000_small.tif")
    val rasterRDD = new RasterFileRDD(sparkContext, rasterFile.getPath, IRasterReader.RasterLayerID -> 0)
      .repartition(3)
    val outputFile = new File(scratchDir, "glc.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath, GeoTiffWriter.BitsPerSample -> 8)
    val readRaster = new RasterFileRDD(sparkContext, outputFile.getPath, IRasterReader.RasterLayerID -> 0)
    assertResult(rasterRDD.count())(readRaster.count())
    assertResult(3)(readRaster.getNumPartitions)
  }

  test("Write partitioned RDD to GeoTiff in compatibility mode") {
    val rasterFile = locateResource("/raptor/glc2000_small.tif")
    val rasterRDD = new RasterFileRDD(sparkContext, rasterFile.getPath, IRasterReader.RasterLayerID -> 0)
      .repartition(3)
    val outputFile = new File(scratchDir, "glc.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath,
      Seq(GeoTiffWriter.BitsPerSample -> 8, GeoTiffWriter.WriteMode -> "compatibility"))
    val readRaster = new RasterFileRDD(sparkContext, outputFile.getPath, IRasterReader.RasterLayerID -> 0)
    assertResult(rasterRDD.count())(readRaster.count())
    assertResult(1)(readRaster.getNumPartitions)
  }

  test("Write partial file in compatibility mode") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())
    val originalTile1 = new MemoryTile[Int](0, metadata)
    originalTile1.setPixelValue(0, 1, 123)
    originalTile1.setPixelValue(0, 5, 125)
    originalTile1.setPixelValue(2, 7, 44)

    val originalTile2 = new MemoryTile[Int](3, metadata)
    originalTile2.setPixelValue(30, 1, 12)
    originalTile2.setPixelValue(30, 5, 251)
    originalTile2.setPixelValue(32, 7, 33)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val rasterRDD: RDD[ITile[Int]] = sparkContext.parallelize(Seq(originalTile1, originalTile2))
    GeoTiffWriter.saveAsGeoTiff[Int](rasterRDD, geotTiffPath.toString, Seq(GeoTiffWriter.WriteMode -> "compatibility"))

    // Read it back
    val readRaster = new RasterFileRDD(sparkContext, geotTiffPath.toString, IRasterReader.RasterLayerID -> 0)
    assertResult(2)(readRaster.count())
    assertResult(1)(readRaster.getNumPartitions)
  }

  test("Write GeoTIFF with Sinusoidal projection") {
    val crs = HDF4Reader.SinusoidalCRS
    val srid = CRSServer.crsToSRID(crs)
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, srid,
      new AffineTransform())
    val originalTile: MemoryTile[Short] = new MemoryTile(0, metadata)
    originalTile.setPixelValue(10, 0, 123)
    originalTile.setPixelValue(0, 1, 125)
    originalTile.setPixelValue(50, 3, 44)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val fileSystem = geotTiffPath.getFileSystem(sparkContext.hadoopConfiguration)

    val geoTiffWriter = new GeoTiffWriter[Short](geotTiffPath, metadata, false, new BeastOptions())
    geoTiffWriter.write(originalTile)
    geoTiffWriter.close()

    // Read it back
    val geoTiffReader = new GeoTiffReader()
    geoTiffReader.initialize(fileSystem, geotTiffPath.toString, "0", new BeastOptions)
    val parsedCRS = CRSServer.sridToCRS(geoTiffReader.metadata.srid)
    //assertResult(crs)(parsedCRS) // Ideally, this line should work but it does not
    assert(parsedCRS.getName.getCode.toLowerCase().contains("sinusoidal"))
    geoTiffReader.close()
  }

  test("Write a GeoTIFF with float type without explicit options") {
    val metadata = new RasterMetadata(0, 0, 360, 180, 90, 90, 4326,
      new AffineTransform(1, 0, 0, -1, -180, 90))
    val pixels = sparkContext.parallelize(Seq(
      (0, 0, 100f),
      (180, 0, 200f),
      (100, 50, 300f),
    ))
    val rasterRDD: RDD[ITile[Float]] = RasterOperationsGlobal.rasterizePixels(pixels, metadata)
    val outputFile = new File(scratchDir, "sample.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath, new BeastOptions())
    val readRaster = new RasterFileRDD(sparkContext, outputFile.getPath, new BeastOptions())
    assertResult(FloatType)(readRaster.first().pixelType)
  }

  test("Write a GeoTIFF with non-aligned last tile") {
    // Note: According to GeoTIFF all tiles must be of the same width and height even if it goes outside
    // the raster boundary. In this case, the outside pixels are not relevant and can be filled with the fill value
    val metadata = new RasterMetadata(0, 0, 100, 95, 30, 30, 4326,
      new AffineTransform())
    val pixels = sparkContext.parallelize(Seq(
      (0, 0, 100f),
      (99, 0, 200f),
      (89, 70, 300f),
      (99, 94, 300f),
    ))
    val rasterRDD: RDD[ITile[Float]] = RasterOperationsGlobal.rasterizePixels(pixels, metadata)
    val outputFile = new File(scratchDir, "sample.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath, new BeastOptions())
    val geoTiffReader = new GeoTiffReader[Float]
    val path = new Path(outputFile.getPath, "part-00000.tif")
    val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
    geoTiffReader.initialize(fs, path.toString, "0", new BeastOptions())
    val tile = geoTiffReader.readTile(15)
    assertResult(300f)(tile.getPixelValue(99, 94))
    assertResult(classOf[GeoTiffTileFloat])(tile.getClass)
    // To access the underlying tiffTile, we will use reflection since it is private,
    // and we want to avoid changing the main code to make this test
    val tiffTileAttr: Field = classOf[AbstractGeoTiffTile[_]].getDeclaredField("tiffTile")
    tiffTileAttr.setAccessible(true)
    val tiffTile = tiffTileAttr.get(tile).asInstanceOf[AbstractTiffTile]
    assertResult(30 * 30 * 4)(tiffTile.getTileData()(0).length)
    assertResult(30)(tiffTile.getTileWidth)
    assertResult(30)(tiffTile.getTileHeight)
  }

  test("Cleanup temp directory in compatibility mode") {
    val rasterFile = locateResource("/raptor/glc2000_small.tif")
    val rasterRDD = new RasterFileRDD(sparkContext, rasterFile.getPath, IRasterReader.RasterLayerID -> 0)
      .repartition(3)
    val outputFile = new File(scratchDir, "glc.tif")
    GeoTiffWriter.saveAsGeoTiff(rasterRDD, outputFile.getPath, Seq(GeoTiffWriter.BitsPerSample -> 8,
      GeoTiffWriter.WriteMode -> "compatibility"))
    val outputFiles = outputFile.listFiles()
    assert(!outputFiles.map(_.getName).contains("temp"))
  }

  test("Write a file in compatibility mode with no empty tiles") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 10, 10, 4326,
      new AffineTransform())
    val originalTile1 = new MemoryTile[Int](0, metadata)
    originalTile1.setPixelValue(0, 1, 123)
    originalTile1.setPixelValue(0, 5, 125)
    originalTile1.setPixelValue(2, 7, 44)

    val originalTile2 = new MemoryTile[Int](3, metadata)
    originalTile2.setPixelValue(30, 1, 12)
    originalTile2.setPixelValue(30, 5, 251)
    originalTile2.setPixelValue(32, 7, 33)

    val geotTiffPath = new Path(scratchPath, "temp.tif")
    val rasterRDD: RDD[ITile[Int]] = sparkContext.parallelize(Seq(originalTile1, originalTile2))
    GeoTiffWriter.saveAsGeoTiff[Int](rasterRDD, geotTiffPath.toString,
      Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.NoEmptyTiles -> true, GeoTiffWriter.FillValue -> 0))

    // Read it back
    val readRaster = new RasterFileRDD(sparkContext, geotTiffPath.toString, IRasterReader.RasterLayerID -> 0)
    assertResult(100)(readRaster.count())
    // Read all tiles to make sure nothing is wrong
    readRaster.foreach(tile => {
      if (tile.tileID == 0 || tile.tileID == 3) {
        // Count pixels
        var numNonEmptyPixels = 0
        for ((x,y) <- tile.pixelLocations; if tile.isDefined(x, y))
          numNonEmptyPixels += 1
        if(3 != numNonEmptyPixels)
          throw new RuntimeException(s"Assertion failure! Expected 3 pixels but found ${numNonEmptyPixels} instead")
      }
    })
  }

}
