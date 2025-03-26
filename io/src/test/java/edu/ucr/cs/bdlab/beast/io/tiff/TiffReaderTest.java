package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TiffReaderTest extends JavaSpatialSparkTest {

  public void testReadSmallFile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/simple.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(8, raster.getWidth());
      assertEquals(8, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0, raster.getPixel(0, 0));
      assertEquals(255, raster.getPixel(1, 0));
      assertEquals(0, raster.getPixel(7, 0));
      assertEquals(195, raster.getPixel(1, 7));
    }
  }

  public void testReadStrippedFile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/FRClouds.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xeaf2e7, raster.getPixel(0, 0));
      assertEquals(0x566733, raster.getPixel(38, 31));
      assertEquals(0x23225e, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[]{0x23, 0x22, 0x5e}, components);
    }
  }

  public void testReadStrippedFileWithTile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/FRClouds.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      assertEquals(raster.getWidth(), raster.getTileWidth());
      int tileHeight = raster.getTileHeight();
      AbstractTiffTile tile1 = raster.getTile(0);
      AbstractTiffTile tile2 = raster.getTile(31 / tileHeight);
      AbstractTiffTile tile3 = raster.getTile(62 / tileHeight);
      assertEquals(0xeaf2e7, tile1.getPixel(0, 0));
      assertEquals(0x566733, tile2.getPixel(38, 31));
      assertEquals(0x23225e, tile3.getPixel(76, 62));
      int[] components = new int[tile3.getNumSamples()];
      tile3.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[]{0x23, 0x22, 0x5e}, components);
    }
  }

  public void testReadJPEGYCbCrCompression() throws IOException {
    Path inputFile = new Path(locateResource("/tif/FRClouds_jpg.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      assertEquals(raster.getWidth(), raster.getTileWidth());
      int tileHeight = raster.getTileHeight();
      AbstractTiffTile tile1 = raster.getTile(0);
      AbstractTiffTile tile2 = raster.getTile(31 / tileHeight);
      AbstractTiffTile tile3 = raster.getTile(62 / tileHeight);
      assertEquals(0xe7efc8, tile1.getPixel(0, 0));
      assertEquals(0x5d6b38, tile2.getPixel(38, 31));
      assertEquals(0x27265f, tile3.getPixel(76, 62));
      int[] components = new int[tile3.getNumSamples()];
      tile3.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[]{0x27, 0x26, 0x5f}, components);
    }
  }

  public void testReadDeflateCompression() throws IOException {
    Path inputFile = new Path(locateResource("/tif/FRClouds_Deflate.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xeaf2e7, raster.getPixel(0, 0));
      assertEquals(0x566733, raster.getPixel(38, 31));
      assertEquals(0x23225e, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[]{0x23, 0x22, 0x5e}, components);
    }
  }

  public void testReadJPEGCompression() throws IOException {
    Path inputFile = new Path(locateResource("/tif/simple_jpg.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(8, raster.getWidth());
      assertEquals(8, raster.getHeight());
      ITiffTile tile = raster.getTile(0);
      assertEquals(0, tile.getPixel(0, 0));
      assertEquals(255, tile.getPixel(1, 0));
      assertEquals(4, tile.getPixel(7, 0));
      assertEquals(190, tile.getPixel(1, 7));
    }
  }

  public void testRead16BitsInterleaved() throws IOException {
    Path inputFile = new Path(locateResource("/tif/FRClouds_16bits.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(99, raster.getWidth());
      assertEquals(72, raster.getHeight());
      raster.readTileData(0);
      assertEquals(0xea00f200e7L, raster.getPixel(0, 0));
      assertEquals(0xe800ee00eaL, raster.getPixel(1, 0));
      assertEquals(0x5600670033L, raster.getPixel(38, 31));
      assertEquals(0x230022005eL, raster.getPixel(76, 62));

      int[] components = new int[raster.getNumSamples()];
      raster.getPixelSamplesAsInt(76, 62, components);
      assertArrayEquals(new int[]{0x23, 0x22, 0x5e}, components);
    }
  }

  public void testReadBigEndianFile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_bigendian.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Blue pixel (ocean)
      assertEquals(0x8ae3ff, raster.getPixel(0, 0));
      assertEquals(0x8ae3ff, raster.getPixel(1, 0));
      assertEquals(0x8ae3ff, raster.getPixel(1, 1));
      assertEquals(0x8ae3ff, raster.getPixel(54, 24));
      // Red pixel (desert)
      assertEquals(0xff0000, raster.getPixel(137, 50));
    }
  }

  public void testReadGriddedFile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_small.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Blue pixel (ocean)
      assertEquals(20, raster.getPixel(54, 24));
      // Gray pixel (desert)
      assertEquals(19, raster.getPixel(137, 58));
    }
  }

  public void testReadGriddedFileWithTile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_small.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      AbstractTiffTile tile1 = raster.getTile(raster.getTileIDAtPixel(54, 24));
      AbstractTiffTile tile2 = raster.getTile(raster.getTileIDAtPixel(137, 58));
      // Blue pixel (ocean)
      assertEquals(20, tile1.getPixel(54, 24));
      // Gray pixel (desert)
      assertEquals(19, tile2.getPixel(137, 58));
    }
  }

  public void testReadBandedFile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_banded_small.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      // Read value from the first band
      assertEquals(896032.625f, raster.getSampleValueAsFloat(200, 100, 0));
      // Read value from the second band
      assertEquals(20, raster.getSampleValueAsInt(200, 100, 1));
    }
  }

  public void testReadBandedFileWithFloatingPointPredictor() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_banded_small_diff.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      assertEquals(256, raster.getWidth());
      assertEquals(128, raster.getHeight());
      AbstractTiffTile tile = raster.getTile(0);
      assertEquals(9677.383f, tile.getSampleValueAsFloat(0, 0, 0));
      assertEquals(20f, tile.getSampleValueAsFloat(0, 0, 1));
      assertEquals(9677.383f, tile.getSampleValueAsFloat(1, 0, 0));
      assertEquals(20f, tile.getSampleValueAsFloat(1, 0, 1));
      // Read value from the first band
      assertEquals(896032.625f, raster.getSampleValueAsFloat(200, 100, 0));
      // Read value from the second band
      assertEquals(20, raster.getSampleValueAsInt(200, 100, 1));
    }
  }

  public void testReadPackBits() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_small_packbits.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      // Blue pixel (ocean)
      assertEquals(20, raster.getPixel(54, 24));
      // Gray pixel (desert)
      assertEquals(19, raster.getPixel(137, 58));
    }
  }

  public void testReadPlanar() throws IOException {
    Path inputFile = new Path(locateResource("/tif/planar_file.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      // Blue pixel (ocean)
      int[] samples = new int[4];
      raster.getPixelSamplesAsInt(0, 0, samples);
      assertArrayEquals(new int[] {852, 1085, 1211, 2182}, samples);
      raster.getPixelSamplesAsInt(2, 1, samples);
      assertArrayEquals(new int[] {912, 1125, 1250, 1881}, samples);
    }
  }

  public void testReadPlanarWithDiffPredictor() throws IOException {
    Path inputFile = new Path(locateResource("/tif/planar_diff.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      // Blue pixel (ocean)
      int[] samples = new int[4];
      raster.getPixelSamplesAsInt(0, 0, samples);
      assertArrayEquals(new int[] {852, 1085, 1211, 2182}, samples);
      raster.getPixelSamplesAsInt(2, 1, samples);
      assertArrayEquals(new int[] {912, 1125, 1250, 1881}, samples);
    }
  }
  public void testReadPlanarWithDiffPredictorMultiTile() throws IOException {
    Path inputFile = new Path(locateResource("/tif/planar_diff_multitile.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      // Blue pixel (ocean)
      int[] samples = new int[4];
      raster.getPixelSamplesAsInt(0, 0, samples);
      assertArrayEquals(new int[] {852, 1085, 1211, 2182}, samples);
      raster.getPixelSamplesAsInt(2, 1, samples);
      assertArrayEquals(new int[] {912, 1125, 1250, 1881}, samples);
    }
  }

  public void testRead64BitDoubleFloatValues() throws IOException {
    Path inputFile = new Path(locateResource("/tif/NLDAS-64.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    try (TiffReader reader = new TiffReader()) {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertNotNull(raster);
      // Read first pixel
      double[] samples =  new double[raster.getNumSamples()];
      raster.getPixelSamplesAsDouble(0, 0, samples);
      assertEquals(-0.96, samples[0], 1E-3);
      assertEquals(0.0032166, samples[1], 1E-3);
      assertEquals(90021.44, samples[2], 1E-3);
      raster.getPixelSamplesAsDouble(2, 1, samples);
      assertEquals(-2.83, samples[0], 1E-3);
      assertEquals(0.002883, samples[1], 1E-3);
      assertEquals(87502.4, samples[2], 1E-3);
    }
  }
}