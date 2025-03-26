package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.synopses.Prefix2DHistogram;
import edu.ucr.cs.bdlab.beast.synopses.UniformHistogram;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultilevelPyramidPlotHelperTest extends JavaSparkTest {

  @Override
  public void setUp() {
    super.setUp();
    try {
      // Ensures that the FileSystem does not hold a cached configuration with extra entries
      new Path(".").getFileSystem(sparkContext().hadoopConfiguration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void testClassifyTile() {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    UniformHistogram histogram = new UniformHistogram(inputMBR, 4, 4);
    histogram.addPoint(new double[] {0.5, 0.5}, 100);
    histogram.addPoint(new double[] {3.5, 2.5}, 100);
    Prefix2DHistogram h = new Prefix2DHistogram(histogram);
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ShallowTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 2, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.DataTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 1, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ImageTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 0, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.EmptyTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 2, 1, 0));
  }

  public void testGetTileSizeWithBuffer() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1024.0, 1024.0);
    UniformHistogram h = new UniformHistogram(mbr, 1024, 1024);
    h.addEntry(new int[] {252, 252}, 65536);
    // Set buffer size to approximately five pixels assuming a 256x256 tile
    double buffer = 4 / 256.0;
    assertEquals(65536, MultilevelPyramidPlotHelper.getTileSize(h, buffer, 2, 0, 0));
    assertEquals(65536, MultilevelPyramidPlotHelper.getTileSize(h, buffer, 2, 1, 1));
    assertEquals(1024, MultilevelPyramidPlotHelper.getTileSize(h, buffer, 10, 251, 252));
    // The current calculation is not accurate enough to handle the following case
    //assertEquals(16, MultilevelPyramidPlotHelper.getTileSize(h, buffer, 10, 251, 251));
  }

  public void testClassifyTileWithPartialHistogram() {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    UniformHistogram histogram = new UniformHistogram(inputMBR, 2, 2);
    histogram.addPoint(new double[] {0.5, 0.5}, 100);
    histogram.addPoint(new double[] {3.5, 2.5}, 100);
    Prefix2DHistogram h = new Prefix2DHistogram(histogram);
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ShallowTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 2, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ShallowTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 2, 1, 1));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.DataTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 1, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ImageTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 0, 0, 0));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ShallowTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 2, 2, 3));

    histogram = new UniformHistogram(inputMBR, 4096, 4096);
    histogram.addEntry(new int[] {0, 0}, 100);
    h = new Prefix2DHistogram(histogram);
    assertEquals(MultilevelPyramidPlotHelper.TileClass.EmptyTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 14, 4201, 6086));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.ShallowTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 14, 3, 3));
    assertEquals(MultilevelPyramidPlotHelper.TileClass.EmptyTile, MultilevelPyramidPlotHelper.classifyTile(h, 150, 0.0, 14, 4, 4));
  }

  public void testCreateImageTiles() {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    UniformHistogram histogram = new UniformHistogram(inputMBR, 4, 4);
    histogram.addPoint(new double[] {0.5, 0.5}, 100);
    histogram.addPoint(new double[] {3.5, 2.5}, 100);
    List<IFeature> features = new ArrayList<>();
    features.add(Feature.create(null, new PointND(new GeometryFactory(), 2, 0.5, 0.5)));
    features.add(Feature.create(null, new PointND(new GeometryFactory(), 2, 3.5, 2.5)));
    SubPyramid subPyramid = new SubPyramid(inputMBR, 0, 2, 0, 0, 4, 4);
    PyramidPartitioner fullPyramid = new PyramidPartitioner(subPyramid);
    GeometricPlotter plotter = new GeometricPlotter();
    BeastOptions opts = new BeastOptions();
    plotter.setup(opts);
    Map<Long, Canvas> tiles = new HashMap<>();
    MultilevelPyramidPlotHelper.createTiles(features, inputMBR, fullPyramid, 256, 256,
        plotter, tiles);
    assertEquals(5, tiles.size());
  }

  public void testSkipEmptyTiles() throws ParseException {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    List<IFeature> features = new ArrayList<>();
    WKTReader reader = new WKTReader();
    features.add(Feature.create(null, new EnvelopeND(new GeometryFactory(), 2, 0.5, 0.5, 3.5, 3.5)));
    features.add(Feature.create(null, (reader.read("POLYGON((0.1 0.1, 7.8 0.2, 10.0 10.0, -5.0 7.0, 0.1 0.1))"))));
    SubPyramid subPyramid = new SubPyramid(inputMBR, 0, 2);
    PyramidPartitioner fullPyramid = new PyramidPartitioner(subPyramid);
    GeometricPlotter plotter = new GeometricPlotter();
    BeastOptions opts = new BeastOptions();
    plotter.setup(opts);
    Map<Long, Canvas> tiles = new HashMap<>();
    MultilevelPyramidPlotHelper.createTiles(features, inputMBR, fullPyramid, 256, 256,
        plotter, tiles);
    assertEquals(12 + 4 + 1, tiles.size());
  }

  public void testCreateImageDataTiles() {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    UniformHistogram histogram = new UniformHistogram(inputMBR, 4, 4);
    histogram.addPoint(new double[] {0.5, 0.5}, 100);
    histogram.addPoint(new double[] {3.5, 2.5}, 100);
    List<IFeature> features = new ArrayList<>();
    features.add(Feature.create(null, new PointND(new GeometryFactory(), 2, 0.5, 0.5)));
    features.add(Feature.create(null, new PointND(new GeometryFactory(), 2, 3.5, 2.5)));
    SubPyramid subPyramid = new SubPyramid(inputMBR, 0, 2, 0, 0, 4, 4);
    PyramidPartitioner imageTilePartitioner = new PyramidPartitioner(subPyramid, histogram, 150, MultilevelPyramidPlotHelper.TileClass.ImageTile);
    GeometricPlotter plotter = new GeometricPlotter();
    BeastOptions opts = new BeastOptions();
    plotter.setup(opts);
    Map<Long, Canvas> imageTiles = new HashMap<>();
    MultilevelPyramidPlotHelper.createTiles(features, inputMBR, imageTilePartitioner, 256, 256,
        plotter, imageTiles);
    assertEquals(1, imageTiles.size());
  }

  public void testPlotTile() throws IOException {
    Path pyramidPath = new Path(locateResource("/pyramid").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root image tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(0, 0, 0), baos));
    baos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    assertEquals(Color.RED.getRGB(), img.getRGB(200, 200));

    // Test an empty tile
    baos = new ByteArrayOutputStream();
    assertFalse(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 0, 0), baos));
    baos.close();

    // Test another image tile
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 0, 1), baos));
    baos.close();
    img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    assertEquals(new Color(102, 102, 102).getRGB(), img.getRGB(200, 200));

    // Test a data tile
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos));
    baos.close();
    img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    // A transparent pixel
    assertEquals(0, new Color(img.getRGB(128, 128), true).getAlpha());
    // A non-transparent pixel
    assertEquals(255, new Color(img.getRGB(64, 64), true).getAlpha());
    // Filter MBR should not be stored in the file system configuration
    assertNull(fs.getConf().get("mbr"));

    // Test a shallow tile
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 2, 0), baos));
    baos.close();
    img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    // A non-transparent pixel
    assertEquals(255, new Color(img.getRGB(128, 128), true).getAlpha());
    // A transparent pixel
    assertEquals(0, new Color(img.getRGB(255, 255), true).getAlpha());
  }

  public void testPlotTilePointSize() throws IOException, InterruptedException {
    // Prepare a sample data path
    Path dataPath = new Path(scratchPath(), "input.txt");
    Configuration conf = new Configuration();
    FileSystem fs = dataPath.getFileSystem(conf);
    try (PrintStream datafile = new PrintStream(fs.create(dataPath))) {
      datafile.println("0,0");
      datafile.println("2,2");
      datafile.println("4,4");
    }

    // Prepare an empty pyramid path that points to the data path
    Path pyramidPath = new Path(scratchPath(), "pyramid");
    fs.mkdirs(pyramidPath);
    BeastOptions pyramidOpts = new BeastOptions()
    .set("iformat", "point")
        .set("separator", ",")
        .set("pointsize", 7)
        .set("data", "../"+dataPath.getName())
        .set("plotter", "gplot")
        .set("mbr", "0,0,4,4")
        .set("vflip", false);
    pyramidOpts.storeToTextFile(fs, new Path(pyramidPath, "_visualization.properties"));

    // Plot the four middle tiles in level 2 and ensure that they are not empty
    assertTrue("Tile (2,1,1) must not be empty", plotTileAndCountPixels(fs, pyramidPath, TileIndex.encode(2, 1, 1)) > 0);
    assertTrue("Tile (2,1,2) must not be empty", plotTileAndCountPixels(fs, pyramidPath, TileIndex.encode(2, 1, 2)) > 0);
    assertTrue("Tile (2,2,1) must not be empty", plotTileAndCountPixels(fs, pyramidPath, TileIndex.encode(2, 2, 1)) > 0);
    assertTrue("Tile (2,2,2) must not be empty", plotTileAndCountPixels(fs, pyramidPath, TileIndex.encode(2, 2, 2)) > 0);
    assertTrue("Tile (10,1,1) must be empty", plotTileAndCountPixels(fs, pyramidPath, TileIndex.encode(10, 1, 1)) == 0);
  }

  public static int plotTileAndCountPixels(FileSystem fs, Path pyramidPath, long tileIndex) throws IOException, InterruptedException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, tileIndex, baos);
    baos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    int numNonEmptyPixels = 0;
    for (int x = 0; x < img.getWidth(); x++)
      for (int y = 0; y < img.getHeight(); y++)
        if (new Color(img.getRGB(x, y), true).getAlpha() > 0)
          numNonEmptyPixels++;
    return numNonEmptyPixels;
  }

  public void testPlotTileGeoJSON() throws IOException, InterruptedException {
    Path pyramidPath = new Path(locateResource("/pyramid_geojson").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root image tile
    ByteArrayOutputStream baos;
    BufferedImage img;

    // Test a data tile
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos));
    baos.close();
    img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    // A transparent pixel
    assertEquals(0, new Color(img.getRGB(128, 128), true).getAlpha());
    // A non-transparent pixel
    assertEquals(255, new Color(img.getRGB(64, 64), true).getAlpha());

    // Test a shallow tile
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 2, 0), baos));
    baos.close();
    img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, img.getWidth());
    // A non-transparent pixel
    assertEquals(255, new Color(img.getRGB(128, 128), true).getAlpha());
    // A transparent pixel
    assertEquals(0, new Color(img.getRGB(255, 255), true).getAlpha());
  }

  public void testPlotTileSkipIfCached() throws IOException, InterruptedException {
    Path pyramidPath = new Path(scratchPath(), "pyramid");
    makeResourceCopy("/pyramid", new File(pyramidPath.toString()));
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Set the timestamps of files in a pre-determined way
    long baseTimestamp = System.currentTimeMillis();
    baseTimestamp -= baseTimestamp % 1000000; // Makes debugging easier
    final long UnitDelay = 10 * 1000;
    new File(pyramidPath.toString(), "_visualization.properties").setLastModified(baseTimestamp + UnitDelay);
    new File(pyramidPath.toString(), "tile-0-0-0.png").setLastModified(baseTimestamp + UnitDelay * 2);
    new File(pyramidPath.toString(), "tile-1-0-1.png").setLastModified(baseTimestamp + UnitDelay * 3);
    new File(pyramidPath.toString(), "tile-1-1-1.png").setLastModified(baseTimestamp + UnitDelay * 4);
    new File(pyramidPath.toString(), "tile-1-1-0.csv").setLastModified(baseTimestamp + UnitDelay * 5);
    // Test the root image tile with a non-cached file
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    LongWritable tileTimestamp = new LongWritable(baseTimestamp);
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(0, 0, 0), baos, tileTimestamp, null));
    assertEquals(UnitDelay * 2, tileTimestamp.get() - baseTimestamp);
    baos.close();
    // Requesting it again should retrieve the cached image
    baos = new ByteArrayOutputStream();
    assertFalse(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(0, 0, 0), baos, tileTimestamp, null));
    baos.close();

    // Test a data tile with a non-cached version
    baos = new ByteArrayOutputStream();
    tileTimestamp = new LongWritable(0);
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos, tileTimestamp, null));
    assertEquals(UnitDelay * 5, tileTimestamp.get() - baseTimestamp);
    baos.close();

    // Requesting it again should not retrieve a new image
    baos = new ByteArrayOutputStream();
    assertFalse(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos, tileTimestamp, null));
    baos.close();

    // Updating the metadata file should trigger a refresh
    new File(pyramidPath.toString(), "_visualization.properties").setLastModified(baseTimestamp + 10 * UnitDelay);
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos, tileTimestamp, null));
    baos.close();
  }

  /**
   * Test plotting a tile when the index contains image indexes and a link to a data index (R-tree index)
   * @throws IOException
   */
  public void testPlotTileRTree() throws IOException {
    Path pyramidPath = new Path(locateResource("/test_partial_pyramid").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    assertNull(fs.getConf().get("mbr"));
    // Test the root image tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 0, 2), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // Filter MBR should not be stored in the file system configuration
    assertNull(fs.getConf().get("mbr"));

    byte[] data = readResourceData("/test_full_pyramid/tile-2-0-2.png");
    BufferedImage expectedImage = ImageIO.read(new ByteArrayInputStream(data));
    assertImageEquals(expectedImage, generatedImage);
  }

  public void testPlotTileRTreeShouldSkipCachedTiles() throws IOException {
    Path pyramidPath = new Path(scratchPath(), "test_partial_pyramid");
    makeResourceCopy("/test_partial_pyramid", new File(pyramidPath.toString()));
    Path rtreePath = new Path(scratchPath(), "test_rtree");
    makeResourceCopy("/test_rtree", new File(rtreePath.toString()));
    // Initialize the modification timestamp of all files
    long baseTimestamp = System.currentTimeMillis();
    baseTimestamp -= baseTimestamp % 1000000; // Makes debugging easier
    for (File f : new File(pyramidPath.toString()).listFiles())
      f.setLastModified(baseTimestamp);
    for (File f : new File(rtreePath.toString()).listFiles())
      f.setLastModified(baseTimestamp);

    LongWritable timestamp = new LongWritable();

    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root image tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    timestamp.set(0);
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 0, 2), baos, timestamp, null));
    baos.close();
    assertEquals(baseTimestamp, timestamp.get());
    // Requesting a data tile again should not regenerate it
    baos = new ByteArrayOutputStream();
    assertFalse(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 0, 2), baos, timestamp, null));
    baos.close();
  }

  /**
   * Test plotting a tile when the index contains image indexes and a link to a data index (R-tree index)
   * when Mercator projection is in use and the MBR is not present.
   * @throws IOException
   * @throws URISyntaxException
   * @throws InterruptedException
   */
  public void testPlotTileMercator() throws IOException, InterruptedException {
    Path pyramidPath = new Path(locateResource("/test_partial_pyramid").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root image tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 0, 2), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(256, generatedImage.getWidth());
  }

  /**
   * Test plotting a non-root tile on-the-fly when the Web Mercator projection is desired and the input is a partial
   * image index with associated data index (R-tree)
   * @throws IOException
   * @throws InterruptedException
   */
  public void testPlotTileMercatorRTree() throws IOException {
    Path pyramidPath = new Path(locateResource("/rect_mercator_shallow_plot").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test a tile at level 1
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 0, 0), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    InputStream expectedImageData = this.getClass().getResourceAsStream("/rect_mercator_plot/tile-1-0-0.png");
    BufferedImage expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 2
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 1, 1), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/rect_mercator_plot/tile-2-1-1.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
  }


  /**
   * Test plotting a non-root tile on-the-fly when the Web Mercator projection is desired and the
   * index is hybrid (contains both image tiles and data tiles)
   * @throws IOException
   * @throws InterruptedException
   */
  public void testPlotTileMercatorHybrid() throws IOException {
    Path pyramidPath = new Path(locateResource("/rect_mercator_hybrid_plot").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test a tile at level 1
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 0, 0), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    InputStream expectedImageData = this.getClass().getResourceAsStream("/rect_mercator_plot/tile-1-0-0.png");
    BufferedImage expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 2
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 1, 1), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/rect_mercator_plot/tile-2-1-1.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
  }

  public void testComputeHistogramDimension() {
    int dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(128 * 1024 * 1024, 5, 8);
    assertEquals(32, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(32, 5, 8);
    assertEquals(2, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(127, 5, 8);
    assertEquals(2, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(128, 5, 8);
    assertEquals(4, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(32, 2, 8);
    assertEquals(2, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(512 * 1024 * 1024, 15, 8);
    assertEquals(8192, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(512 * 1024 * 1024, 15, 32);
    assertEquals(4096, dimension);

    dimension = MultilevelPyramidPlotHelper.computeHistogramDimension(512 * 1024 * 1024, 15, 32);
    assertEquals(4096, dimension);
  }

  public void testPlotTileVFlip() throws IOException {
    Path pyramidPath = new Path(locateResource("/test_mercator_vflip_shallow").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(0, 0, 0), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    InputStream expectedImageData = this.getClass().getResourceAsStream("/test_mercator_vflip/tile-0-0-0.png");
    BufferedImage expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 1
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 0, 0), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/test_mercator_vflip/tile-1-0-0.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 2
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 3, 1), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/test_mercator_vflip/tile-2-3-1.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
  }

  public void testPlotDataTileVFlip() throws IOException {
    Path pyramidPath = new Path(locateResource("/test2_vflip_shallow").getPath());
    FileSystem fs = pyramidPath.getFileSystem(new Configuration());
    // Test the root tile
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(0, 0, 0), baos));
    baos.close();
    BufferedImage generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    InputStream expectedImageData = this.getClass().getResourceAsStream("/test2_vflip_full/tile-0-0-0.png");
    BufferedImage expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 1
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(1, 1, 0), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/test2_vflip_full/tile-1-1-0.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
    // Test a tile at level 2
    baos = new ByteArrayOutputStream();
    assertTrue(MultilevelPyramidPlotHelper.plotTile(fs, pyramidPath, TileIndex.encode(2, 2, 1), baos));
    baos.close();
    generatedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    expectedImageData = this.getClass().getResourceAsStream("/test2_vflip_full/tile-2-2-1.png");
    expectedImage = ImageIO.read(expectedImageData);
    assertImageEquals(expectedImage, generatedImage);
  }

  public void testFindDeepestTile() {
    EnvelopeNDLite inputMBR = new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0);
    UniformHistogram histogram = new UniformHistogram(inputMBR, 4, 4);
    histogram.addEntry(new int[] {0, 0}, 100);
    histogram.addEntry(new int[] {3, 2}, 100);
    histogram.addEntry(new int[] {1, 0}, 100);
    Prefix2DHistogram h = new Prefix2DHistogram(histogram);
    assertEquals(0, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 300));
    assertEquals(1, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 200));
    assertEquals(2, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 100));
    assertEquals(2, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 50));
    assertEquals(3, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 25));
    assertEquals(5, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 1));


    histogram = new UniformHistogram(inputMBR, 4, 4);
    histogram.addEntry(new int[] {0, 0}, 100);
    histogram.addEntry(new int[] {3, 2}, 8192);
    histogram.addEntry(new int[] {1, 0}, 100);
    h = new Prefix2DHistogram(histogram);
    assertEquals(8, MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, 1));
  }
}
