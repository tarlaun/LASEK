/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.cg.Reprojector;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVEnvelopeEncoder;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.util.MathUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.beast.CRSServer;
import org.locationtech.jts.geom.Envelope;
import scala.Option;

import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class that helps with pyramid multilevel plotting.
 */
public class MultilevelPyramidPlotHelper {
  private static final Log LOG = LogFactory.getLog(MultilevelPyramidPlotHelper.class);

  /**
   * Returns the range of zoom levels which is encoded as either numLevels or minLevel..maxLevel.
   * If the string value is a single number (n), the returned range is 0..n-1, if the range is value is two numbers
   * separated by two dots, e.g., a..b, the two values are returned
   * @param conf the configuration to extract the range from
   * @param key the name of the key to retrieve from the configuration
   * @param defaultValue the default value
   * @return the parsed range or the parsed default value
   */
  public static Range getRange(Configuration conf, String key, String defaultValue) {
    String value = conf.get(key, defaultValue);
    int i = value.indexOf("..");
    if (i == -1)
      return new Range(0, Integer.parseInt(value) - 1);
    int min = Integer.parseInt(value.substring(0, i));
    int max = Integer.parseInt(value.substring(i + 2));
    return new Range(min, max);
  }

  /**
   * Compute the side length of the histogram used for adaptive multilevel plot. The total size of the histogram in
   * bytes cannot be larger than the given {@code maxHistogramSize}. At the same time, it does not have to be any larger
   * than 2<sup>{@code maxLevel}</sup>
   * @param maxHistogramSize the maximum size of the histogram to compute in bytes
   * @param maxLevel the maximum (deepest) level that needs to be considered
   * @param cellSize the size of each cell in the histogram in bytes
   * @return the dimension of the histogram to compute, i.e., number of rows = number of columns
   */
  public static int computeHistogramDimension(long maxHistogramSize, int maxLevel, int cellSize) {
    long idealSize = (1L << (2 * maxLevel)) * cellSize;
    if (idealSize <= maxHistogramSize) {
      // Can compute a histogram of the ideal size
      return 1 << maxLevel;
    }
    // Has to abide by the maximum histogram size in bytes.
    // We still need to return a value that is a power of two
    int maxNumberOfBins = (int) (maxHistogramSize / cellSize);
    int maxDimension = (int) Math.sqrt(maxNumberOfBins);
    return Integer.highestOneBit(maxDimension);
  }

  /**
   * Finds the level of the deepest tile that is at least of the given size.
   * @param h the histogram that represents the data
   * @param sizeThreshold the size threshold to find
   * @return the deepest (maximum) level of the tile with the at least the given size according to the histogram.
   * If the threshold is larger than the entire histogram, a zero is returned.
   */
  public static int findDeepestTileWithSize(AbstractHistogram h, long sizeThreshold) {
    int level = 0;
    boolean tileFound;
    int[] searchPos = new int[2];
    int[] searchSize = new int[2];
    assert h.getNumPartitions(0) == h.getNumPartitions(1) :
        String.format("Histogram dimension %d x %d is not square", h.getNumPartitions(0), h.getNumPartitions(1));
    assert Integer.highestOneBit(h.getNumPartitions(0)) == h.getNumPartitions(0) :
        String.format("Histogram size %d is not power of two", h.getNumPartitions(0));
    int histogramSize = h.getNumPartitions(0);
    int maxLevel = MathUtil.log2(histogramSize);
    // Keep track of the first tile that goes beyond the threshold to save time in the next round
    int[] firstTileAboveThreshold = new int[2];
    // Keep track of the maximum tile size found
    do {
      // Try all the tiles at this level
      tileFound = false;
      int dimension = 1 << level;
      searchSize[0] = searchSize[1] = histogramSize / dimension;
      for (searchPos[0] = firstTileAboveThreshold[0]; searchPos[0] < histogramSize && !tileFound; searchPos[0] += searchSize[0])
        for (searchPos[1] = firstTileAboveThreshold[1]; searchPos[1] < histogramSize && !tileFound; searchPos[1] += searchSize[1]) {
          tileFound = h.getValue(searchPos, searchSize) >= sizeThreshold;
          if (tileFound) {
            firstTileAboveThreshold[0] = searchPos[0];
            firstTileAboveThreshold[1] = searchPos[1];
          }
        }

      // If the tile was found, try the deeper level
      if (tileFound) {
        level++;
      }
    } while (level < maxLevel && tileFound);
    // If the loop terminates when the tile is not found, then the maximum level is the one just before it
    if (!tileFound)
      return Math.max(0, level - 1);

    // If the tile is not found, we search for the tile at the last level and also locate the tile with max size
    long maxTileSize = 0;
    searchSize[0] = searchSize[1] = 1;
    tileFound = false;
    // Since we are searching for the maximum, we do not stop when we find the first tile
    for (searchPos[0] = firstTileAboveThreshold[0]; searchPos[0] < histogramSize; searchPos[0]++)
      for (searchPos[1] = firstTileAboveThreshold[1]; searchPos[1] < histogramSize; searchPos[1]++)
        tileFound = (maxTileSize = Math.max(maxTileSize, h.getValue(searchPos, searchSize))) >= sizeThreshold;

    // We couldn't find the tile at the deepest level, it  must have been at the previous level
    if (!tileFound)
      return level - 1;

    // If the tile is found, then we need to dig deeper to see if it also exists at deeper levels
    // We need to go deeper delta_d levels such that maxTileSize / 4^(delta_d) <= sizeThreshold
    // delta_d <= log(maxTileSize / sizeThreshold) / log(4)
    // But delta_d is integer.
    // delta_d = Math.floor(log(maxTileSize / sizeThreshold) / log(4))
    int delta_d = MathUtil.log2((int) (maxTileSize / sizeThreshold)) / 2;
    level += delta_d;
    return level;
  }

  /**
   * A class that represents a range (min..max) inclusive of both. Used to store a range of zoom levels.
   */
  public static class Range implements Externalizable {
    public int min, max;

    public Range() {}

    public Range(int min, int max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public String toString() {
      return min == 0? Integer.toString(max + 1) : String.format("%d..%d", min, max);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(min);
      out.writeInt(max);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
      this.min = in.readInt();
      this.max = in.readInt();
    }
  }


  /** The classes of tiles according to our design */
  public enum TileClass {ImageTile, DataTile, ShallowTile, EmptyTile}

  /**
   * Computes the size of the given tile in the histogram.
   * @param h the histogram that summarizes the input data
   * @param z the zoom level of the tile
   * @param x the x-coordinate of the tile at level z
   * @param y the y-coordinate of the tile at level z
   * @return the size of the tile according to the given histogram
   */
  public static long getTileSize(AbstractHistogram h, double buffer, int z, int x, int y) {
    assert h.getCoordinateDimension() == 2;
    assert x >= 0 && x < (1 << z) : String.format("Illegal x coordinate for tile (%d,%d,%d)", z, x, y);
    assert y >= 0 && y < (1 << z) : String.format("Illegal y coordinate for tile (%d,%d,%d)", z, x, y);
    assert h.getNumPartitions(0) == h.getNumPartitions(1);
    int gridDimensionAtLevelZ = 1 << z;
    int histogramDimension = h.getNumPartitions(0);
    int numHistogramCellsPerTile;
    double fractionOfGridCellCoveredByTile;
    int col1, row1;
    if (gridDimensionAtLevelZ <= histogramDimension) {
      // The given tile (at level Z) covers multiple cells in the histogram
      numHistogramCellsPerTile = histogramDimension / gridDimensionAtLevelZ;
      fractionOfGridCellCoveredByTile = 1.0;
      col1 = x * numHistogramCellsPerTile;
      row1 = y * numHistogramCellsPerTile;
    } else { // gridDimensionAtLevelZ > histogramDimension
      // The given tile (at level Z) covers only a partial cell in the histogram
      numHistogramCellsPerTile = 1;
      fractionOfGridCellCoveredByTile = ((double) histogramDimension) / gridDimensionAtLevelZ;
      col1 = (x / (gridDimensionAtLevelZ / histogramDimension)) * numHistogramCellsPerTile;
      row1 = (y / (gridDimensionAtLevelZ / histogramDimension)) * numHistogramCellsPerTile;
    }
    long size = (long) (fractionOfGridCellCoveredByTile * fractionOfGridCellCoveredByTile *
        h.getValue(new int[] {col1, row1}, new int[] {numHistogramCellsPerTile, numHistogramCellsPerTile}));
    if (buffer > 0) {
      // Expand to include the buffer
      int bufferInt = (int) Math.ceil(numHistogramCellsPerTile * buffer);
      double fraction = bufferInt / (numHistogramCellsPerTile * buffer);
      int col2 = Math.min(histogramDimension, col1 + numHistogramCellsPerTile + bufferInt);
      int row2 = Math.min(histogramDimension, row1 + numHistogramCellsPerTile + bufferInt);
      col1 = Math.max(0, col1 - bufferInt);
      row1 = Math.max(0, row1 - bufferInt);
      long expandedSize = (long) (fractionOfGridCellCoveredByTile * fractionOfGridCellCoveredByTile *
          h.getValue(new int[] {col1, row1}, new int[] {col2 - col1, row2 - row1}));
      // This calculation takes the entire bins that partially overlaps with the buffer
      // For more accurate calculations, include only a value proportional to the buffer
      long diff = expandedSize - size;
      size += diff / fraction;
    }
    return size;
  }

  /**
   * Classifies a tile according to the file histogram into one of the four
   * classes: ImageTile, DataTile, EmptyTile, and ShallowTile
   * @param h A precomputed histogram
   * @param threshold The size threshold of image and data tiles
   * @param buffer The size of the buffer as a percentage of the tile size.
   * @param z The level of the tile
   * @param x The column index of the tile in the level z
   * @param y The row index of the tile in the level z
   * @return the class of the tile
   */
  public static TileClass classifyTile(AbstractHistogram h, long threshold, double buffer, int z, int x, int y) {
    long tileSize = getTileSize(h, buffer, z, x, y);
    // If the size is larger than the threshold then it has to be an image tile
    if (tileSize > threshold)
      return TileClass.ImageTile;
    // The code below is incorrect because the histogram is not 100% accurate
    if (tileSize == 0)
      return TileClass.EmptyTile;

    // It could be either a data tile or shallow/empty tile based on its parent size
    if (z == 0)
      return TileClass.DataTile; // No parent

    // Compute the size of the parent
    z--;
    x /= 2;
    y /= 2;
    long parentSize = getTileSize(h, buffer, z, x, y);
    if (parentSize > threshold)
      return TileClass.DataTile;

    return TileClass.ShallowTile;
  }

  static class CanvasModified {
    Canvas canvas;
    boolean modified;

    CanvasModified(Canvas c) {
      this.canvas = c;
      this.modified = false;
    }
  }

  /**
   * Creates and returns all the tiles in the given sub pyramid that contains
   * the given set of shapes.
   * @param features The features to be plotted
   * @param featuresMBR the minimum bounding rectangle of the features
   * @param partitioner the pyramid partitioner that defines the tiles to generate
   * @param tileWidth Width of each tile in pixels
   * @param tileHeight Height of each tile in pixels
   * @param plotter The plotter used to create canvases
   * @param tiles (output) The set of tiles that have been created already. It could be
   *              empty which indicates no tiles created yet.
   */
  public static void createTiles(
      Iterable<? extends IFeature> features, EnvelopeNDLite featuresMBR, PyramidPartitioner partitioner,
      int tileWidth, int tileHeight, Plotter plotter, Map<Long, Canvas> tiles) {

    EnvelopeNDLite shapeMBR = null;
    Envelope tileMBR = new Envelope();

    TileIndex tileIndex = new TileIndex();

    Map<Long, CanvasModified> tempTiles = new HashMap<>();

    for (IFeature feature : features) {
      if (shapeMBR == null) {
        shapeMBR = new EnvelopeNDLite(GeometryHelper.getCoordinateDimension(feature.getGeometry()));
      } else {
        shapeMBR.setEmpty();
      }
      shapeMBR.merge(feature.getGeometry());

      long[] overlappingTiles = partitioner.overlapPartitions(shapeMBR);
      for (long tileID : overlappingTiles) {
        CanvasModified c = tempTiles.get(tileID);
        if (c == null) {
          // First time to encounter this tile, create the corresponding canvas
          TileIndex.decode(tileID, tileIndex);
          TileIndex.getMBR(featuresMBR, tileIndex.z, tileIndex.x, tileIndex.y, tileMBR);
          c = new CanvasModified(plotter.createCanvas(tileWidth, tileHeight, tileMBR, tileID));
          tempTiles.put(tileID, c);
        }
        c.modified = plotter.plot(c.canvas, feature) || c.modified ;
      }
    }
    for (Map.Entry<Long, CanvasModified> tempEntry : tempTiles.entrySet())
      if (tempEntry.getValue().modified)
        tiles.put(tempEntry.getKey(), tempEntry.getValue().canvas);
  }

  public static String getTileFileName(int z, int x, int y) {
    return String.format("tile-%d-%d-%d", z, x, y);
  }

  /**
   * Plot the tile without checking its modification time.
   * @param fs the file system that contains the tile
   * @param vizPath the path of the visualization index
   * @param tileID the ID of the tile to plot
   * @param out the output stream to write the generated imge to
   * @return {@code true} if the tile was not empty
   * @throws IOException if an error happens while reading the input
   */
  public static boolean plotTile(FileSystem fs, Path vizPath, long tileID, OutputStream out) throws IOException {
    return plotTile(fs, vizPath, tileID, out, new LongWritable(0), null);
  }

  /**
   * Runs the visualization query and returns the image of a single tile.
   * For efficiency, this function can skip creating the image based on the given timestamp of a previously cached
   * image as follow.
   * <ol>
   *   <li>If the modification time of the metadata file is older than the given timestamp, skip.</li>
   *   <li>If the modification time of the data tile is older than the given timestamp, skip.</li>
   *   <li>If no data tiles are present and the modification time of the master file of the index is older than
   *   the given timestamp, skip</li>
   * </ol>
   * @param fs the file system that contains the data
   * @param vizPath the path of the visualization index
   * @param tileID the ID of the tile to plot
   * @param out the output to write the tile data to
   * @param clientTimestamp (inout) as input, it holds the timestamp of a cached version on the client or zero if the client
   *                  did not mention a cached version. As output, it will hold the latest timestamp of a file used
   *                  to generate the image.
   * @return whether the tile was non-empty, {@code true} if the produced image is not empty
   * @throws IOException if an error happens while reading the input
   */
  public static boolean plotTile(FileSystem fs, Path vizPath, long tileID, OutputStream out,
                                 LongWritable clientTimestamp, BeastOptions opts)
      throws IOException {
    TileIndex t = new TileIndex();
    TileIndex.decode(tileID, t);
    boolean movedUp = false;
    Path vizMetadataPath = new Path(vizPath, "_visualization.properties");
    if (opts == null)
      opts = new BeastOptions(fs.getConf());
    if (fs.exists(vizMetadataPath))
      opts = opts.mergeWith(new BeastOptions().loadFromTextFile(fs, vizMetadataPath));

    String imageExtension = Plotter.getImageExtension(opts.getString(CommonVisualizationHelper.PlotterName));
    String dataExtension = opts.getString(SpatialFileRDD.InputFormat()) == null?
        null : FeatureReader.getFileExtension(opts.getString(SpatialFileRDD.InputFormat()));
    do {
      String filename = getTileFileName(t.z, t.x, t.y);
      Path imgFilePath = new Path(vizPath, filename + imageExtension);
      if (fs.exists(imgFilePath)) {
        if (movedUp)
          break;
        long localTimestamp = fs.getFileStatus(imgFilePath).getModificationTime();
        if (localTimestamp <= clientTimestamp.get()) {
          // The client-cached version is at least as new as the local file, skip file generation
          return false;
        }
        // Return the timestamp of that image
        clientTimestamp.set(localTimestamp);
        try (InputStream in = fs.open(imgFilePath)) {
          IOUtils.copyBytes(in, out, 8192);
          return true;
        }
      } else {
        // Test if there is a data tile
        Path dataFilePath = new Path(vizPath, filename + dataExtension);
        if (fs.exists(dataFilePath)) {
          long dataFileTimestamp = Math.max(fs.getFileStatus(dataFilePath).getModificationTime(),
              fs.getFileStatus(vizMetadataPath).getModificationTime());
          if (dataFileTimestamp <= clientTimestamp.get()) {
            // The client-cached version is newer than the disk version
            return false;
          }
          // Update the timestamp of the generated tile to the timestamp of this data file
          clientTimestamp.set(dataFileTimestamp);
          // Visualize the data tile on the fly
          Plotter plotter = Plotter.createAndConfigurePlotter(new BeastOptions(opts));
          DataOutputStream dos = new DataOutputStream(out);
          int tileWidth = opts.getInt(SingleLevelPlotHelper.ImageWidth, 256);
          int tileHeight = opts.getInt(SingleLevelPlotHelper.ImageHeight, 256);
          // Disable Mercator projection since data tiles are already projected
          opts.set(CommonVisualizationHelper.UseMercatorProjection, false);
          // Set the MBR based on the tile
          EnvelopeNDLite inputMBR = EnvelopeNDLite.decodeString(opts.getString("mbr"), new EnvelopeNDLite());
          Envelope tileMBR = new Envelope();
          TileIndex.decode(tileID, t);
          if (opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true))
            t.vflip();
          tileID = TileIndex.encode(t.z, t.x, t.y);
          TileIndex.getMBR(inputMBR, tileID, tileMBR);
          opts.set("mbr", CSVEnvelopeEncoder.defaultEncoderJTS.apply(tileMBR));
          SingleLevelPlotHelper.plotLocal(dataFilePath, dos, plotter.getClass(), tileWidth, tileHeight, false,
              opts.loadIntoHadoopConf(new Configuration(fs.getConf())));
          dos.flush();
          return true;
        } else {
          t.moveToParent();
          movedUp = true;
        }
      }
    } while (t.z >= 0);
    // No image or data file found. Check if there is an associated data index to generate the image on the fly
    if (opts.get("data").isEmpty())
      return false;
    Path dataPath = new Path(vizPath, opts.getString("data")); // Path is relative to the vizPath
    FileStatus[] masterFiles = fs.listStatus(dataPath, SpatialFileRDD.MasterFileFilter());
    if (masterFiles.length > 0) {
      if (masterFiles[0].getModificationTime() <= clientTimestamp.get())
        return false; // An up-to-date version is already cached by client
      clientTimestamp.set(masterFiles[0].getModificationTime());
    }
    // There is an associated data index. Use it to generate the image
    EnvelopeNDLite inputMBR;
    TileIndex.decode(tileID, t); // Restore the requested tile ID
    if (opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false)) {
      // For Mercator projection, the data MBR is fixed
      inputMBR = new EnvelopeNDLite(Reprojector.reprojectEnvelope(
          CommonVisualizationHelper.MercatorMapBoundariesEnvelope,
          FeatureReader.DefaultCRS, CommonVisualizationHelper.MercatorCRS)) ;
      // Always apply vflip
      t.vflip();
    } else {
      inputMBR = EnvelopeNDLite.decodeString(opts.getString("mbr"), new EnvelopeNDLite());
      // For non-mercator projection, adjust the tile ID to get the correct MBR if vflip was used
      if (opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true))
        t.vflip();
    }
    // Set the MBR based on the tile
    Envelope tileMBR = new Envelope();
    TileIndex.getMBR(inputMBR, t.z, t.x, t.y, tileMBR);

    // If data needs to be converted to mercator projection on-the-fly, correct the tile MBR but not the filter MBR
    // because the filter MBR is based on the input data
    if (opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false)) {
      // Web Mercator projection is in use while original data in world coordinates.
      // We need to convert the MBR and FilterMBR back to the world coordinates to match
      tileMBR = Reprojector.reprojectEnvelope(tileMBR, CommonVisualizationHelper.MercatorCRS, FeatureReader.DefaultCRS);
    }
    opts.set("mbr", CSVEnvelopeEncoder.defaultEncoderJTS.apply(tileMBR));

    Plotter plotter = Plotter.createAndConfigurePlotter(opts);
    int tileWidth = opts.getInt("width", 256);
    int tileHeight = opts.getInt("height", 256);

    DataOutputStream dos = new DataOutputStream(out);
    SingleLevelPlotHelper.plotLocal(dataPath, dos, plotter.getClass(), tileWidth, tileHeight, false,
        opts.loadIntoHadoopConf(new Configuration(fs.getConf())));
    dos.flush();
    // TODO: if the image is empty, we might want to return false
    return true;
  }

  /**
   * Write some additional files that help visualizing a multilevel visualization image.
   * @param fs the file system that contains the visulized data
   * @param vizPath the path of the visualization
   * @param opts the user options that needs to be written
   * @throws IOException if an error happens while writing the output
   */
  public static void writeVisualizationAddOnFiles(FileSystem fs, Path vizPath, BeastOptions opts) throws IOException {
    int tileWidth = opts.getInt(SingleLevelPlotHelper.ImageWidth, 256);
    int tileHeight = opts.getInt(SingleLevelPlotHelper.ImageHeight, 256);
    boolean mercator = opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false);
    Class<? extends Plotter> plotter = opts.getClass(CommonVisualizationHelper.PlotterClassName, null, Plotter.class);
    // Get the correct levels.
    String[] strLevels = opts.getString("levels", "7").split("\\.\\.");
    int minLevel, maxLevel;
    if (strLevels.length == 1) {
      minLevel = 0;
      maxLevel = Integer.parseInt(strLevels[0]) - 1;
    } else {
      minLevel = Integer.parseInt(strLevels[0]);
      maxLevel = Integer.parseInt(strLevels[1]);
    }

    // Add an HTML file that visualizes the result using OpenLayers API
    String templateFile =  "/zoom_view.html";
    LineReader templateFileReader = new LineReader(MultilevelPyramidPlotHelper.class.getResourceAsStream(templateFile));
    PrintStream htmlOut = new PrintStream(fs.create(new Path(vizPath, "index.html")));
    try {
      Text line = new Text();
      String imageExtension = Plotter.getImageExtension(plotter);
      while (templateFileReader.readLine(line) > 0) {
        String lineStr = line.toString();
        lineStr = lineStr.replace("#{MERCATOR}", Boolean.toString(mercator));
        lineStr = lineStr.replace("#{TILE_WIDTH}", Integer.toString(tileWidth));
        lineStr = lineStr.replace("#{TILE_HEIGHT}",  Integer.toString(tileHeight));
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel));
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel));
        lineStr = lineStr.replace("#{TILE_URL}", "tile-{z}-{x}-{y}"+imageExtension);

        htmlOut.println(lineStr);
      }
    } finally {
      templateFileReader.close();
      htmlOut.close();
    }

    // Store the user options
    // Whatever we used as an output format should be used as an input format

    Option<String> oformat = opts.get(SpatialWriter.OutputFormat());
    if (oformat.isDefined())
      opts.set(SpatialFileRDD.InputFormat(), oformat.get());
    opts.storeToTextFile(fs, new Path(vizPath, "_visualization.properties"));
  }

}
