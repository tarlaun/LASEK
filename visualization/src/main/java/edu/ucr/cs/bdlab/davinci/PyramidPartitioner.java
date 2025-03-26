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

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import org.locationtech.jts.geom.Envelope;

import java.awt.*;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Partitions features based on a pyramid structure. It also accepts a histogram that can limit to specific partitions
 * based on their total value in the histogram. The histogram must be a uniform histogram with a side length equal to
 * 2<sup>k</sup>, where k is an integer.
 * This partitioner has three modes of operations:
 * <ol>
 *   <li>Full: Each record is assigned to all overlapping tiles in the given subpyramid. To use this mode of operation
 *   use the {@link #PyramidPartitioner(SubPyramid)} constructor</li>
 *   <li>Tile class: Each record is assigned to overlapping tiles of the given type. To use this mode, construct
 *   the partitioner using {@link #PyramidPartitioner(SubPyramid, AbstractHistogram, long, MultilevelPyramidPlotHelper.TileClass)}</li>
 *   <li>Tile size: Each record is assigned to ovlerapping tiles that have a size in a given range [min, max).
 *   This mode is activated using the constructor {@link #PyramidPartitioner(SubPyramid, AbstractHistogram, long, long)}</li>
 * </ol>
 * This class does NOT implement the {@link SpatialPartitioner} interface because the partition ID has to be long.
 * The partition ID has to be long to support deep pyramids with more than 16 levels where the total number of tiles
 * goes beyond the limit of the 32-bit integer.
 */
public class PyramidPartitioner implements Externalizable {
  /**A pyramid that defines the tiles be considered*/
  protected SubPyramid pyramid = new SubPyramid();

  /**For efficiency, this set can be prepopulated with all tile IDs that should be considered*/
  protected Set<Long> precachedTiles;

  /**This histogram used to classify tiles for class-based and size-based partitioning modes*/
  protected AbstractHistogram histogram;

  /**The type of tiles to consider. Only used when class-based partitioning mode is activated*/
  protected MultilevelPyramidPlotHelper.TileClass tileToConsider;

  /**When class-based partitioning is activated, the threshold to use for classifying tiles*/
  protected long threshold;

  /**When size-based partitioning is activated, this indicates the minimum threshold to consider (inclusive)*/
  protected long minThreshold;

  /**When size-based partitioning is activated, this indicates the maximum threshold to consider (exclusive)*/
  protected long maxThreshold;

  /**
   * How many levels to combine together. If set to 1 (default value), then no grouping is done and the records are
   * partitioned to all levels. If set to 3, then every three levels are combined together and the partitioner assigns
   * records to only the top level of these three. The levels are counted from the pyramid base (deepest level).
   * So, if the granularity is 3 and the levels of the sub pyramid are [0, 10], then records are only assigned
   * to levels [0, 2, 5, 8].
   */
  protected int granularity = 1;

  /**
   * The buffer to add around objects when partitioning. The buffer is proportionate to the tile size so the
   * absolute value might be different from one level to another. For example, if the buffer is 0.1, then the buffer
   * amount is 10% of the tile size in each level.
   */
  protected double buffer;

  /**Default constructor for the Writable interface*/
  public PyramidPartitioner() {
  }

  /**
   * Creates a full pyramid partitioner which partitions record to all overlapping tiles in the given pyramid
   * @param pyramid the pyramid that defines the range of tiles to partition to
   */
  public PyramidPartitioner(SubPyramid pyramid) {
    this.pyramid.set(pyramid);
  }

  /**
   * Constructs a tile class partitioner that partitions to tiles of the given class.
   * @param pyramid defines the shape of the pyramid used for partitioning
   * @param histogram a uniform histogram of dimensions 2<sup>k</sup>x2<sup>k</sup>, where k is an integer
   * @param threshold if histogram is not null, this is the threshold that is used to classify tiles
   * @param tileToConsider the type of tiles to consider for this partitioner
   */
  public PyramidPartitioner(SubPyramid pyramid, AbstractHistogram histogram, long threshold,
                            MultilevelPyramidPlotHelper.TileClass tileToConsider) {
    assert histogram != null : "Histogram cannot be null";
    this.pyramid.set(pyramid);
    this.histogram = histogram;
    this.threshold=threshold;
    this.tileToConsider = tileToConsider;
    precacheConsideredTiles();
  }

  /**
   * Constructs a size partitioner that partitions to tiles that are within the given range.
   * @param pyramid defines the shape of the pyramid used for partitioning
   * @param histogram a uniform histogram of dimensions
   * @param minThreshold the minimum threshold of a tile to partition to (inclusive)
   * @param maxThreshold the maximum threshold of a tile to partition to (exclusive)
   */
  public PyramidPartitioner(SubPyramid pyramid, AbstractHistogram histogram, long minThreshold, long maxThreshold) {
    assert histogram != null : "Histogram cannot be null";
    this.pyramid.set(pyramid);
    this.histogram = histogram;
    this.minThreshold = minThreshold;
    this.maxThreshold = maxThreshold;
    precacheConsideredTiles();
  }

  /**
   * Creates another partitioner that follows the same logic of the given one but operates on a different pyramid region.
   * @param partitioner the pyramid partitioner to copy
   * @param pyramid the new pyramid region to consider for the new partitioner. Must be within the original one.
   */
  public PyramidPartitioner(PyramidPartitioner partitioner, SubPyramid pyramid) {
    this.pyramid.set(pyramid);
    this.histogram = partitioner.histogram;
    this.minThreshold = partitioner.minThreshold;
    this.maxThreshold = partitioner.maxThreshold;
    this.threshold = partitioner.threshold;
    this.buffer = partitioner.buffer;
    this.granularity = partitioner.granularity;
    // Since the passed pyramid region is contained within the original region, we can reuse the same precached list
    this.tileToConsider = partitioner.tileToConsider;
  }

  /**
   * If it seems more efficient, this function will pre-classify all tiles and cache the results. This would be
   * more efficient if there are many record to process and a few tiles covered by this partitioner.
   */
  protected void precacheConsideredTiles() {
    assert histogram != null : "No need to precache tiles if there is no histogram";
    // First, count the total number of tiles that we can check for
    long totalNumOfTiles = 0;
    for (SubPyramid sp : pyramid.getSubPyramids(this.granularity)) {
      // For each subpyramid, we only consider its root tile
      totalNumOfTiles++;
    }

    // Check if the hashtable will be smaller than the histogram assuming a load factor of 0.75
    if (totalNumOfTiles * 4 / 3 < histogram.getNumBins()) {
      Set<Long> consideredTileIDs = new HashSet<>();
      // We can reduce the memory size by removing the histogram and preclassifying and caching all tiles
      for (SubPyramid sp : pyramid.getSubPyramids(this.granularity)) {
        int rootX = sp.getC1() >> (sp.getMaximumLevel() - sp.getMinimumLevel());
        int rootY = sp.getR1() >> (sp.getMaximumLevel() - sp.getMinimumLevel());
        if (isTileConsidered(sp.getMinimumLevel(), rootX, rootY, sp.getMaximumLevel() + 1))
          consideredTileIDs.add(TileIndex.encode(sp.getMinimumLevel(), rootX, rootY));
      }
      histogram = null;
      precachedTiles = consideredTileIDs;
    }
  }

  /**
   * Sets the partition granularity; i.e., how many levels to group together.
   * @param k the new granularity to set, see {@link #granularity}
   */
  public void setGranularity(int k) {
    this.granularity = k;
  }

  /**
   * Set the buffer to add around objects as ratio of the tile size.
   * See {@link #buffer}
   * @param b the size of the buffer as a ratio of the tile size
   */
  public void setBuffer(double b) {
    this.buffer = b;
  }

  public boolean isEmpty() {
    return pyramid.getMinimumLevel() > pyramid.getMaximumLevel();
  }

  /**
   * Check if the given tile should be considered or not.
   * @param z1 the zoom level of the tile to check
   * @param x the column at level z of the tile to check
   * @param y the row at level z of the tile to check
   * @param z2 when granularity &gt; 1, the maximum level (exclusive) to check under the tile (z1, x, y).
   *           To test only one tile, set z2 = z1 + 1
   * @return {@code true} if the given tile should be considered for partitioning
   */
  protected boolean isTileConsidered(int z1, int x, int y, int z2) {
    if (precachedTiles != null)
      return precachedTiles.contains(TileIndex.encode(z1, x, y));

    boolean considerTile;
    // Check if we need to consider the tile at (z1, x, y)
    if (histogram == null) {
      // Full pyramid partitioning mode, always consider the tile
      considerTile = true;
    } else if (tileToConsider != null) {
      // tile-based partitioning mode is enabled
      if (granularity == 1) {
        // Shortcut and more efficient code for granularity = 1. Just check this tile
        considerTile = MultilevelPyramidPlotHelper.classifyTile(histogram, threshold, buffer, z1, x, y) == tileToConsider;
      } else {
        considerTile = false;
        // Support granularity > 1 by iterating over all the tiles in the subpyramid rooted at (z1,x,y)
        // and going until z2 (exclusive of z2)
        for (int z = z1; z < z2 && !considerTile; z++) {
          int minc = x << (z - z1);
          int maxc = (x + 1) << (z - z1);
          int minr = y << (z - z1);
          int maxr = (y + 1) << (z - z1);
          boolean imageTileFound = false;
          boolean dataTileFound = false;
          for (int $c = minc; $c < maxc && !considerTile; $c++) {
            for (int $r = minr; $r < maxr && !considerTile; $r++) {
              MultilevelPyramidPlotHelper.TileClass tileClass =
                      MultilevelPyramidPlotHelper.classifyTile(histogram, threshold, buffer, z, $c, $r);
              imageTileFound = imageTileFound || (tileClass == MultilevelPyramidPlotHelper.TileClass.ImageTile);
              dataTileFound = dataTileFound || (tileClass == MultilevelPyramidPlotHelper.TileClass.DataTile);
              considerTile = tileClass == tileToConsider;
            }
          }
          if (tileToConsider == MultilevelPyramidPlotHelper.TileClass.ImageTile && !imageTileFound)
            break;
          if (tileToConsider == MultilevelPyramidPlotHelper.TileClass.DataTile && !dataTileFound && !imageTileFound)
            break;
        }
      }
    } else {
      // Size-based partitioning
      if (granularity == 1) {
        // Shortcut and more efficient code for granularity = 1
        long tileSize = MultilevelPyramidPlotHelper.getTileSize(histogram, buffer, z1, x, y);
        considerTile = tileSize >= minThreshold && tileSize < maxThreshold;
      } else {
        considerTile = false;
        // To support granularity > 1, check all the tiles under the current tile (column, row) and up-to
        // granularity levels
        for (int z = z1; z < z2 && !considerTile; z++) {
          int minc = x << (z - z1);
          int maxc = (x + 1) << (z - z1);
          int minr = y << (z - z1);
          int maxr = (y + 1) << (z - z1);
          long maxTileSize = 0;
          for (int $c = minc; $c < maxc && !considerTile; $c++) {
            for (int $r = minr; $r < maxr && !considerTile; $r++) {
              long tileSize = MultilevelPyramidPlotHelper.getTileSize(histogram, buffer, z, $c, $r);
              maxTileSize = Math.max(maxTileSize, tileSize);
              considerTile = tileSize >= minThreshold && tileSize < maxThreshold;
            }
          }
          // If the maximum tile size at this level is below threshold, then all child tiles are also below
          if (maxTileSize < minThreshold)
            break;
        }
      }
    }
    return considerTile;
  }

  /**
   * Clears the array {@code matchedTiles} and fills it with tile IDs that overlap the given MBR in the configured
   * subPyramid.
   * @param mbr the MBR to calculate the overlaps for
   */
  public long[] overlapPartitions(EnvelopeNDLite mbr) {
    if (mbr.isEmpty())
      return new long[0];
    List<Long> matchedTiles = new ArrayList<>();
    if (buffer == 0.0) {
      // Compute the overlaps with the base (deepest) level of the pyramid
      Rectangle overlaps = new Rectangle();
      pyramid.getOverlappingTiles(mbr, overlaps);
      // Handle the case when the MBR does not overlap any tiles at all
      if (overlaps.width <= 0 || overlaps.height <= 0)
        return new long[0];

      // Start at the deepest level and cover k=granularity levels at a time until reaching the minimum level
      int z2 = pyramid.getMaximumLevel() + 1;
      do {
        // Partition to the tiles at the level z1
        int z1 = Math.max(pyramid.getMinimumLevel(), z2 - granularity);
        // (c1,r1)-(c2,r2) are at level z1
        int c1 = overlaps.x >>> (pyramid.getMaximumLevel() - z1);
        int c2 = (overlaps.x + overlaps.width - 1) >>> (pyramid.getMaximumLevel() - z1);
        int r1 = overlaps.y >>> (pyramid.getMaximumLevel() - z1);
        int r2 = (overlaps.y + overlaps.height - 1) >>> (pyramid.getMaximumLevel() - z1);
        // Loop over all the tiles at level z1 and partition to them if they are considered
        for (int x = c1; x <= c2; x++) {
          for (int y = r1; y <= r2; y++) {
            if (isTileConsidered(z1, x, y, z2))
              matchedTiles.add(TileIndex.encode(z1, x, y));
          }
        }
        z2 = z1;
      } while (z2 > pyramid.getMinimumLevel());
    } else /* if (buffer != 0.0) */ {
      // General handling for a non-zero buffer
      int z2 = pyramid.getMaximumLevel() + 1;
      // The expanded MBR after adding the buffer
      EnvelopeNDLite expandedMBR = new EnvelopeNDLite(mbr);
      do {
        int z1 = Math.max(pyramid.getMinimumLevel(), z2 - granularity);
        expandedMBR.set(mbr);
        expandedMBR.buffer(pyramid.getTileWidth(z1) * buffer, pyramid.getTileHeight(z1) * buffer);
        // Compute the overlaps with the the level z1 of the pyramid
        Rectangle overlaps = new Rectangle();
        pyramid.getOverlappingTiles(expandedMBR, overlaps, z1);
        // Loop over all the overlapping tiles and check which ones should be reported
        for (int x = overlaps.x; x < overlaps.getMaxX(); x++) {
          for (int y = overlaps.y; y < overlaps.getMaxY(); y++) {
            if (isTileConsidered(z1, x, y, z2))
              matchedTiles.add(TileIndex.encode(z1, x, y));
          }
        }
        z2 = z1;
      } while (z2 > pyramid.getMinimumLevel());
    }
    long[] finalResults = new long[matchedTiles.size()];
    for (int i = 0; i < matchedTiles.size(); i++)
      finalResults[i] = matchedTiles.get(i);
    return finalResults;
  }

  /**
   * Returns the MBR of the given partition (i.e., tile).
   * @param tileID the ID of the tile
   * @param tileMBR the output MBR
   */
  public void getPartitionMBR(int tileID, Envelope tileMBR) {
    pyramid.getTileMBR(tileID, tileMBR);
  }

  /**
   * Returns an upper bound on the number of partitions that this partitioner can assign.
   * This takes into account the range of zoom levels and the tiles defined in the subpyramid.
   * It also takes into account the granularity to make this upper bound as tight as possible.
   * @return total number of partitions (tiles) covered by the underlying pyrpamid
   */
  public long getPartitionCount() {
    long totalNumOfTiles = 0;

    for (SubPyramid sp : pyramid.getSubPyramids(granularity))
      totalNumOfTiles++; // For each subpyramid, we only count the root
    return totalNumOfTiles;
  }

  /**
   * Returns {@code true} since pyramid partitioning is always disjoint for each level.
   * @return {@code true} since this is a space partitioning model
   */
  public boolean isDisjoint() {
    return true;
  }

  /**
   * Returns two. Pyramid partitioning is only defined for two-dimensional data.
   * @return the dimension of the space (always 2 since it is a two-dimensional space)
   */
  public int getCoordinateDimension() {
    return 2;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(pyramid);
    out.writeInt(granularity);
    out.writeDouble(buffer);
    if (histogram == null) {
      out.writeBoolean(false);
      if (precachedTiles == null) {
        out.writeBoolean(false);
      } else {
        out.writeInt(precachedTiles.size());
        for (long precachedTileID : precachedTiles)
          out.writeLong(precachedTileID);
      }
    } else {
      out.writeBoolean(true);
      out.writeObject(histogram);
      out.writeLong(threshold);
      out.writeLong(minThreshold);
      out.writeLong(maxThreshold);
      out.writeInt(tileToConsider == null? -1 : tileToConsider.ordinal());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    try {
      this.pyramid = (SubPyramid) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Error reading pyramid", e);
    }
    this.granularity = in.readInt();
    this.buffer = in.readDouble();
    boolean histogramExists = in.readBoolean();
    if (!histogramExists) {
      histogram = null;
      boolean precachedTilesExists = in.readBoolean();
      if (!precachedTilesExists) {
        this.precachedTiles = null;
      } else {
        int size = in.readInt();
        this.precachedTiles = new HashSet<>(size * 4 / 3);
        while (size-- > 0)
          this.precachedTiles.add(in.readLong());
      }
    } else {
      try {
        histogram = (AbstractHistogram) in.readObject();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find the histogram class", e);
      }
      threshold = in.readLong();
      minThreshold = in.readLong();
      maxThreshold = in.readLong();
      int iTileClass= in.readInt();
      tileToConsider = iTileClass == -1? null : MultilevelPyramidPlotHelper.TileClass.values()[iTileClass];
    }
  }

  public int getGranularity() {
    return granularity;
  }

  public int getMaxLevel() {
    return pyramid.getMaximumLevel();
  }

  public long getMinThreshold() {
    return this.minThreshold;
  }

  public long getMaxThreshold() {
    return this.maxThreshold;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    if (this.histogram == null && precachedTiles == null)
      str.append("full ");
    str.append("PyramidPartitioner: pyramid=")
       .append(pyramid);
    if (this.histogram != null || precachedTiles != null) {
      if (tileToConsider != null)
        str.append(" Tile: ")
           .append(tileToConsider);
      else
        str.append(" Threshold [")
           .append(minThreshold)
           .append(", ")
           .append(maxThreshold)
           .append(")");
    }
    if (granularity > 1)
      str.append(", granularity: ")
         .append(granularity);
    if (buffer != 0)
      str.append(", buffer: ")
         .append(buffer);
    return str.toString();
  }

  public AbstractHistogram getHistogram() {
    return histogram;
  }

  public EnvelopeNDLite getInputMBR() {
    return pyramid.getInputMBR();
  }
}
