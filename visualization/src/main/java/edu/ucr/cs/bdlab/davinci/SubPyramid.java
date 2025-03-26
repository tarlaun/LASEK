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

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import org.locationtech.jts.geom.Envelope;

import java.awt.*;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Represents a part of the pyramid structure defined by a minimum level,
 * a maximum level, and a rectangular range of tiles at the maximum level.
 * Created by Ahmed Eldawy on 1/2/18.
 */
public class SubPyramid implements Serializable, Iterable<Long> {
  /**
   * The MBR covered by the full pyramid (not this subpyramid).
   * The range is inclusive for (x1, y1) and exclusive for (x2, y2)
   */
  private double x1, y1, x2, y2;

  /** The range of levels (inclusive) covered by the subpyramid */
  private int minimumLevel, maximumLevel;

  /**
   * The range of tiles covered by this subpyramid at the maximumLevel
   * The range includes the first row (r1), the first column (c1), and
   * excludes the last row (r2) and last column (c2).
   */
  private int c1, r1, c2, r2;

  /** The width and height of each tile at maximumLevel */
  private transient double tileWidth, tileHeight;

  /**Empty constructor for the Writable interface*/
  public SubPyramid() {
  }

  /**
   * Construct a new SubPyramid
   * @param mbr the minimum bounding rectangle of the full pyramid, i.e., the MBR of the tile at level 0
   * @param minLevel the minimum level covered by this subpyramid (0-based inclusive)
   * @param maxLevel the maximum level covereted by this subpyramid (0-based and inclusive)
   * @param c1 the first column at maxLevel covered by this subpyramid (0-based and inclusive)
   * @param r1 the first row at maxLevel covered by this subpyramid (0-based and inclusive)
   * @param c2 the last column at maxLevel covered by this subpyramid (0-based and exclusive)
   * @param r2 the last row at maxLevel covered by this subpyramid (0-based and exclusive)
   */
  public SubPyramid(EnvelopeNDLite mbr, int minLevel, int maxLevel, int c1, int r1, int c2, int r2) {
    this.set(mbr, minLevel, maxLevel, c1, r1, c2, r2);
  }


  /**
   * Construct a new SubPyramid that covers all the tiles rooted at a specified tile and going in a range of levels
   * @param mbr the minimum bounding rectangle of the full pyramid, i.e., the MBR of the tile at level 0
   * @param minLevel the minimum level covered by this subpyramid (0-based inclusive)
   * @param maxLevel the maximum level covered by this subpyramid (0-based and inclusive)
   * @param x the column of the root tile at minLevel
   * @param y the row of the root tile at minLevel
   */
  public SubPyramid(EnvelopeNDLite mbr, int minLevel, int maxLevel, int x, int y) {
    this.set(mbr, minLevel, maxLevel, x << (maxLevel - minLevel), y << (maxLevel - minLevel),
            (x+1) << (maxLevel - minLevel), (y + 1) << (maxLevel - minLevel));
  }


  /**
   * Creates a pyramid that covers the entire region of interest in the given zoom levels (inclusive of both)
   * @param mbr the region of interest
   * @param minLevel the minimum level (inclusive)
   * @param maxLevel the maximum level (inclusive)
   */
  public SubPyramid(EnvelopeNDLite mbr, int minLevel, int maxLevel) {
    this.set(mbr, minLevel, maxLevel, 0, 0, 1 << maxLevel, 1 << maxLevel);
  }

  public void set(SubPyramid that) {
    this.x1 = that.x1;
    this.y1 = that.y1;
    this.x2 = that.x2;
    this.y2 = that.y2;
    this.minimumLevel = that.minimumLevel;
    this.maximumLevel = that.maximumLevel;
    this.c1 = that.c1;
    this.r1 = that.r1;
    this.c2 = that.c2;
    this.r2 = that.r2;
    this.tileWidth = that.tileWidth;
    this.tileHeight = that.tileHeight;
  }

  /**
   *
   * @param mbr the MBR of the input space covered by the entire pyramid (not the subpyramid)
   * @param minLevel the minimum level covered by this sub-pyramid (inclusive)
   * @param maxLevel the maximum level covered by this sub-pyramid (inclusive)
   * @param c1 At {@code maxLevel} the index of the first column covered by this sub-pyramid (inclusive)
   * @param r1 At {@code maxLevel} the index of the first row covered by this sub-pyramid (inclusive)
   * @param c2 At {@code maxLevel} the index of the last column covered by this sub-pyramid (exclusive)
   * @param r2 At {@code maxLevel} the index of the last row covered by this sub-pyramid (exclusive)
   */
  public void set(EnvelopeNDLite mbr, int minLevel, int maxLevel,
                  int c1, int r1, int c2, int r2) {
    assert c1 >= 0 && c2 <= (1 << maxLevel) && r1 >= 0 && r2 <= (1 << maxLevel):
            String.format("Illegal pyramid boundaries maxlevel=%d, (%d,%d)->(%d,%d)", maxLevel, c1, r1, c2, r2);
    this.x1 = mbr.getMinCoord(0);
    this.x2 = mbr.getMaxCoord(0);
    this.y1 = mbr.getMinCoord(1);
    this.y2 = mbr.getMaxCoord(1);
    this.minimumLevel = minLevel;
    this.maximumLevel = maxLevel;
    this.c1 = c1;
    this.r1 = r1;
    this.c2 = c2;
    this.r2 = r2;
    this.tileWidth = 0;
    this.tileHeight = 0;
  }

  /**
   * Set the sub-pyramid to cover only one tile
   * @param mbr The MBR of the entire data set
   * @param z The level of the given tile starting at zero
   * @param c The column of the given tile
   * @param r The row of the given tile
   */
  public void set(EnvelopeNDLite mbr, int z, int c, int r) {
    assert c >= 0 && c < (1 << z) && r >= 0 && r < (1 << z) :
            String.format("Invalid tile (%d,%d,%d)", z, c, r);
    this.x1 = mbr.getMinCoord(0);
    this.x2 = mbr.getMaxCoord(0);
    this.y1 = mbr.getMinCoord(1);
    this.y2 = mbr.getMaxCoord(1);
    this.minimumLevel = this.maximumLevel = z;
    this.c1 = c;
    this.c2 = c + 1;
    this.r1 = r;
    this.r2 = r + 1;
    this.tileWidth = 0;
    this.tileHeight = 0;
  }

  public void getOverlappingTiles(EnvelopeNDLite envelope, Rectangle overlaps) {
    getOverlappingTiles(envelope, overlaps, this.maximumLevel);
  }

  /**
   * Returns the range of tiles that overlaps the given rectangle at the maximum (deepest) level
   * @param rect the rectangle that represents the object
   * @param overlaps (output) the range of overlapping tile IDs in the given level.
   * @param z the zoom level to consider for the returned tiles
   */
  public void getOverlappingTiles(EnvelopeNDLite rect, java.awt.Rectangle overlaps, int z) {
    int diffZ = this.maximumLevel - z;
    double zTileWidth = getTileWidth() * (1 << diffZ);
    double zTileHeight = getTileHeight() * (1 << diffZ);
    // the bounds of the subpyramid at the given level (z)
    int zc1 = c1 >> diffZ;
    int zc2 = c2 >> diffZ;
    int zr1 = r1 >> diffZ;
    int zr2 = r2 >> diffZ;
    overlaps.x = (int) Math.floor((rect.getMinCoord(0) - this.x1) / zTileWidth);
    if (rect.getMinCoord(0) == rect.getMaxCoord(0) && rect.getMinCoord(0) == x2) {
      // Special case when the rect represents a point aligned along the upper edge of the input along the x-axis
      overlaps.x--;
    }
    if (overlaps.x >= zc2) {
      overlaps.width = 0;
      return;
    }
    overlaps.y = (int) Math.floor((rect.getMinCoord(1) - this.y1) / zTileHeight);
    if (rect.getMinCoord(1) == rect.getMaxCoord(1) && rect.getMinCoord(1) == y2) {
      // Special case when the rect represent a point aligned along the upper edge of the input along the y-axis
      overlaps.y--;
    }
    if (overlaps.y >= zr2) {
      overlaps.height = 0;
      return;
    }
    // Compute the width and height
    int x2 = (int) Math.ceil((rect.getMaxCoord(0) - this.x1) / zTileWidth);
    int y2 = (int) Math.ceil((rect.getMaxCoord(1) - this.y1) / zTileHeight);
    // To accommodate points, the minimum width and height should be 1
    overlaps.width = Math.max(1, x2 - overlaps.x);
    overlaps.height = Math.max(1, y2 - overlaps.y);
    // Update the width and height when the maxx or maxy is beyond the treshold
    if (overlaps.x + overlaps.width > zc2)
      overlaps.width = zc2 - overlaps.x;
    if (overlaps.y + overlaps.height > zr2)
      overlaps.height = zr2 - overlaps.y;
    if (overlaps.x < zc1) {
      overlaps.width -= zc1 - overlaps.x;
      overlaps.x = zc1;
    }
    if (overlaps.y < zr1) {
      overlaps.height -= zr1 - overlaps.y;
      overlaps.y = zr1;
    }
  }

  public EnvelopeNDLite getInputMBR() {
    return new EnvelopeNDLite(2, x1, y1, x2, y2);
  }

  /**
   * Returns the total number of tiles covered by this subpyramid
   * @return the total number of tiles in the pyramid
   */
  public long getTotalNumberOfTiles() {
    // The following (commented) formulae only work if the entire levels are covered.
    // All tiles until z_max = (4^{z_max+1} - 1) / (4 - 1)
    //long totalNumTilesUntilMaximumLevel = (1L << (2 * (maximumLevel + 1))) / (4 - 1);
    // All tiles up-to (not including) minimum level = (4^{z_min} - 1) / (4 - 1)
    //long totalNumTilesBeforeMinimumLevel = (1L << (2 * minimumLevel)) / (4 - 1);
    //return totalNumTilesUntilMaximumLevel - totalNumTilesBeforeMinimumLevel;

    long totalNumOfTiles = 0;
    int $c1 = c1, $c2 = c2 - 1, $r1 = r1, $r2 = r2 - 1;
    for (int $l = maximumLevel; $l >= minimumLevel; $l--) {
      totalNumOfTiles += ($c2 - $c1 + 1) * ($r2 - $r1 + 1);
      $c1 /= 2;
      $c2 /= 2;
      $r1 /= 2;
      $r2 /= 2;
    }
    return totalNumOfTiles;
  }

  public void getTileMBR(int tileID, Envelope tileMBR) {
    // TODO: Avoid creating the envelope
    TileIndex.getMBR(new EnvelopeNDLite(2, x1, y1, x2, y2), tileID, tileMBR);
  }

  /**
   * Computes the width of a tile at the given zoom level. Notice that all tiles at the same zoom level will have the
   * same width.
   * @param z the zoom level.
   * @return the width of the tile in the input space
   */
  public double getTileWidth(int z) {
    return (x2 - x1) / (1 << z);
  }

  public double getTileHeight(int z) {
    return (y2 - y1) / (1 << z);
  }

  public void setMaximumLevel(int maximumLevel) {
    if (maximumLevel > this.maximumLevel) {
      int diff = maximumLevel - this.maximumLevel;
      this.c1 <<= diff;
      this.c2 <<= diff;
      this.r1 <<= diff;
      this.r2 <<= diff;
    } else if (this.maximumLevel > maximumLevel) {
      int diff = this.maximumLevel - maximumLevel;
      this.c1 >>= diff;
      this.c2 >>= diff;
      this.r1 >>= diff;
      this.r2 >>= diff;
      // Ensure that the range did not become empty
      if (this.c1 == this.c2)
        this.c2++;
      if (this.r1 == this.r2)
        this.r2++;
    }

    this.maximumLevel = maximumLevel;
  }

  public final double getX1() {
    return x1;
  }

  public final double getY1() {
    return y1;
  }

  public final double getX2() {
    return x2;
  }

  public final double getY2() {
    return y2;
  }

  public final int getMinimumLevel() {
    return minimumLevel;
  }

  public final int getMaximumLevel() {
    return maximumLevel;
  }

  public final int getC1() {
    return c1;
  }

  public final int getR1() {
    return r1;
  }

  public final int getC2() {
    return c2;
  }

  public int getR2() {
    return r2;
  }

  /**
   * The width of one tile at the deepest level (maximumLevel)
   * @return
   */
  public double getTileWidth() {
    if (tileWidth == 0)
      tileWidth = (x2 - x1) / (1 << maximumLevel);
    return tileWidth;
  }

  /**
   * The height of one tile at the deepest level (maximumLevel)
   * @return
   */
  public double getTileHeight() {
    if (tileHeight == 0)
      tileHeight = (y2 - y1) / (1 << maximumLevel);
    return tileHeight;
  }

  @Override
  public String toString() {
    return String.format("SubPyramid[%d,%d], (%d,%d)->(%d,%d), MBR (%f, %f) -(%f, %f)",
            minimumLevel, maximumLevel, c1, r1, c2, r2, x1, y1, x2, y2);
  }

  /**
   * If this pyramid covers only one tile at the level minimumLevel, it returns the ID of that tile.
   * Otherwise, it returns the lowest tileID at minimumLevel
   * @return the ID of the root tile for this subpyramid
   */
  public long getRootTileID() {
    int x = c1 >> (maximumLevel - minimumLevel);
    int y = r1 >> (maximumLevel - minimumLevel);
    return TileIndex.encode(minimumLevel, x, y);
  }

  @Override
  public Iterator<Long> iterator() {
    return new TileIterator();
  }

  class TileIterator implements Iterator<Long> {
    /**The ID of the tile to return when next is called*/
    int z, x, y;

    /**The range of x and y at the current level [min, max)*/
    int minx, maxx;
    int miny, maxy;

    TileIterator() {
      // Start at the deepest level
      z = maximumLevel;
      minx = c1;
      maxx = c2;
      miny = r1;
      maxy = r2;
      x = minx;
      y = miny;
    }


    @Override
    public boolean hasNext() {
      return z >= minimumLevel;
    }

    @Override
    public Long next() {
      long currentTile = TileIndex.encode(z, x, y);
      // Move to the next tile
      x++;
      if (x == maxx) {
        x = minx;
        y++;
        if (y == maxy) {
          // Move to the upper level
          z--;
          if (z >= minimumLevel) {
            minx = c1 >>> (maximumLevel - z);
            maxx = c2 >>> (maximumLevel - z);
            miny = r1 >>> (maximumLevel - z);
            maxy = r2 >>> (maximumLevel - z);
            x = minx;
            y = miny;
          }
        }
      }
      return currentTile;
    }
  }

  /**
   * Returns all sub-pyramids that are contained in this pyramid with the given granularity.
   *
   * @param granularity how many levels to combine together
   * @return an iterable over all subpyramids at the given granulairty
   */
  public Iterable<SubPyramid> getSubPyramids(final int granularity) {
    return () -> new SubPyramidIterator(granularity);
  }

  class SubPyramidIterator implements Iterator<SubPyramid> {
    private final int granularity;
    /**The value that will be returned when next is called*/
    final SubPyramid nextValue = new SubPyramid();

    /**The range of zoom levels currently being iterated*/
    int z1, z2;
    /**The range of tiles at level z1 that are currently being iterated*/
    int c1, c2, r1, r2;
    /**The root of the pyramid that will be returned next at level z1*/
    int x, y;

    SubPyramidIterator(int granularity) {
      this.granularity = granularity;
      // Initialize the iterators
      z2 = maximumLevel + 1;
      z1 = Math.max(minimumLevel, z2 - granularity);
      // (c1,r1)-(c2,r2) are at level z1 and inclusive of c2 and r2
      c1 = getC1() >>> (maximumLevel - z1);
      c2 = (getC2() - 1) >>> (maximumLevel - z1);
      r1 = getR1() >>> (maximumLevel - z1);
      r2 = (getR2() - 1) >>> (maximumLevel - z1);
      // Start the iteration at level z1
      x = c1;
      y = r1;
      nextValue.x1 = SubPyramid.this.x1;
      nextValue.y1 = SubPyramid.this.y1;
      nextValue.x2 = SubPyramid.this.x2;
      nextValue.y2 = SubPyramid.this.y2;
    }

    @Override
    public boolean hasNext() {
      return z2 > minimumLevel;
    }

    @Override
    public SubPyramid next() {
      nextValue.c1 = x << (z2 - 1 - z1);
      nextValue.c2 = (x + 1) << (z2 - 1 - z1);
      nextValue.r1 = y << (z2 - 1 - z1);
      nextValue.r2 = (y + 1) << (z2 - 1 - z1);
      nextValue.minimumLevel = z1;
      nextValue.maximumLevel = z2 - 1;
      x++;
      if (x > c2) {
        y++;
        x = c1;
        if (y > r2) {
          // Move up the pyramid
          z2 = z1;
          if (z2 > minimumLevel) {
            z1 = Math.max(minimumLevel, z2 - granularity);
            // (c1,r1)-(c2,r2) are at level z1 and inclusive of c2 and r2
            c1 = getC1() >>> (maximumLevel - z1);
            c2 = (getC2() - 1) >>> (maximumLevel - z1);
            r1 = getR1() >>> (maximumLevel - z1);
            r2 = (getR2() - 1) >>> (maximumLevel - z1);
            // Start the iteration at level z1
            x = c1;
            y = r1;
          }
        }
      }
      return nextValue;
    }
  }
}
