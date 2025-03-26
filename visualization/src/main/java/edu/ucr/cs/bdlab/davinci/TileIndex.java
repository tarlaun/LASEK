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

/**
 * An class that represents a position of a tile in the pyramid.
 * Level is the level of the tile starting with 0 at the top.
 * x and y are the index of the column and row of the tile in the grid
 * at this level.
 * @author Ahmed Eldawy
 *
 */
public class TileIndex {
  /**Coordinates of the tile*/
  public int z, x, y;
  
  public TileIndex() {}

  public TileIndex(int z, int x, int y) {
    assert x >= 0;
    assert y >= y;
    assert x < (1 << z);
    assert y < (1 << z);
    this.z = z;
    this.x = x;
    this.y = y;
  }

  @Override
  public String toString() {
    return String.format("Level: %d @(%d,%d)", z, x, y);
  }
  
  @Override
  public int hashCode() {
    return z * 31 + x * 25423 + y;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    TileIndex b = (TileIndex) obj;
    return this.z == b.z && this.x == b.x && this.y == b.y;
  }

  /**
   * Encodes the tile index (z,x,y) into an integer value for a compact storage and communication.
   * @param z the zoom level
   * @param x the x-coordinate as an integer in the range [0, 2^z)
   * @param y the y-coordinate as an integer in the range [0, 2^z)
   * @return a long that uniquely identifies this log
   */
  public static long encode(int z, int x, int y) {
    assert x >= 0;
    assert y >= y;
    assert x < (1 << z);
    assert y < (1 << z);
    return (1L << (2*z)) | (((long)x) << z) | (y);
  }

  /**
   * Decodes the given encoded tile ID back into z, x, and y values.
   * @param encodedTileID a tile ID that was encoded using the {@link #encode(int, int, int)} method.
   * @param tileIndex an existing TileIndex object to reuse. If {@code null}, a new object is created and returned.
   * @return either the same tile index that was passed, or a new object if the passed tileIndex was null.
   * @see #encode(int, int, int)
   */
  public static TileIndex decode(long encodedTileID, TileIndex tileIndex) {
    if (tileIndex == null)
      tileIndex = new TileIndex();
    tileIndex.z = (63 - Long.numberOfLeadingZeros(encodedTileID)) / 2;
    long mask = 0x7fffffffffffffffL >>> (63 - tileIndex.z);
    tileIndex.x = (int) ((encodedTileID >> tileIndex.z) & mask);
    tileIndex.y = (int) (encodedTileID & mask);
    return tileIndex;
  }
  
  /**
   * Returns the MBR of this rectangle given that the MBR of whole pyramid
   * (i.e., the top tile) is the given one.
   * @param spaceMBR the MBR of the entire input domain (i.e., the MBR of each level)
   * @param z the zoom level of the tile
   * @param x the x-coordinate of the tile in level z
   * @param y the y-coordinate of the tile in level z
   * @param tileMBR the output MBR is returned in this tileMBR object
   */
  public static void getMBR(EnvelopeNDLite spaceMBR, int z, int x, int y, Envelope tileMBR) {
    int fraction = 1 << z;
    double tileWidth = spaceMBR.getSideLength(0) / fraction;
    double tileHeight = spaceMBR.getSideLength(1) / fraction;
    double x1 = spaceMBR.getMinCoord(0) + tileWidth * x;
    double y1 = spaceMBR.getMinCoord(1) + tileHeight * y;
    tileMBR.init(x1, x1 + tileWidth, y1, y1 + tileHeight);
  }

  public static void getMBR(EnvelopeNDLite spaceMBR, long encodedTileID, Envelope tileMBR) {
    int z = (63 - Long.numberOfLeadingZeros(encodedTileID)) / 2;
    long mask = 0x7fffffffffffffffL >>> (63 - z);
    int x = (int) ((encodedTileID >> z) & mask);
    int y = (int) (encodedTileID & mask);
    getMBR(spaceMBR, z, x, y, tileMBR);
  }

  public void moveToParent() {
    this.x /= 2;
    this.y /= 2;
    this.z--;
  }

  public void vflip() {
    this.y = ((1 << z) - 1) - y;
  }
}
