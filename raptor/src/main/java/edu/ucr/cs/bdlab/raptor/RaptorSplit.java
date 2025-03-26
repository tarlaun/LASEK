/*
 * Copyright 2019 University of California, Riverside
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
package edu.ucr.cs.bdlab.raptor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A split that combines Raster-Plus-Vector (Raptor) data.
 * The split represents a set of tiles in a pair of a raster file and an intersection file.
 */
public class RaptorSplit extends InputSplit {
  /**Path to the raster file*/
  protected Path rasterPath;

  /**Path to the intersection file*/
  protected Path intersectionFilePath;

  /**The first tile ID to read*/
  protected int tileIDStart;

  /**The end tile ID to read (inclusive)*/
  protected int tileIDEnd;

  /**Number of pixels per tile. Not really used for processing but just to give a meaningful size.*/
  protected long pixelsPerTile;

  public RaptorSplit() {}

  public RaptorSplit(Path rasterPath, Path intersectionFilePath, int tileIDStart, int tileIDEnd, long pixelsPerTile) {
    this.rasterPath = rasterPath;
    this.intersectionFilePath = intersectionFilePath;
    this.tileIDStart = tileIDStart;
    this.tileIDEnd = tileIDEnd;
    this.pixelsPerTile = pixelsPerTile;
  }

  @Override
  public long getLength() {
    return (tileIDEnd - tileIDStart + 1) * pixelsPerTile;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }
}
