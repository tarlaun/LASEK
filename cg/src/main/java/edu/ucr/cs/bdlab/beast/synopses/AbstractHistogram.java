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
package edu.ucr.cs.bdlab.beast.synopses;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * An abstract class for a histogram that can retrieve the total value for a range.
 */
public abstract class AbstractHistogram extends EnvelopeNDLite implements Serializable {

  /**Dimensions of the grid of the histogram*/
  protected int[] numPartitions;

  abstract public long getValue(int[] minPos, int[] sizes);

  /**
   * Get the number of partitions along the given dimension
   * @param d the number of partitions along the given dimension
   * @return the number of partitions along the given dimension
   */
  abstract public int getNumPartitions(int d);

  /**
   * Returns the total number of bins in the histogram
   * @return the total number of bing in the histogram
   */
  public int getNumBins() {
    int numBins = 1;
    for (int d = 0; d < getCoordinateDimension(); d++) {
      numBins *= getNumPartitions(d);
    }
    return numBins;
  }

  /**
   * Returns a unique ID for the bin (cell) that contains the given point
   * @param coord the coordinate of the point
   * @return the ID of the bin that contains the given point
   */
  public int getBinID(double[] coord) {
    assert coord.length == getCoordinateDimension();
    int[] position = new int[coord.length];
    for (int d = 0; d < coord.length; d++) {
      position[d] = (int) Math.floor((coord[d] - this.getMinCoord(d)) * this.getNumPartitions(d) / this.getSideLength(d));
      position[d] = Math.min(position[d], getNumPartitions(d) - 1);
    }
    int d = position.length;
    int pos = 0;
    while (d-- > 0) {
      pos *= getNumPartitions(d);
      pos += position[d];
    }
    return pos;
  }

  /**
   * Returns the value of the given binID
   * @param binID the ID of the bin
   * @return the value of the given bin
   */
  public abstract long getBinValue(int binID);

  /**
   * Returns the envelope of a cell given its column and row position. To avoid unnecessary object creation, the given
   * envelope is filled in with the coordinates of the given cell.
   * @param position the position of the point to test
   * @param mbr the MBR to fill with the information. If {@code null}, a {@link NullPointerException} is thrown.
   */
  public void getCellEnvelope(int[] position, EnvelopeNDLite mbr) {
    mbr.setCoordinateDimension(this.getCoordinateDimension());
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      mbr.setMinCoord(d, (this.getMinCoord(d) * (numPartitions[d] - position[d]) + this.getMaxCoord(d) * position[d]) / numPartitions[d]);
      mbr.setMaxCoord(d, (this.getMinCoord(d) * (numPartitions[d] - (position[d]+ 1)) + this.getMaxCoord(d) * (position[d] + 1)) / numPartitions[d]);
    }
  }

  /**
   * Print the histogram information to a text output stream for debugging purposes.
   * @param out the print stream to write to, e.g., System.out
   * @param includeZeros whether to print zeros or not
   */
  public void print(PrintStream out, boolean includeZeros) {
    out.println("Column\tRow\tGeometry\tFrequency");
    EnvelopeNDLite env = new EnvelopeNDLite();
    for (int row = 0; row < getNumPartitions(1); row++) {
      for (int col = 0; col < getNumPartitions(0); col++) {
        long value = getValue(new int[] {col, row}, new int[] {1, 1});
        if (includeZeros || value > 0) {
          getCellEnvelope(new int[]{col, row}, env);
          out.printf("%d\t%d\t%s\t%d\n", col, row, env.toWKT(new StringBuilder()).toString(), value);
        }
      }
    }
  }
}
