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

import java.util.Arrays;

/**
 * A prefix histogram that can compute the total size of any rectangle over the histogram in constant time using
 * the idea of prefix sum.
 */
public class Prefix2DHistogram extends UniformHistogram {
  public Prefix2DHistogram() {}

  /**
   * Constructs a prefix sum histogram from an existing regular histogram
   * @param h the underlying histogram
   */
  public Prefix2DHistogram(UniformHistogram h) {
    if (h.getCoordinateDimension() != 2)
      throw new RuntimeException("Prefix histogram currently supports only two dimensions");

    // Copy attributes from the given histogram
    this.set(h);
    this.numPartitions = Arrays.copyOf(h.numPartitions, h.numPartitions.length);
    this.values = Arrays.copyOf(h.values, h.values.length);

    // Compute the prefix sum
    for (int $row = 0; $row < this.numPartitions[1]; $row++) {
      int offset = $row * numPartitions[0];
      for (int $col = 1; $col < this.numPartitions[0]; $col++)
        this.values[offset + $col] += this.values[offset + $col - 1];
      // Add the values from the previous row if needed
      if ($row != 0) {
        for (int $col = 0; $col < this.numPartitions[0]; $col++)
          this.values[offset + $col] += this.values[offset + $col - this.numPartitions[0]];
      }
    }

  }

  @Override
  public long getValue(int[] minPos, int[] sizes) {
    // Compute max row and column inclusive
    int maxCol = minPos[0] + sizes[0] - 1;
    int maxRow = minPos[1] + sizes[1] - 1;
    // Compute min row and column exclusive
    int minCol = minPos[0] - 1;
    int minRow = minPos[1] - 1;
    long sum = values[maxRow * numPartitions[0] + maxCol];
    if (minCol >= 0)
      sum -= values[maxRow * numPartitions[0] + minCol];
    if (minRow >= 0)
      sum -= values[minRow * numPartitions[0] + maxCol];
    if (minRow >= 0 && minCol >= 0)
      sum += values[minRow * numPartitions[0] + minCol];
    return sum;
  }

  @Override
  public long getBinValue(int binID) {
    int col = binID % numPartitions[0];
    int row = binID / numPartitions[0];
    long value = values[binID];
    if (col > 0)
      value -= values[binID - 1];
    if (row > 0)
      value -= values[binID - numPartitions[0]];
    if (col > 0 && row > 0)
      value += values[binID - numPartitions[0] - 1];
    return value;
  }
}
