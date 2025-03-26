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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * An Euler histogram that uses prefix sum to allow the calculation of any range in constant time.
 * This histogram is read-only. It is created from a regular (non-prefix) Euler histogram and used to
 * answer queries but cannot be updated.
 */
public class PrefixEulerHistogram2D extends EulerHistogram2D {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(PrefixEulerHistogram2D.class);

	/**Default constructor is needed for deserialization*/
	public PrefixEulerHistogram2D() { }

  /**
   * Initializes this histogram from a regular (non-prefix) Euler histogram
   * @param h the underlying Euler histogram
   */
  public PrefixEulerHistogram2D(EulerHistogram2D h) {
    super(h, h.getNumColumns(), h.getNumRows());
    // Copy all attributes as-is from the given histogram
    c1 = Arrays.copyOf(h.c1, h.c1.length);
    c2 = Arrays.copyOf(h.c2, h.c2.length);
    c3 = Arrays.copyOf(h.c3, h.c3.length);
    c4 = Arrays.copyOf(h.c4, h.c4.length);

    // Compute the prefix sums as follows
    // c1: Compute a prefix sum along the two dimensions
    // c2: Compute a prefix sum along the y-axis
    // c3: Compute a prefix sum along the x-axis
    // c4: No prefix sum is needed
    for (int $row = 0; $row < this.numRows; $row++) {
      int offset = $row * numColumns;
      for (int $col = 1; $col < this.numColumns; $col++) {
        this.c1[offset + $col] += this.c1[offset + $col - 1];
        this.c3[offset + $col] += this.c3[offset + $col - 1];
      }
      // Compute prefix sum along the y-axis
      if ($row != 0) {
        for (int $col = 0; $col < this.numColumns; $col++) {
          this.c1[offset + $col] += this.c1[offset + $col - this.numColumns];
          this.c2[offset + $col] += this.c2[offset + $col - this.numColumns];
        }
      }
    }
	}

  /**
   * This method should not be used. Instead, the histogram should be updated using the super class
   * {@link EulerHistogram2D} and this class should be used to query it.
   * @param col the first column that the range covers
   * @param row the first row that the range covers
   * @param width the number of column that the range covers
   * @param height the number of rows that the range covers
   * @param value that value to be added
   */
  @Override
  public void addEntry(int col, int row, int width, int height, long value) {
    throw new RuntimeException("Not efficiently supported!");
  }

  /**
	 * Computes the sum of all values in the given range of grid cells.
   * @param col the first column that the range covers
   * @param row the first row that the range covers
   * @param width the number of column that the range covers
   * @param height the number of rows that the range covers
	 * @return the sum of values in the given rectangle
	 */
	@Override
	public long getValue(int col, int row, int width, int height) {
	  long sum = 0;
	  int offset = row * numColumns + col;
	  // Count the ranges that overlap the top-left corner of the given range (col, row)
	  sum += c4[offset];
	  // Add the ranges that have a left edge in a cell along the top line
    sum += c3[offset + (width - 1)] - c3[offset];
	  // Add the ranges that have a top edge in a cell along the left line
    sum += c2[offset + (height - 1) * numColumns] - c2[offset];
   // Add all the ranges that have a top-left corner in all other overlapping cells
    sum += c1[offset + (height - 1) * numColumns + (width - 1)]
        - c1[offset + (width - 1)]
        - c1[offset + (height - 1) * numColumns]
        + c1[offset];
		return sum;
	}
}