/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.cg;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import org.apache.hadoop.util.IndexedSortable;

/**
 * A class for comparing and sorting points based on the Hilbert-Curve
 */
public class HCurveSortable implements IndexedSortable {

  /**A temporary array for holding the position of the points to compare*/
  int[] centeri;
  int[] centerj;
  /**The MBR of the entire input space*/
  EnvelopeNDLite mbr;
  /**The array of point coordinate to compare. The first index is for the dimensions and the second for the coordinate*/
  double[][] points;

  public HCurveSortable(EnvelopeNDLite mbr) {
    this.mbr = mbr;
    centeri = new int[mbr.getCoordinateDimension()];
    centerj = new int[mbr.getCoordinateDimension()];
  }

  public void setPoints(double[][] points) {
    this.points = points;
  }

  @Override
  public int compare(int i, int j) {
    EnvelopeNDLite submbr = new EnvelopeNDLite(mbr);
    int n = submbr.getCoordinateDimension();
    // We always start at 0,0 but this is arbitrary
    long startCorner = 0;
    // The exit corner varies in only one dimension
    long exitCorner = 1L;
    // We try up-to 32 iterations and then assume that the two points are equal.
    // We can go further if we want but we decided to stop at 32 iterations to ensure an upper-bound
    for (int iteration = 0; iteration < 32; iteration++) {
      assert Long.lowestOneBit(startCorner ^ exitCorner) == Long.highestOneBit(startCorner ^ exitCorner) :
              String.format("Start and end corners can differ in only dimension, %d , %d", startCorner, exitCorner);
      long cornerI = 0;
      long cornerJ = 0;
      // Locate each point in one of the 2^n corners
      int mostSignificantCoordinate = Long.numberOfTrailingZeros(startCorner ^ exitCorner);
      boolean reverseOrder = false;
      for (int d = 0; d < n; d++) {
        int dim = (mostSignificantCoordinate + d) % n;
        long dimensionMask = 1L << dim;
        if (points[dim][i] >= submbr.getCenter(dim))
          cornerI |= dimensionMask;
        if (points[dim][j] >= submbr.getCenter(dim))
          cornerJ |= dimensionMask;
        boolean iCloserToStartCorner = (cornerI & dimensionMask) == (startCorner & dimensionMask);
        if (cornerI != cornerJ) {
          // If they are in different corners, return the comparison result now
          // The point that remained the same as the start corner is the lower order point
          return iCloserToStartCorner && !reverseOrder || !iCloserToStartCorner && reverseOrder? -1 : +1;
        }
        // Both I and J fall in the same corner
        if (!iCloserToStartCorner)
          reverseOrder = !reverseOrder;
        // Reduce the region since both point fall in the same half space
        if ((cornerI & dimensionMask) == 0)
          submbr.setMaxCoord(dim, submbr.getCenter(dim));
        else
          submbr.setMinCoord(dim, submbr.getCenter(dim));
      }
      // Both points are in the same subregion, prepare for the next iteration
      // We need to update the start corner and the most significant coordinate for the next iteration
      // We do that by moving one corner at a time until we reach the end corner.
      // TODO This might take up-to 2^n iterations. If this is too slow, we can try to optimize later
      long[] dimensionMasks = new long[n];
      // Start with the most significant coordinate
      dimensionMasks[n - 1] = startCorner ^ exitCorner;
      for (int d = n - 2; d >= 0; d--) {
        dimensionMasks[d] = dimensionMasks[d+1] << 1;
        if (dimensionMasks[d] == 1L << n)
          dimensionMasks[d] = 1L;
      }
      int step = 0;
      long startCornerNextIter = startCorner;
      long cornerIterator = startCorner;
      do {
        //assert step < 1 << n : "Too many steps "+step;
        long grayCodeThisStep = step ^ (step >>> 1);
        int nextStep = (step + 1) % (1 << n);
        long grayCodeNextStep = nextStep ^ (nextStep >>> 1);
        long dimensionToChange = grayCodeThisStep ^ grayCodeNextStep;
        long exitCornerNextIter;
        if (step < (1 << n) - 1) {
          exitCornerNextIter = computeExitCorner(dimensionMasks[Long.numberOfTrailingZeros(dimensionToChange)],
                  cornerIterator, startCornerNextIter, exitCorner);
        } else {
          // Last corner, go directly to the exit corner
          exitCornerNextIter = exitCorner;
        }
        if (cornerIterator == cornerI) {
          startCorner = startCornerNextIter;
          exitCorner = exitCornerNextIter;
          assert startCorner != exitCorner :
              String.format("Start and exit corner cannot be the same %d == %d", startCorner, exitCorner);
          assert Long.lowestOneBit(startCorner ^ exitCorner) == Long.highestOneBit(startCorner ^ exitCorner) :
                  String.format("Only one bit should be changed %d %d", startCorner, exitCorner);
          break;
        }
        startCornerNextIter = exitCornerNextIter ^ dimensionMasks[Long.numberOfTrailingZeros(dimensionToChange)];
        cornerIterator ^= dimensionMasks[Long.numberOfTrailingZeros(dimensionToChange)];
        step++;
      } while (true);
    }
    return 0;
  }

  static long computeExitCorner(long changeMask, long currentCorner, long startCorner, long lastExitCorner) {
    if ((startCorner & changeMask) != (currentCorner & changeMask)) {
      // Change any other bit. We choose to change the bit that would make it closer to the lastExitCorner
      long differentBitsBetweenStartCornerAndLastExitCorner = startCorner ^ lastExitCorner;
      long removeChangeMaskSinceWeCannotModifyIt = differentBitsBetweenStartCornerAndLastExitCorner & ~(changeMask);
      if (removeChangeMaskSinceWeCannotModifyIt != 0) {
        changeMask = Long.lowestOneBit(removeChangeMaskSinceWeCannotModifyIt);
      } else {
        changeMask = changeMask == 1? (changeMask << 1) : (changeMask >> 1);
      }
    }
    assert changeMask != 0 : "Change mask cannot be zero";
    return startCorner ^ changeMask;
  }

  @Override
  public void swap(int i, int j) {
    double t;
    for (int d = 0; d < points.length; d++) {
      t = points[d][i];
      points[d][i] = points[d][j];
      points[d][j] = t;
    }
  }
}
