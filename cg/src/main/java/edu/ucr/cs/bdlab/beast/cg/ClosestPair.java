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

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class for computing the closest pair of points
 */
public class ClosestPair {

  /**
   * Returns the distance between the two closest pair of points
   * @param xs the list of x coordinates
   * @param ys the list of y coordinates
   * @param numPoints number of points in the lists
   * @return the L1-norm (Manhattan distance) between the two closest points
   */
  public static int closestPairInMemory(final int[] xs, final int[] ys, final int numPoints) {
    final int threshold = 100;
    // Sort points by increasing x-axis
    new QuickSort().sort(new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return xs[i] - xs[j];
      }

      @Override
      public void swap(int i, int j) {
        int t = xs[i];
        xs[i] = xs[j];
        xs[j] = t;

        t = ys[i];
        ys[i] = ys[j];
        ys[j] = t;
      }
    }, 0, numPoints);

    class SubListComputation {
      int start, end;
      int p1, p2;
      double distance;
    }

    List<SubListComputation> sublists = new ArrayList<SubListComputation>();

    // Compute the closest pair for each sublist below the threshold
    int start = 0;
    while (start < numPoints) {
      int end;
      if (start + (threshold * 3 / 2) > numPoints)
        end = numPoints;
      else
        end = start + threshold;
      SubListComputation closestPair = new SubListComputation();
      closestPair.start = start;
      closestPair.end = end;
      closestPair.p1 = start;
      closestPair.p2 = start+1;
      // Use Manhattan distance for simplicity
      closestPair.distance = Math.abs(xs[start] - xs[start+1]) + Math.abs(ys[start] - ys[start+1]);

      for (int i1 = start; i1 < end; i1++) {
        for (int i2 = i1 + 1; i2 < end; i2++) {
          double distance = Math.abs(xs[i1] - xs[i2]) + Math.abs(ys[i1] - ys[i2]);
          if (distance < closestPair.distance) {
            closestPair.p1 = i1;
            closestPair.p2 = i2;
            closestPair.distance = distance;
          }
        }
      }
      sublists.add(closestPair);
      start = end;
    }

    // Merge each pair of adjacent sublists
    while (sublists.size() > 1) {
      List<SubListComputation> newSublists = new ArrayList<SubListComputation>();
      for (int ilist = 0; ilist < sublists.size() - 1; ilist += 2) {
        SubListComputation list1 = sublists.get(ilist);
        SubListComputation list2 = sublists.get(ilist+1);
        SubListComputation merged = new SubListComputation();
        merged.start = list1.start;
        merged.end = list2.end;
        // The closest pair of (list1 UNION list2) is either the closest pair
        // of list1, list2, or a new closest pair with one point in list1
        // and one point in list2
        double mindistance = Math.min(list1.distance, list2.distance);
        double xmin = xs[list1.end - 1] - mindistance;
        double xmax = xs[list2.start] + mindistance;
        int leftMargin = exponentialSearchLeft(xs, list1.end, xmin);
        int rightMargin = exponentialSearchRight(xs, list2.start, xmax);
        int minPointL = leftMargin, minPointR = list2.start;
        double minDistanceLR = Math.abs(xs[minPointL] - xs[minPointR]) + Math.abs(ys[minPointL] - ys[minPointR]);
        if (rightMargin - leftMargin < threshold) {
          // Use brute force technique
          for (int i1 = leftMargin; i1 < list1.end; i1++) {
            for (int i2 = list2.start; i2 < rightMargin; i2++) {
              double distance = Math.abs(xs[i1] - xs[i2]) + Math.abs(ys[i1] - ys[i2]);
              if (distance < mindistance) {
                minPointL = i1;
                minPointR = i2;
                minDistanceLR = distance;
              }
            }
          }
        } else {
          // Use a y-sort technique
          final int[] rPoints = new int[rightMargin - list2.start];
          for (int i = 0; i < rPoints.length; i++)
            rPoints[i] = i + list2.start;
          IndexedSortable ysort = new IndexedSortable() {
            @Override
            public void swap(int i, int j) {
              int temp = rPoints[i]; rPoints[i] = rPoints[j]; rPoints[j] = temp;
            }

            @Override
            public int compare(int i, int j) {
              double dy = ys[rPoints[i]] - ys[rPoints[j]];
              if (dy < 0) return -1; if (dy > 0) return 1; return 0;
            }
          };
          new QuickSort().sort(ysort, 0, rPoints.length);
          int rpoint1 = 0, rpoint2 = 0;
          for (int ilPoint = leftMargin; ilPoint < list1.end; ilPoint++) {
            while (rpoint1 < rPoints.length && ys[ilPoint] - ys[rPoints[rpoint1]] > mindistance)
              rpoint1++;
            while (rpoint2 < rPoints.length && ys[rPoints[rpoint2]] - ys[ilPoint] < mindistance)
              rpoint2++;
            for (int rpoint = rpoint1; rpoint < rpoint2; rpoint++) {
              double distance = Math.abs(xs[ilPoint] - xs[rpoint]) + Math.abs(ys[ilPoint] - ys[rpoint]);
              if (distance < minDistanceLR) {
                minPointL = ilPoint;
                minPointR = rPoints[rpoint];
                minDistanceLR = distance;
              }
            }
          }
        }

        if (minDistanceLR < mindistance) {
          // The closest pair is in the middle (between list1 and list2)
          merged.distance = minDistanceLR;
          merged.p1 = minPointL;
          merged.p2 = minPointR;
        } else if (list1.distance < list2.distance) {
          // The closest pair is in list1
          merged.distance = list1.distance;
          merged.p1 = list1.p1;
          merged.p2 = list1.p2;
        } else {
          // The closest pair is in list2
          merged.distance = list2.distance;
          merged.p1 = list2.p1;
          merged.p2 = list2.p2;
        }

        newSublists.add(merged);
      }
      sublists = newSublists;
    }

    int p1 = sublists.get(0).p1;
    int p2 = sublists.get(0).p2;
    return Math.abs(xs[p1] - xs[p2]) + Math.abs(ys[p1] - ys[p2]);
  }

  /**
   * Exponential search on the first point with x-coordinate larger than the given xmin.
   * @param xs
   * @param bound2
   * @param xmin
   * @return
   */
  static int exponentialSearchLeft(int[] xs, int bound2, double xmin) {
    int size = 1;
    while (bound2 - size > 0 && xs[bound2 - size] > xmin)
      size *= 2;
    int bound1 = Math.max(0, bound2 - size);
    // Binary search in the given boundary
    while (bound1 < bound2) {
      int m = (bound1 + bound2) / 2;
      if (xs[m] >= xmin)
        bound2 = m;
      else
        bound1 = m + 1;
    }
    return bound1;
  }

  /**
   * Exponential search on the first point with x-coordinate less than the given xmax.
   * @param xs Array of all points
   * @param bound1 The first item to start the search
   * @param xmax The value of x to search for
   * @return the index of the result in the array
   */
  static int exponentialSearchRight(int[] xs, int bound1, double xmax) {
    int size = 1;
    while (bound1 + size <= xs.length && xs[bound1 + size - 1] > xmax)
      size *= 2;
    int bound2 = Math.min(xs.length, bound1 + size);
    // Binary search in the given boundary
    while (bound1 < bound2) {
      int m = (bound1 + bound2) / 2;
      if (xs[m] >= xmax)
        bound2 = m;
      else
        bound1 = m + 1;
    }
    return bound1;
  }
}
