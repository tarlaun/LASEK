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
import junit.framework.TestCase;
import org.apache.hadoop.util.QuickSort;

public class HCurveSortableTest extends TestCase {
  public void testMultidimensionalAllPoints() {
    for (int numDimensions = 1; numDimensions <= 5; numDimensions++) {
      for (int levels = 1; levels <= 4; levels++) {
        int valueRange = 1 << levels;
        int numPoints = (int) Math.pow(valueRange, numDimensions);
        double[][] points = new double[numDimensions][numPoints];
        for (int i = 0; i < numPoints; i++) {
          int x = i;
          for (int d = 0; d < numDimensions; d++) {
            points[d][i] = x % valueRange;
            x /= valueRange;
          }
        }
        EnvelopeNDLite mbb = new EnvelopeNDLite(numDimensions);
        for (int d = 0; d < numDimensions; d++) {
          mbb.setMinCoord(d, 0);
          mbb.setMaxCoord(d, valueRange);
        }
        HCurveSortable sorter = new HCurveSortable(mbb);
        sorter.setPoints(points);
        new QuickSort().sort(sorter, 0, numPoints);
        //printPoints(points);
        for (int i = 1; i < numPoints; i++) {
          double distance = 0.0;
          for (int d = 0; d < numDimensions; d++) {
            distance += Math.abs(points[d][i - 1] - points[d][i]);
          }
          assertEquals(String.format("Distance between points #%d and #%d is more than 1.0", i-1, i),
              1.0, distance, 1E-3);
        }
      }
    }
  }

  public void testMultidimensionalRandomPoints() {
    // This test generates random points and sorts them using HilbertCurve sorter
    // Since these are random points, it is hard to verify the order but we just
    // make sure that the code will not throw an exception and non of the assertions in the code will fail.
    for (int numDimensions = 1; numDimensions <= 10; numDimensions++) {
      int numPoints = 1000;
      double[][] points = new double[numDimensions][numPoints];
      for (int i = 0; i < numPoints; i++) {
        for (int d = 0; d < numDimensions; d++) {
          points[d][i] = Math.random();
        }
      }
      EnvelopeNDLite mbb = new EnvelopeNDLite(numDimensions);
      for (int d = 0; d < numDimensions; d++) {
        mbb.setMinCoord(d, 0);
        mbb.setMaxCoord(d, 1.0);
      }
      HCurveSortable sorter = new HCurveSortable(mbb);
      sorter.setPoints(points);
      new QuickSort().sort(sorter, 0, numPoints);
    }
  }

  /**
   * Print points for debugging purposes
   * @param points points to print
   */
  private void printPoints(double[][] points) {
    for (int i = 0; i < points[0].length; i++) {
      System.out.printf("#%03d - ", i);
      for (int d = 0; d < points.length; d++) {
        if (d > 0)
          System.out.print(" ");
        System.out.print(points[d][i]);
      }
      System.out.println();
    }
  }
}