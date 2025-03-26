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
package edu.ucr.cs.bdlab.beast.cg;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * A utility class that contains implementations of spatial join algorithms.
 */
public class SpatialJoinAlgorithms {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(SpatialJoinAlgorithms.class);

  /**Enumerated type for the distributed spatial join algorithms*/
  public enum ESJDistributedAlgorithm {BNLJ, PBSM, SJMR, DJ, REPJ, SJ1D}

  /**Enumerated type for the spatial join predicate*/
  public enum ESJPredicate {Intersects, MBRIntersects, Contains}

  public interface SJResult {
    void accept(int i, int j, double[] refPoint);
  }

  /**
   * Computes the spatial join between two sets of rectangles and reports all overlapping pairs of indexes.
   *
   * @param minCoord1 an array of coordinates for the lower corners of the first set of rectangles
   * @param maxCoord1 an array of coordinates for the upper corners of the first set of rectangles
   * @param minCoord2 an array of coordinates for the lower corners of the second set of rectangles
   * @param maxCoord2 an array of coordinates for the upper corners of the second set of rectangles
   * @param results the output is reported by calling this function with the indexes of the two rectangles that
   *                overlap. If {@code null}, results are not reported; only the total count is.
   * @param mbrTests an accumulator that counts number of MBR tests done while running the plane-sweep algorithm
   * @return the total number of results found.
   */
  public static int planeSweepRectangles(double[][] minCoord1, double[][] maxCoord1,
                                         double[][] minCoord2, double[][] maxCoord2,
                                         SJResult results, LongAccumulator mbrTests) {
    long t1 = System.nanoTime();
    assert minCoord1.length == maxCoord1.length;
    assert minCoord1.length == minCoord2.length;
    assert minCoord1.length == maxCoord2.length;
    int count = 0;
    int numDimensions = minCoord1.length;
    int n1 = minCoord1[0].length;
    int n2 = minCoord2[0].length;
    LOG.debug(String.format("Running the plane sweep join algorithm between lists sizes of %d and %d", n1, n2));
    // Generate indexes to report the answer
    int[] indexes1 = new int[n1];
    int[] indexes2 = new int[n2];
    for (int $i = 0; $i < n1; $i++)
      indexes1[$i] = $i;
    for (int $i = 0; $i < n2; $i++)
      indexes2[$i] = $i;
    long localMBRTests = 0;
    // Sort by the first coordinate and keep track of the array index for each entry for reporting
    IndexedSortable sortable1 = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(minCoord1[0][i] - minCoord1[0][j]);
      }

      @Override
      public void swap(int i, int j) {
        if (i == j)
          return;
        // Swap indexes
        indexes1[i] ^= indexes1[j];
        indexes1[j] ^= indexes1[i];
        indexes1[i] ^= indexes1[j];
        // Swap coordinates
        for (int $d = 0; $d < numDimensions; $d++) {
          double t = minCoord1[$d][i];
          minCoord1[$d][i] = minCoord1[$d][j];
          minCoord1[$d][j] = t;
          t = maxCoord1[$d][i];
          maxCoord1[$d][i] = maxCoord1[$d][j];
          maxCoord1[$d][j] = t;
        }
      }
    };
    IndexedSortable sortable2 = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(minCoord2[0][i] - minCoord2[0][j]);
      }

      @Override
      public void swap(int i, int j) {
        if (i == j)
          return;
        // Swap indexes
        indexes2[i] ^= indexes2[j];
        indexes2[j] ^= indexes2[i];
        indexes2[i] ^= indexes2[j];
        // Swap coordinates
        for (int $d = 0; $d < numDimensions; $d++) {
          double t = minCoord2[$d][i];
          minCoord2[$d][i] = minCoord2[$d][j];
          minCoord2[$d][j] = t;
          t = maxCoord2[$d][i];
          maxCoord2[$d][i] = maxCoord2[$d][j];
          maxCoord2[$d][j] = t;
        }
      }
    };
    QuickSort sorter = new QuickSort();
    sorter.sort(sortable1, 0, n1);
    sorter.sort(sortable2, 0, n2);

    // Now, run the planesweep algorithm
    int lastReportedProgress = 0;
    long lastReportedProgressTime = System.currentTimeMillis();
    int i = 0, j = 0;
    double[] refPoint = new double[numDimensions];
    while (i < n1 && j < n2) {
      if (minCoord1[0][i] < minCoord2[0][j]) {
        // R1[i] is the left-most rectangle. Activate it and compare to all rectangles R2 until passing the right end
        // of R1[i]
        int jj = j;
        while (jj < n2 && minCoord2[0][jj] < maxCoord1[0][i]) {
          localMBRTests++;
          // Compare the two rectangles R1[i] and R2[jj] and report if needed
          if (rectanglesOverlap(minCoord1, maxCoord1, i, minCoord2, maxCoord2, jj)) {
            // Found an overlap
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++)
                refPoint[d] = Math.max(minCoord1[d][i], minCoord2[d][jj]);
              results.accept(indexes1[i], indexes2[jj], refPoint);
            }
          }
          jj++;
        }
        // Skip until the first record that might produce a result from i
        do {
          i++;
        } while (i < n1 && maxCoord1[0][i] < minCoord2[0][j]);
      } else {
        // R2[j] is the left-most rectangle. Activate it and compare to all rectangles of R1 until passing the right
        // end of R2[j]
        int ii = i;
        while (ii < n1 && minCoord1[0][ii] < maxCoord2[0][j]) {
          // Compare the two rectangles R1[ii] and R2[j] and report if needed
          localMBRTests++;
          if (rectanglesOverlap(minCoord1, maxCoord1, ii, minCoord2, maxCoord2, j)) {
            // Found an overlap
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++)
                refPoint[d] = Math.max(minCoord1[d][ii], minCoord2[d][j]);
              results.accept(indexes1[ii], indexes2[j], refPoint);
            }
          }
          ii++;
        }
        // Skip until the first record that might produce a result from j
        do {
          j++;
        } while (j < n2 && maxCoord2[0][j] < minCoord1[0][i]);
      }
      if (System.currentTimeMillis() - lastReportedProgressTime > 60000) {
        // Report the result every 60 seconds if there is at least 1% change
        int progress = Math.min(100, (i * 10 / n1) * 10 + (j * n2 / 10));
        if (progress != lastReportedProgress) {
          lastReportedProgress = progress;
          LOG.info(String.format("Spatial join progress %d%% (i=%d/%d, j=%d/%d)", lastReportedProgress, i, n1, j, n2));
        }
        lastReportedProgressTime = System.currentTimeMillis();
      }
    }
    if (mbrTests != null)
      mbrTests.accumulate(localMBRTests);
    long t2 = System.nanoTime();
    return count;
  }

  /**
   * Test if two rectangles overlap R1[i] and R2[j].
   * @param minCoord1 the lower corners of the rectangles in the first set
   * @param maxCoord1 the upper corners of the rectangles in the first set
   * @param i the index of the first rectangle
   * @param minCoord2 the lower corners of the rectangles in the second set
   * @param maxCoord2 the upper corners of the rectangles in the second set
   * @param j the index of the second rectangle
   * @return {@code true} if the rectangles overlap, i.e., not disjoint, and false if they are disjoint
   */
  static final boolean rectanglesOverlap(double[][] minCoord1, double[][] maxCoord1, int i,
                                   double[][] minCoord2, double[][] maxCoord2, int j) {
    assert minCoord1.length == 2;
    return !(minCoord1[0][i] >= maxCoord2[0][j] || minCoord2[0][j] >= maxCoord1[0][i] ||
        minCoord1[1][i] >= maxCoord2[1][j] || minCoord2[1][j] >= maxCoord1[1][i]);
  }

  /**
   * Runs a spatial join of the given lists of geometries through a plan-sweep filter-refine approach.
   * @param r a list of geometries (left)
   * @param s a list of geometries (right)
   * @param results a collector that is notified with the output
   * @param mbrTests an accumulator that counts the number of MBR tests
   * @return the number of results found
   */
  public static int spatialJoinIntersectsPlaneSweep(List<Geometry> r, List<Geometry> s, SJResult results,
                                                    LongAccumulator mbrTests) {
    if (r.size() == 0 || s.size() == 0)
      return 0;
    assert GeometryHelper.getCoordinateDimension(r.get(0)) == 2 : "R should be 2D geometries";
    assert GeometryHelper.getCoordinateDimension(s.get(0)) == 2 : "S should be 2D geometries";
    double[][] coordsR = getMBRs(r);
    double[][] coordsS = getMBRs(s);
    return planeSweepRectangles(new double[][]{coordsR[0], coordsR[1]}, new double[][]{coordsR[2], coordsR[3]},
        new double[][]{coordsS[0], coordsS[1]}, new double[][]{coordsS[2], coordsS[3]}, (i, j, refPoint) -> {
          // Run the refine step
          if (r.get(i).intersects((s.get(j))))
            results.accept(i, j, refPoint);
        }, mbrTests);
  }

  /**
   * Computes the minimum bounding rectangles of all given geometries as primitive arrays.
   * The returned 2D array stores the minimum and maximum coordinate along each dimension.
   * @param geometries the list of geometries to get their MBRs
   * @return the computed coordinates
   */
  public static double[][] getMBRs(List<Geometry> geometries) {
    int numDimensions = GeometryHelper.getCoordinateDimension(geometries.get(0));
    double[][] coords = new double[numDimensions*2][geometries.size()];
    EnvelopeND mbr = new EnvelopeND(geometries.get(0).getFactory(), numDimensions);
    for (int iGeom = 0; iGeom < geometries.size(); iGeom++) {
      mbr.setEmpty();
      mbr.merge(geometries.get(iGeom));
      for (int d = 0; d < numDimensions; d++) {
        coords[d][iGeom] = mbr.getMinCoord(d);
        coords[d + numDimensions][iGeom] = mbr.getMaxCoord(d);
      }
    }
    return coords;
  }

  /**
   * Computes the minimum bounding rectangles of all given geometries as primitive arrays.
   * The returned 2D array stores the minimum and maximum coordinate along each dimension.
   * @param geometries the list of features to get their MBRs
   * @return the coordinates of the MBRs
   */
  public static double[][] getMBRs2(List<IFeature> geometries) {
    int numDimensions = GeometryHelper.getCoordinateDimension(geometries.get(0).getGeometry());
    double[][] coords = new double[numDimensions*2][geometries.size()];
    EnvelopeND mbr = new EnvelopeND(geometries.get(0).getGeometry().getFactory(), numDimensions);
    for (int iGeom = 0; iGeom < geometries.size(); iGeom++) {
      mbr.setEmpty();
      mbr.merge(geometries.get(iGeom).getGeometry());
      for (int d = 0; d < numDimensions; d++) {
        coords[d][iGeom] = mbr.getMinCoord(d);
        coords[d + numDimensions][iGeom] = mbr.getMaxCoord(d);
      }
    }
    return coords;
  }

  /**
   * Computes the overlap join between the two given lists of geometries. Each geometry is simplified using the given
   * threshold. Any geometry that contains more than the threshold points will be simplified by partitioning it
   * into smaller geometries.
   * @param r the left set of geometries
   * @param s the right set of geometries
   * @param threshold the threshold for simplification in terms of number of points per geometry
   * @param results a collector for the results
   * @param mbrTests an accumulator for counting the number of MBR tests
   * @return the number of results found
   */
  public static int spatialJoinIntersectsWithSimplification(List<Geometry> r, List<Geometry> s, int threshold,
                                                            SJResult results, LongAccumulator mbrTests) {
    GeometryFactory geometryFactory = r.get(0).getFactory();
    // A set of reported results to run duplicate elimination
    Set<Long> reportedPairs = new HashSet<>();
    // Run quad split for both inputs
    List<Geometry> simplifiedR = new ArrayList<>();
    // Map each part to its main
    IntArray mappingR = new IntArray();
    for (int iGeom = 0; iGeom < r.size(); iGeom++) {
      List<Geometry> parts = quadSplit(r.get(iGeom), threshold);
      simplifiedR.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingR.add(iGeom);
    }
    // Simplify the second dataset (s)
    List<Geometry> simplifiedS = new ArrayList<>();
    IntArray mappingS = new IntArray();
    for (int iGeom = 0; iGeom < s.size(); iGeom++) {
      List<Geometry> parts = quadSplit(s.get(iGeom), threshold);
      simplifiedS.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingS.add(iGeom);
    }
    // Find overlaps between the simplified lists
    final EnvelopeND mbr1 = new EnvelopeND(geometryFactory, 2);
    final EnvelopeND mbr2 = new EnvelopeND(geometryFactory, 2);
    spatialJoinIntersectsPlaneSweep(simplifiedR, simplifiedS, (i, j, refPoint) -> {
      // Retrieve the indexes of the original geometries
      i = mappingR.get(i);
      j = mappingS.get(j);
      long pair = (i << 32) | j;
      if (!reportedPairs.contains(pair)) {
        // First time to find this pair, report it and add it to the list of reported pairs
        reportedPairs.add(pair);
        if (results != null) {
          // Compute the correct reference point based on the original polygons
          mbr1.setEmpty();
          mbr1.merge(r.get(i));
          mbr2.setEmpty();
          mbr2.merge(s.get(j));
          refPoint[0] = Math.max(mbr1.getMinCoord(0), mbr2.getMinCoord(0));
          refPoint[1] = Math.max(mbr1.getMinCoord(1), mbr2.getMinCoord(1));
          results.accept(i, j, refPoint);
        }
      }
    }, mbrTests);

    return reportedPairs.size();
  }

  /**
   * Computes the overlap join between the two given lists of geometries. Each geometry is simplified using the given
   * threshold. Any geometry that contains more than the threshold points will be simplified by partitioning it
   * into smaller geometries.
   * @param r the list of the features in the left set
   * @param s the list of the features in the right set
   * @param threshold the simplification threshold in terms of number of points per geometry
   * @param results a collector for the results
   * @param mbrTests an accumulator for counting the number of MBR tests
   * @return the number of results found
   */
  public static int spatialJoinIntersectsWithSimplification2(List<IFeature> r, List<IFeature> s, int threshold,
                                                            SJResult results, LongAccumulator mbrTests) {
    GeometryFactory geometryFactory = r.get(0).getGeometry().getFactory();
    // A set of reported results to run duplicate elimination
    Set<Long> reportedPairs = new HashSet<>();
    // Run quad split for both inputs
    List<Geometry> simplifiedR = new ArrayList<>();
    // Map each part to its main
    IntArray mappingR = new IntArray();
    for (int iGeom = 0; iGeom < r.size(); iGeom++) {
      List<Geometry> parts = quadSplit(r.get(iGeom).getGeometry(), threshold);
      simplifiedR.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingR.add(iGeom);
    }
    // Simplify the second dataset (s)
    List<Geometry> simplifiedS = new ArrayList<>();
    IntArray mappingS = new IntArray();
    for (int iGeom = 0; iGeom < s.size(); iGeom++) {
      List<Geometry> parts = quadSplit(s.get(iGeom).getGeometry(), threshold);
      simplifiedS.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingS.add(iGeom);
    }
    // Find overlaps between the simplified lists
    final EnvelopeND mbr1 = new EnvelopeND(geometryFactory, 2);
    final EnvelopeND mbr2 = new EnvelopeND(geometryFactory, 2);
    spatialJoinIntersectsPlaneSweep(simplifiedR, simplifiedS, (i, j, refPoint) -> {
      // Retrieve the indexes of the original geometries
      i = mappingR.get(i);
      j = mappingS.get(j);
      long pair = (i << 32) | j;
      if (!reportedPairs.contains(pair)) {
        // First time to find this pair, report it and add it to the list of reported pairs
        reportedPairs.add(pair);
        if (results != null) {
          // Compute the correct reference point based on the original polygons
          mbr1.setEmpty();
          mbr1.merge(r.get(i).getGeometry());
          mbr2.setEmpty();
          mbr2.merge(s.get(j).getGeometry());
          refPoint[0] = Math.max(mbr1.getMinCoord(0), mbr2.getMinCoord(0));
          refPoint[1] = Math.max(mbr1.getMinCoord(1), mbr2.getMinCoord(1));
          results.accept(i, j, refPoint);
        }
      }
    }, mbrTests);

    return reportedPairs.size();
  }

  private static Polygon createRectangle(double minx, double miny, double maxx, double maxy, GeometryFactory f) {
    CoordinateSequence cs = f.getCoordinateSequenceFactory().create(5, 2);
    cs.setOrdinate(0, 0, minx);
    cs.setOrdinate(0, 1, miny);
    cs.setOrdinate(1, 0, maxx);
    cs.setOrdinate(1, 1, miny);
    cs.setOrdinate(2, 0, maxx);
    cs.setOrdinate(2, 1, maxy);
    cs.setOrdinate(3, 0, minx);
    cs.setOrdinate(3, 1, maxy);
    cs.setOrdinate(4, 0, minx);
    cs.setOrdinate(4, 1, miny);
    return f.createPolygon(cs);
  }

  /**
   * Simplifies a polygon by recursively splitting it into quadrants until each part has no more than {@code threshold}
   * points.
   * @param geometry the list of geometries to split
   * @param threshold the simplification threshold in terms of number of points
   * @return the list of simplified geometries for the given geometry
   */
  public static List<Geometry> quadSplit(Geometry geometry, int threshold) {
    assert GeometryHelper.getCoordinateDimension(geometry) == 2 : "This function only works with 2D geometries";
    Geometry esrGeometry = (geometry);
    List<Geometry> parts = new ArrayList<>();
    int numGeomsToCheck = 1;
    parts.add(esrGeometry);
    // Convert the threshold to an estimated memory size for simplicity to use with Esri API
    while (numGeomsToCheck > 0) {
      Geometry geomToCheck = parts.remove(0);
      numGeomsToCheck--;
      if (geomToCheck.getNumPoints() <= threshold) {
        // Already simple. Add to results
        parts.add(geomToCheck);
      } else {
        // A complex geometry, split into four
        Envelope mbr = geomToCheck.getEnvelopeInternal();
        double centerx = (mbr.getMinX() + mbr.getMaxX()) / 2.0;
        double centery = (mbr.getMinY() + mbr.getMaxY()) / 2.0;
        // First quadrant
        Polygon quadrant = createRectangle(mbr.getMinX(), mbr.getMinY(), centerx, centery, geomToCheck.getFactory());
        parts.add(geomToCheck.intersection(quadrant));
        // Second quadrant
        quadrant = createRectangle(centerx, mbr.getMinY(), mbr.getMaxX(), centery, geomToCheck.getFactory());
        parts.add(geomToCheck.intersection(quadrant));
        // Third quadrant
        quadrant = createRectangle(centerx, centery, mbr.getMaxX(), mbr.getMaxY(), geomToCheck.getFactory());
        parts.add(geomToCheck.intersection(quadrant));
        // Fourth quadrant
        quadrant = createRectangle(mbr.getMinX(), centery, centerx, mbr.getMaxY(), geomToCheck.getFactory());
        parts.add(geomToCheck.intersection(quadrant));
        numGeomsToCheck += 4;
      }
    }
    // Convert all parts back to lite geometry
    List<Geometry> results = new ArrayList<>();
    for (int i = 0; i < parts.size(); i++) {
      results.add((parts.get(i)));
    }
    return results;
  }

}
