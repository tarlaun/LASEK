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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import edu.ucr.cs.bdlab.beast.util.IntArray;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class RStarTreeTest extends JavaSpatialSparkTest {

  public void testBuild() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RStarTree(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 22);
    int maxNumOfNodes = 6;
    int minNumOfNodes = 4;
    assertTrue(String.format("Too few nodes %d<%d",rtree.numOfNodes(), minNumOfNodes),
            rtree.numOfNodes() >= minNumOfNodes);
    assertTrue(String.format("Too many nodes %d>%d", rtree.numOfNodes(), maxNumOfNodes),
            rtree.numOfNodes() <= maxNumOfNodes);
    assertEquals(1, rtree.getHeight());
  }

  public void testBuild2() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName, 19);
    RStarTree rtree = new RStarTree(2, 8);
    rtree.initializeFromPoints(points);

    Rectangle2D.Double[] expectedLeaves = {
            new Rectangle2D.Double(1, 2, 13, 1),
            new Rectangle2D.Double(3, 6, 3, 6),
            new Rectangle2D.Double(9, 6, 10, 2),
            new Rectangle2D.Double(12, 10, 3, 2),
    };
    int numFound = 0;
    for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
      for (int i = 0; i < expectedLeaves.length; i++) {
        if (expectedLeaves[i] != null && expectedLeaves[i].equals(
                new Rectangle2D.Double(leaf.min[0], leaf.min[1], leaf.max[0]-leaf.min[0], leaf.max[1]-leaf.min[1]))) {
          numFound++;
        }
      }
    }
    assertEquals(expectedLeaves.length, numFound);
  }

  public void testBuild111() {
    String fileName = "/test111.points";
    double[][] points = readCoordsResource(fileName);
    RStarTree rtree = new RStarTree(6, 20);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 111);

    int numLeaves = 0;

    for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
      numLeaves++;
    }
    assertEquals(9, numLeaves);
  }

  public void testSplit() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    RTreeGuttman rtree = new RStarTree(22, 44);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 22);
    // Perform one split at the root
    rtree.split(rtree.root, 4);

    Iterable<RTreeGuttman.Node> leaves = rtree.getAllLeaves();
    int numOfLeaves = 0;
    for (RTreeGuttman.Node leaf : leaves)
      numOfLeaves++;
    assertEquals(2, numOfLeaves);
  }

  public void testPartitionPoints() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 8;
    EnvelopeNDLite[] partitions =
            RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, false, 0, null);
    // Minimum number of partitions = Ceil(# points / capacity)
    int minNumPartitions = (points[0].length + capacity - 1) / capacity;
    int maxNumPartitions = (points[0].length + capacity / 2 - 1) / (capacity / 2);
    assertTrue("Too many partitions " + partitions.length,
            partitions.length <= maxNumPartitions);
    assertTrue("Too few partitions " + partitions.length,
            partitions.length >= minNumPartitions);
    // Make sure the MBR of all partitions cover the input space
    EnvelopeNDLite mbrAllPartitions = (EnvelopeNDLite) partitions[0];
    for (EnvelopeNDLite leaf : partitions) {
      mbrAllPartitions.merge(leaf);
    }
    assertEquals(new EnvelopeNDLite(2, 1, 2, 22, 12), mbrAllPartitions);
  }

  public void testPartition1() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 8;
    EnvelopeNDLite[] partitions =
            RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, false, 0, null);

    assertEquals(2, partitions.length);
    Arrays.sort(partitions, Comparator.comparing(e -> e.getMinCoord(0)));
    assertEquals(new EnvelopeNDLite(2, 1, 3, 6, 12), partitions[0]);
    assertEquals(new EnvelopeNDLite(2, 9, 2, 12, 10), partitions[1]);
  }

  public void testAuxiliarySearchStructure() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 4;
    AuxiliarySearchStructure aux = new AuxiliarySearchStructure();
    EnvelopeNDLite[] partitions = RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, true, 0, aux);
    assertEquals(3, aux.partitionGreaterThanOrEqual.length);
    assertEquals(9.0, aux.splitCoords[aux.rootSplit]);
    assertTrue(aux.partitionGreaterThanOrEqual[aux.rootSplit] < 0);
    assertTrue(aux.partitionLessThan[aux.rootSplit] >= 0);
    int p1 = aux.search(new double[] {5,5});
    EnvelopeNDLite expectedMBR = new EnvelopeNDLite(2, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 9, 6);
    assertEquals(expectedMBR, partitions[p1]);
    p1 = aux.search(new double[] {10,0});
    expectedMBR = new EnvelopeNDLite(2,9, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    assertEquals(expectedMBR, partitions[p1]);
    // Search a rectangle that matches one partition
    IntArray ps = new IntArray();
    aux.search(new EnvelopeNDLite(2, 10, 0, 15, 15), ps);
    assertEquals(1, ps.size());
    // Make sure that it returns the same ID returned by the point search
    p1 = aux.search(new double[] {10, 0});
    assertEquals(p1, ps.peek());
    // Search a rectangle that machines two partitions
    aux.search(new EnvelopeNDLite(2, 0, 0, 5, 7), ps);
    assertEquals(2, ps.size());
  }

  public void testAuxiliarySearchStructureWithBiggerIndex() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 3;
    AuxiliarySearchStructure aux = new AuxiliarySearchStructure();
    EnvelopeNDLite[] partitions = RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, true, 0, aux);
    IntArray ps = new IntArray();
    // Make a search that should match with all paritions
    aux.search(new EnvelopeNDLite(2, 0, 0, 100, 100), ps);
    assertEquals(partitions.length, ps.size());
  }


  public void testAuxiliarySearchStructureWithOnePartition() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 100;
    AuxiliarySearchStructure aux = new AuxiliarySearchStructure();
    EnvelopeNDLite[] partitions = RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, true, 0, aux);
    int p = aux.search(new double[] {0, 0});
    assertEquals(0, p);
    IntArray ps = new IntArray();
    aux.search(new EnvelopeNDLite(2, 0, 0, 5, 5), ps);
    assertEquals(1, ps.size());
    assertEquals(0, ps.get(0));
  }

  public void testPartitionInfinity() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree without splits
    int capacity = 4;
    EnvelopeNDLite[] partitions =
            RStarTree.partitionPoints(new double[][]{points[0], points[1]}, capacity/2, capacity, true, 0, null);

    assertEquals(4, partitions.length);

    // The MBR of all partitions should cover the entire space
    EnvelopeNDLite mbrAllPartitions = new EnvelopeNDLite(2, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    for (EnvelopeNDLite partition : partitions)
      mbrAllPartitions.merge(partition);
    assertEquals(new EnvelopeNDLite(2, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), mbrAllPartitions);

    // The partitions should not be overlapping
    for (int i = 0; i < partitions.length; i++) {
      for (int j = i + 1; j < partitions.length; j++) {
        assertFalse(String.format("Partitions %s and %s are overlapped",
                partitions[i], partitions[j]), partitions[i].intersectsEnvelope(partitions[j]));
      }
    }
  }

  public void testPartitionWeightedPoints() throws IOException {
    double[][] coords = readCoordsResource("/weighted-sample-1000.txt");
    int numPoints = coords[0].length;
    long[] weights = new long[numPoints];
    long totalWeight = 0;
    for (int $i = 0; $i < numPoints; $i++)
      totalWeight += weights[$i] = (long) coords[2][$i];

    int numPartitions = 10;
    long m = (long) (totalWeight / numPartitions * 0.95);
    long M = (long) (totalWeight / numPartitions * 1.05);
    // Notice: No explicit assertions are added but we rely on the assertions in the function that we call
    RStarTree.partitionWeightedPoints(new double[][]{coords[0], coords[1]}, weights, m, M, false, 0.0, null);
  }

  public void testPartitionWeightedPointsWithCorrection() {
    int numPoints = 2;
    double[][] coords = new double[2][numPoints];
    coords[0][0] = 1;
    coords[1][0] = 1;
    coords[0][1] = 3;
    coords[1][1] = 2;
    long[] weights = {5, 24};

    long m = 9;
    long M = 10;
    // Notice: No explicit assertions are added but we rely on the assertions in the function that we call
    RStarTree.partitionWeightedPoints(coords, weights, m, M, false, 0.0, null);
  }


}
