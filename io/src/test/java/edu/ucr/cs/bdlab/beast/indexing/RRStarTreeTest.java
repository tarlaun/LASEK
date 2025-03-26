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
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.awt.geom.Rectangle2D;
import java.util.Random;

public class RRStarTreeTest extends JavaSpatialSparkTest {

  public void testDOvlp() {
    Rectangle2D.Double[] entries = {
        new Rectangle2D.Double(0, 0, 3, 2),
        new Rectangle2D.Double(2, 1, 2, 3),
        new Rectangle2D.Double(3, 3, 1, 1),
    };
    RRStarTree rrStarTree = createRRStarWithDataEntries(entries);
    int t = rrStarTree.Node_createNodeWithChildren(true, 0);
    int j = rrStarTree.Node_createNodeWithChildren(true, 1);
    assertEquals(5.0, rrStarTree.Node_perimeter(t));
    assertEquals(5.0, rrStarTree.Node_perimeter(j));
    int o = 2;
    double dOvlpPerim = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.PERIMETER);
    assertEquals(3.0, dOvlpPerim);
    double dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(5.0, dOvlpVol);

    // Test when t and j are disjoint
    entries[0] = new Rectangle2D.Double(0, 0, 2, 1);
    rrStarTree = createRRStarWithDataEntries(entries);
    t = rrStarTree.Node_createNodeWithChildren(true, 0);
    j = rrStarTree.Node_createNodeWithChildren(true, 2);
    o = 1;
    dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(1.0, dOvlpVol);

    // Test when (t U o) and j are disjoint
    entries[2] = new Rectangle2D.Double(5, 5, 1, 1);
    rrStarTree = createRRStarWithDataEntries(entries);
    t = rrStarTree.Node_createNodeWithChildren(true, 0);
    j = rrStarTree.Node_createNodeWithChildren(true, 2);
    o = 1;
    dOvlpVol = rrStarTree.dOvlp(t, o, j, RRStarTree.AggregateFunction.VOLUME);
    assertEquals(0.0, dOvlpVol);
  }

  public void testCovers() {
    Rectangle2D.Double[] entries = {
        new Rectangle2D.Double(0, 0, 3, 2),
        new Rectangle2D.Double(2, 1, 2, 3),
        new Rectangle2D.Double(3, 3, 1, 1),
    };
    RRStarTree rrStarTree = createRRStarWithDataEntries(entries);
    int n1 = rrStarTree.Node_createNodeWithChildren(true, 0);
    int n2 = rrStarTree.Node_createNodeWithChildren(true, 1);
    assertFalse(rrStarTree.Node_contains(n1, 2));
    assertTrue(rrStarTree.Node_contains(n2, 2));
  }

  public void testInsertion() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    double[] xs = points[0], ys = points[1];
    // Case 1: COV is not empty, choose the node with zero volume
    RRStarTree rtree = new RRStarTree(4, 8);
    rtree.initializeDataEntries(xs, ys);
    int n1 = rtree.Node_createNodeWithChildren(true, 1, 7);
    int n2 = rtree.Node_createNodeWithChildren(true, 5, 6);
    rtree.root = rtree.Node_createNodeWithChildren(false, n1, n2);
    // n1 and n2 cover p4 but n1 has a zero volume, p4 should be added to n1
    rtree.insertAnExistingDataEntry(4);
    assertEquals(3, rtree.Node_size(n1));
    assertEquals(2, rtree.Node_size(n2));

    // Case 2: COV is not empty, multiple nodes with zero volume, choose the one with min perimeter
    rtree = new RRStarTree(4, 8);
    xs[6] = 5; ys[6] = 7;
    rtree.initializeDataEntries(xs, ys);
    n1 = rtree.Node_createNodeWithChildren(true, 1, 7);
    n2 = rtree.Node_createNodeWithChildren(true, 5, 6);
    rtree.root = rtree.Node_createNodeWithChildren(false, n1, n2);
    // Both n1 and n2 cover p4 with zero volume, but n2 has a smaller perimeter
    rtree.insertAnExistingDataEntry(4);
    assertEquals(2, rtree.Node_size(n1));
    assertEquals(3, rtree.Node_size(n2));

    // Case 3: COV is not empty, all have non-zero volume, choose the one with min volume
    rtree = new RRStarTree(4, 8);
    points = readCoordsResource(fileName); // Reload the file
    xs = points[0]; ys = points[1];
    rtree.initializeDataEntries(xs, ys);
    n1 = rtree.Node_createNodeWithChildren(true, 3, 9);
    n2 = rtree.Node_createNodeWithChildren(true, 2, 10);
    rtree.root = rtree.Node_createNodeWithChildren(false, n1, n2);
    // Both n1 and n2 cover p4 but n1 has a smaller volume
    rtree.insertAnExistingDataEntry(4);
    assertEquals(3, rtree.Node_size(n1));
    assertEquals(2, rtree.Node_size(n2));

    // Case 4: Some node has to be expanded. We add it to the node with smallest
    // deltaPerim if this would not increase the perimetric overlap
    rtree = new RRStarTree(4, 8);
    points = readCoordsResource(fileName); // Reload the file
    xs = points[0]; ys = points[1];
    rtree.initializeDataEntries(xs, ys);
    n1 = rtree.Node_createNodeWithChildren(true, 4, 6);
    n2 = rtree.Node_createNodeWithChildren(true, 7, 10);
    int n3 = rtree.Node_createNodeWithChildren(true, 2, 8);
    rtree.root = rtree.Node_createNodeWithChildren(false, n1, n2, n3);
    rtree.insertAnExistingDataEntry(1);
    assertEquals(3, rtree.Node_size(n1));
    assertEquals(2, rtree.Node_size(n2));
    assertEquals(2, rtree.Node_size(n3));

    // Case 5: Recreate the same example in Figure 1 on page 802 in the paper
    double[] x1s = new double[7];
    double[] y1s = new double[7];
    double[] x2s = new double[7];
    double[] y2s = new double[7];
    x1s[1] = 0.6; y1s[1] = 0.6; x2s[1] = 3.4; y2s[1] = 2.8; // E1
    x1s[2] = 0; y1s[2] = 0; x2s[2] = 2.9; y2s[2] = 2.4; // E2
    x1s[3] = 1.4; y1s[3] = 2.7; x2s[3] = 2.3; y2s[3] = 3.8; // E3
    x1s[4] = 3.5; y1s[4] = 0.5; x2s[4] = 4.6; y2s[4] = 1.3; // E4
    x1s[5] = 5.4; y1s[5] = 2.9; x2s[5] = 6.2; y2s[5] = 3.6; // E5
    x1s[6] = 3.1; y1s[6] = 2.5; x2s[6] = 4; y2s[6] = 3.1; // Omega
    rtree = new RRStarTree(4, 8);
    rtree.initializeDataEntries(x1s, y1s, x2s, y2s);
    int e1 = rtree.Node_createNodeWithChildren(true, 1);
    int e2 = rtree.Node_createNodeWithChildren(true, 2);
    int e3 = rtree.Node_createNodeWithChildren(true, 3);
    int e4 = rtree.Node_createNodeWithChildren(true, 4);
    int e5 = rtree.Node_createNodeWithChildren(true, 5);
    rtree.root = rtree.Node_createNodeWithChildren(false, e1, e2, e3, e4, e5);

    rtree.insertAnExistingDataEntry(6); // Insert omega
    // It should be added to E3 as described in the paper
    assertEquals(2, rtree.Node_size(e3));
  }

  public void testBuild() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    RRStarTree rtree = new RRStarTree(4, 8);
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
    double[][] points = readCoordsResource(fileName);
    RRStarTree rtree = new RRStarTree(6, 12);
    rtree.initializeFromPoints(points);
  }

  public void testSplit() {
    String fileName = "/test3.points";
    double[][] points = readCoordsResource(fileName);
    RRStarTree rtree = new RRStarTree(2, 9);

    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 10);
    Rectangle2D.Double[] expectedLeaves = {
        new Rectangle2D.Double(1, 2, 5, 10),
        new Rectangle2D.Double(7,3,6,4)
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

  public void testSplitNonLeaf() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    RRStarTree rtree = new RRStarTree(2, 4);

    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 22);
  }

  public void testChooseSubtree() {
    String fileName = "/test4.points";
    double[][] points = readCoordsResource(fileName);
    RRStarTree rtree = new RRStarTree(2, 10);

    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 14);
    Rectangle2D.Double[] leaves = new Rectangle2D.Double[2];
    int numLeaves = 0;
    for (RTreeGuttman.Node leaf : rtree.getAllLeaves()) {
      leaves[numLeaves++] = new Rectangle2D.Double(leaf.min[0], leaf.min[1],
          leaf.max[0]-leaf.min[0], leaf.max[1]-leaf.min[1]);
    }

    assertEquals(2, numLeaves);
    assertFalse("Leaves should be disjoint", leaves[0].intersects(leaves[1]));
  }

  private RRStarTree createRRStarWithDataEntries(Rectangle2D.Double[] entries) {
    double[] x1s = new double[entries.length];
    double[] y1s = new double[entries.length];
    double[] x2s = new double[entries.length];
    double[] y2s = new double[entries.length];
    for (int i = 0; i < entries.length; i++) {
      x1s[i] = entries[i].getMinX();
      y1s[i] = entries[i].getMinY();
      x2s[i] = entries[i].getMaxX();
      y2s[i] = entries[i].getMaxY();
    }
    RRStarTree rrStarTree = new RRStarTree(4, 8);
    rrStarTree.initializeDataEntries(x1s, y1s, x2s, y2s);
    return rrStarTree;
  }


  public void testWithEmptyRecords() {
    int numPoints = 1000;
    int numDimensions = 2;
    double[][] points = new double[numDimensions][numPoints];
    Random r = new Random(0);
    for (int $i = 0; $i < numPoints; $i++) {
      if ($i % 100 == 0) {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = Double.NaN;
      } else {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = r.nextDouble();
      }
    }
    RTreeGuttman rtree = new RStarTree(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 1000);
    // Retrieving all the entries in the tree should return all the points
    int count = 0;
    for (RTreeGuttman.Entry e : rtree.entrySet())
      count++;
    assertEquals(numPoints, count);
    // A search with a box should skip NaN entries
    EnvelopeNDLite searchBox = new EnvelopeNDLite(numDimensions);
    for (int $d = 0; $d < numDimensions; $d++) {
      searchBox.setMinCoord($d, 0.0);
      searchBox.setMaxCoord($d, 1.0);
    }
    count = 0;
    for (RTreeGuttman.Entry e : rtree.search(searchBox))
      count++;
    assertEquals(numPoints * 99 / 100, count);
  }

  public void testSplitNonLeafNodeWithChildrenWithEmptyMBR() {
    RRStarTree rtree = new RRStarTree(2, 4);
    int numDimensions = 2;
    int numElements = 8;
    Random random = new Random(0);
    double[][] minCoord = new double[numDimensions][numElements];
    double[][] maxCoord = new double[numDimensions][numElements];
    // Set the first two elements to an empty object
    int numEmptyElements = 8;
    for (int $d = 0; $d < numDimensions; $d++) {
      for (int $i = 0; $i < numEmptyElements; $i++) {
        minCoord[$d][$i] = Double.POSITIVE_INFINITY;
        maxCoord[$d][$i] = Double.NEGATIVE_INFINITY;

      }
      for (int $i = numEmptyElements; $i < numElements; $i++) {
        minCoord[$d][$i] = random.nextDouble();
        maxCoord[$d][$i] = minCoord[$d][$i] + 0.1;
      }
    }
    // Initialize the rtree with the data elements
    rtree.initializeDataEntries(minCoord, maxCoord);
    // Create four leaf nodes as one node for each two points
    int n1 = rtree.Node_createNodeWithChildren(true, 0, 1);
    int n2 = rtree.Node_createNodeWithChildren(true, 2, 3);
    int n3 = rtree.Node_createNodeWithChildren(true, 4, 5);
    int n4 = rtree.Node_createNodeWithChildren(true, 6, 7);
    // Create one root that combines the four nodes
    int rootNode = rtree.Node_createNodeWithChildren(false, n1, n2, n3, n4);
    rtree.root = rootNode;
    // Now, try to split the root node
    rtree.splitNonLeaf(rootNode, 2);
  }

  public void testSplitLeafNodeWithChildrenWithEmptyMBR() {
    RRStarTree rtree = new RRStarTree(2, 4);
    int numDimensions = 2;
    int numElements = 4;
    Random random = new Random(0);
    double[][] minCoord = new double[numDimensions][numElements];
    double[][] maxCoord = new double[numDimensions][numElements];
    // Set the first two elements to an empty object
    int numEmptyElements = 4;
    for (int $d = 0; $d < numDimensions; $d++) {
      for (int $i = 0; $i < numEmptyElements; $i++) {
        minCoord[$d][$i] = Double.POSITIVE_INFINITY;
        maxCoord[$d][$i] = Double.NEGATIVE_INFINITY;

      }
      for (int $i = numEmptyElements; $i < numElements; $i++) {
        minCoord[$d][$i] = random.nextDouble();
        maxCoord[$d][$i] = minCoord[$d][$i] + 0.1;
      }
    }
    // Initialize the rtree with the data elements
    rtree.initializeDataEntries(minCoord, maxCoord);
    // Create one leaf (and root node) with the four children
    int n1 = rtree.Node_createNodeWithChildren(true, 0, 1, 2, 3);
    rtree.root = n1;
    // Now, try to split the root node
    rtree.splitLeaf(n1, 2);
  }

  public void testWithEmptyAttributesAndSorted() {
    int numPoints = 1000;
    int numDimensions = 2;
    double[][] points = new double[numDimensions][numPoints];
    Random r = new Random(0);
    for (int $i = 0; $i < numPoints; $i++) {
      if ($i % 100 < 90) {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = Double.NaN;
      } else {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = r.nextDouble();
      }
    }
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        for (int $d = 0; $d < numDimensions; $d++) {
          int diff = (int) Math.signum(points[$d][i] - points[$d][j]);
          if (diff != 0)
            return diff;
        }
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        for (int $d = 0; $d < numDimensions; $d++) {
          double t = points[$d][i];
          points[$d][i] = points[$d][j];
          points[$d][j] = t;
        }
      }
    };
    new QuickSort().sort(sortable, 0, numPoints);
    RTreeGuttman rtree = new RStarTree(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 1000);
    // Retrieving all the entries in the tree should return all the points
    int count = 0;
    for (RTreeGuttman.Entry e : rtree.entrySet())
      count++;
    assertEquals(numPoints, count);
  }
}