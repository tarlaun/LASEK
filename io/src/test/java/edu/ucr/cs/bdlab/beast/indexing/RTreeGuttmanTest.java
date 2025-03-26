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
import edu.ucr.cs.bdlab.beast.util.LongArray;
import edu.ucr.cs.bdlab.beast.util.MemoryInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Unit test for the RTreeGuttman class
 */
public class RTreeGuttmanTest extends JavaSpatialSparkTest {

  public void testShouldTestTreeCapacity() {
    int[] m = {3, 4, 5, 0};
    int[] M = {8, 8, 8, 2};
    boolean[] valid = {true, true, false, false};
    for (int i = 0; i < valid.length; i++) {
      boolean exceptionThrown = false;
      try {
        new RTreeGuttman(m[i], M[i]);
      } catch (Exception e) {
        exceptionThrown = true;
      }

      if (exceptionThrown && valid[i])
        fail(String.format("RTree should not fail with m=%d and M=%d", m[i], M[i]));
      else if (!exceptionThrown && !valid[i])
        fail(String.format("RTree should fail with m=%d and M=%d", m[i], M[i]));

    }
  }

  public void testInsertShouldExpandCorrectly() {
    RTreeGuttman rtree = new RTreeGuttman(3, 6);
    double[][] points = new double[2][7];
    points[0][0] = 2.0;
    points[1][0] = 4.0;
    points[0][1] = 6.0;
    points[1][1] = 10.0;
    points[0][2] = 7.0;
    points[1][2] = 3.0;
    points[0][3] = 12.0;
    points[1][3] = 8.0;
    points[0][4] = 13.0;
    points[1][4] = 5.0;
    points[0][5] = 16.0;
    points[1][5] = 8.0;
    points[0][6] = 0.0;
    points[1][6] = 8.0;
    rtree.initializeDataEntries(points[0], points[1]);
    int n1 = rtree.Node_createNodeWithChildren(true, 0, 1);
    int n2 = rtree.Node_createNodeWithChildren(true, 2, 3);
    int n3 = rtree.Node_createNodeWithChildren(true, 4, 5);
    int root = rtree.Node_createNodeWithChildren(false, n1, n2, n3);
    rtree.root = root;
    rtree.insertAnExistingDataEntry(6);
    assertEquals(0.0, rtree.minCoord[0][root]);
  }

  public void testBuild() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 11);
    assertEquals(3, rtree.numOfNodes());
    assertEquals(1, rtree.getHeight());
  }

  public void testBuildRectangles() {
    String fileName = "/test.rect";
    double[][] rects = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromRects(rects[0], rects[1], rects[2], rects[3]);
    assertEquals(rtree.numOfDataEntries(), 14);
  }

  public void testBuildInfiniteRectangles() {
    String fileName = "/test_inf.rect";
    double[][] rects = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromRects(rects[0], rects[1], rects[2], rects[3]);
    assertEquals(rtree.numOfDataEntries(), 14);
  }

  public void testSearch() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    double x1 = 0, y1 = 0, x2 = 5, y2 = 4;
    assertTrue("First object should overlap the search area",
        rtree.Object_overlaps(0, new double[] {x1, y1}, new double[] {x2, y2}));
    assertTrue("Second object should overlap the search area",
        rtree.Object_overlaps(2, new double[] {x1, y1}, new double[] {x2, y2}));
    IntArray expectedResult = new IntArray();
    expectedResult.add(0);
    expectedResult.add(2);
    for (RTreeGuttman.Entry entry : rtree.search(x1, y1, x2, y2)) {
      assertTrue("Unexpected result "+entry, expectedResult.remove(entry.id));
    }
    assertTrue("Some expected results not returned "+expectedResult, expectedResult.isEmpty());
  }

  public void testIterateOverEntries() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    double[] x1s = points[0];
    double[] y1s = points[1];
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(11, rtree.numOfDataEntries());
    int count = 0;
    for (RTreeGuttman.Entry entry : rtree.entrySet()) {
      assertEquals(x1s[entry.id], entry.min[0]);
      assertEquals(y1s[entry.id], entry.min[1]);
      count++;
    }
    assertEquals(11, count);
  }
  public void testBuild2() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
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

  public void testSplit() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    // Create a tree with a very large threshold to avoid any splits
    RTreeGuttman rtree = new RTreeGuttman(50, 100);
    rtree.initializeFromPoints(points);
    assertEquals(11, rtree.numOfDataEntries());
    // Make one split at the root
    int iNode = rtree.root;
    int iNewNode = rtree.split(iNode, 4);
    assertTrue("Too small size " + rtree.Node_size(iNewNode), rtree.Node_size(iNewNode) > 2);
    assertTrue("Too small size " + rtree.Node_size(iNode), rtree.Node_size(iNode) > 2);
  }


  public void testBuild3() {
    String fileName = "/test2.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(2, 4);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 22);
    int maxNumOfNodes = 18;
    int minNumOfNodes = 9;
    assertTrue(String.format("Too few nodes %d<%d",rtree.numOfNodes(), minNumOfNodes),
        rtree.numOfNodes() >= minNumOfNodes);
    assertTrue(String.format("Too many nodes %d>%d", rtree.numOfNodes(), maxNumOfNodes),
        rtree.numOfNodes() <= maxNumOfNodes);
    assertTrue(String.format("Too short tree %d",rtree.getHeight()),
        rtree.getHeight() >= 2);
  }

  public void testRetrieveLeaves() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 11);
    Iterable<RTreeGuttman.Node> leaves = rtree.getAllLeaves();
    EnvelopeNDLite mbrAllLeaves = new EnvelopeNDLite(2, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    int numOfLeaves = 0;
    for (RTreeGuttman.Node leaf : leaves) {
      mbrAllLeaves.merge(leaf.min);
      mbrAllLeaves.merge(leaf.max);
      numOfLeaves++;
    }
    assertEquals(2, numOfLeaves);
    assertEquals(new EnvelopeNDLite(2, 1.0, 2.0, 12.0, 12.0), mbrAllLeaves);
  }


  public void testExpansion() {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(0.0, rtree.Node_volumeExpansion(rtree.root, 0));
  }

  public void testWrite() throws IOException {
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 11);
    assertEquals(3, rtree.numOfNodes());
    assertEquals(1, rtree.getHeight());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    rtree.write(dos, null);
    dos.close();

    ByteBuffer serializedTree = ByteBuffer.wrap(baos.toByteArray());
    int footerOffset = serializedTree.getInt(serializedTree.limit() - 8);
    // 0 bytes per data entry + 4 bytes per node (# of children) + (4 (offset) + 32 (MBR) + 8 (start,end)) for each child (root excluded)
    final int expectedFooterOffset = 11 * 0 + 3 * 4 + (14 - 1) * (8 * 4 + 4 + 8);
    assertEquals(expectedFooterOffset, footerOffset);
    // Check the MBR of the root
    int numDimensions = serializedTree.getInt(footerOffset);
    double x1 = serializedTree.getDouble(footerOffset+4);
    double x2 = serializedTree.getDouble(footerOffset + 4 + 8);
    double y1 = serializedTree.getDouble(footerOffset + 4 + 16);
    double y2 = serializedTree.getDouble(footerOffset + 4 + 24);
    assertEquals(2, numDimensions);
    assertEquals(1.0, x1);
    assertEquals(2.0, y1);
    assertEquals(12.0, x2);
    assertEquals(12.0, y2);
  }

  public void testRead() throws IOException {
    byte[] treeBytes = null;
    String fileName = "/test.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 11);
    assertEquals(3, rtree.numOfNodes());
    assertEquals(1, rtree.getHeight());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    rtree.write(dos, null);
    dos.close();
    treeBytes = baos.toByteArray();

    rtree = new RTreeGuttman(4, 8);
    try {
      FSDataInputStream fsdis = new FSDataInputStream(new MemoryInputStream(treeBytes));
      rtree.readFields(fsdis, treeBytes.length, null);
      IntArray results = new IntArray();
      rtree.search(new double[]{0, 0}, new double[]{4.5, 10}, results);
      assertEquals(3, results.size());
      Iterable<RTreeGuttman.Entry> iResults = rtree.search(10, 6, 11, 7);
      assertFalse(iResults.iterator().hasNext());

      iResults = rtree.search(5.5, 5, 10, 7);
      int resultCount = 0;
      for (Object o : iResults)
        resultCount++;
      assertEquals(1, resultCount);
    } finally {
      rtree.close();
    }
  }

  public void testReadMultilevelFile() throws IOException {
    byte[] treeBytes = null;
    String fileName = "/test111.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 111);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    rtree.write(dos, null);
    dos.close();
    treeBytes = baos.toByteArray();

    rtree = new RTreeGuttman(4, 8);
    FSDataInputStream fsdis = new FSDataInputStream(new MemoryInputStream(treeBytes));
    rtree.readFields(fsdis, treeBytes.length, null);
    int resultSize = 0;
    for (Object o : rtree.entrySet()) {
      resultSize++;
    }
    assertEquals(111, resultSize);
  }

  public void testDiskSearch() throws IOException {
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    byte[] treeBytes = null;
    String fileName = "/test111.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 111);

    DataOutputStream dos = new DataOutputStream(new FileOutputStream(rtreePath.toString()));
    rtree.write(dos, (out, iObject) -> {
      out.writeInt(iObject);
      return 4;
    });
    dos.close();

    Iterable<Integer> searchResults = null;
    double[] searchBoxMin = {-100, 20};
    double[] searchBoxMax = {-50, 60};
    int expectedCount = 0;
    for (int i$ = 0; i$ < points[0].length; i$++) {
      boolean match = true;
      for (int d$ = 0; match && d$ < points.length; d$++) {
        if (points[d$][i$] < searchBoxMin[d$] || points[d$][i$] >= searchBoxMax[d$])
          match = false;
      }
      if (match)
        expectedCount++;
    }
    Configuration conf = new Configuration();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FSDataInputStream in = fs.open(rtreePath);
    try {
      searchResults = RTreeGuttman.search(in, fs.getFileStatus(rtreePath).getLen(),
          searchBoxMin, searchBoxMax, input -> input.readInt());
      int actualCount = 0;
      for (Integer e : searchResults)
        actualCount++;
      assertEquals(expectedCount, actualCount);
    } finally {
      in.close();
    }
  }

  public void testReadAllFromDisk() throws IOException {
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    byte[] treeBytes = null;
    String fileName = "/test111.points";
    double[][] points = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeFromPoints(points);
    assertEquals(rtree.numOfDataEntries(), 111);

    DataOutputStream dos = new DataOutputStream(new FileOutputStream(rtreePath.toString()));
    rtree.write(dos, (out, iObject) -> {
      out.writeInt(iObject);
      return 4;
    });
    dos.close();

    Iterable<Integer> searchResults = null;
    int expectedCount = 111;
    Configuration conf = new Configuration();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FSDataInputStream in = fs.open(rtreePath);
    try {
      searchResults = RTreeGuttman.readAll(in, fs.getFileStatus(rtreePath).getLen(), input -> input.readInt());
      int actualCount = 0;
      for (Integer e : searchResults)
        actualCount++;
      assertEquals(expectedCount, actualCount);
    } finally {
      in.close();
    }
  }

  public void testHollowRTree() {
    String fileName = "/test.rect";
    double[][] rects = readCoordsResource(fileName);
    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.initializeHollowRTree(rects[0], rects[1], rects[2], rects[3]);
    int numLeaves = 0;
    for (Object leaf : rtree.getAllLeaves())
      numLeaves++;
    assertEquals(14, rtree.numOfDataEntries());

    int i = rtree.noInsert(new double[] {999, 200}, new double[] {999, 200});
    assertEquals(12, i);
  }

  public void testSpatialJoin() {
    int numRecords = 100;
    for (int seed = 0; seed < 100; seed++) {
      // Generate random rectangles
      Random random = new Random(seed);
      int numDimensions = 2;
      double[][] r = new double[numDimensions * 2][numRecords];
      double[][] s = new double[numDimensions * 2][numRecords];
      for (int i = 0; i < numRecords; i++) {
        for (int d = 0; d < numDimensions; d++) {
          r[d][i] = random.nextDouble();
          r[numDimensions + d][i] = r[d][i] + 0.05;
          s[d][i] = random.nextDouble();
          s[numDimensions + d][i] = s[d][i] + 0.05;
        }
      }
      // Count actual number of overlapping pairs
      LongArray expectedResults = new LongArray();
      EnvelopeNDLite r1 = new EnvelopeNDLite(numDimensions);
      EnvelopeNDLite s1 = new EnvelopeNDLite(numDimensions);
      for (int i = 0; i < numRecords; i++) {
        for (int d = 0; d < numDimensions; d++) {
          r1.setMinCoord(d, r[d][i]);
          r1.setMaxCoord(d, r[numDimensions + d][i]);
        }
        for (int j = 0; j < numRecords; j++) {
          for (int d = 0; d < numDimensions; d++) {
            s1.setMinCoord(d, s[d][j]);
            s1.setMaxCoord(d, s[numDimensions + d][j]);
          }
          if (r1.intersectsEnvelope(s1))
            expectedResults.add(((long) i << 32) | j);
        }
      }

      // Run spatial join using R-trees
      RTreeGuttman rtree1 = new RTreeGuttman(4, 8);
      rtree1.initializeFromRects(r[0], r[1], r[2], r[3]);

      RTreeGuttman rtree2 = new RTreeGuttman(4, 8);
      rtree2.initializeFromRects(s[0], s[1], s[2], s[3]);

      RTreeGuttman.spatialJoin(rtree1, rtree2, (i, j) -> {
        assertTrue(String.format("Result %d,%d not found", i, j),
            expectedResults.remove(((long) i << 32) | j));
      });
      for (long pair : expectedResults) {
        int i = (int) (pair >>> 32);
        int j = (int) (pair & 0xffffffff);
        System.err.printf("Result not found %d,%d\n", i, j);
      }
      assertTrue("Some results not found", expectedResults.isEmpty());
    }
  }
}
