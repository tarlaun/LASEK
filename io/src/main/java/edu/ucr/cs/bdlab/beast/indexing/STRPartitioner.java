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

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A spatial partitioner based on the Sort-Tile-Recursive algorithm (STR) as described in the following paper.
 * Scott T. Leutenegger, J. M. Edgington, Mario A. López:
 * STR: A Simple and Efficient Algorithm for R-Tree Packing. ICDE 1997: 497-506
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "str",
    description = "Partitions the space using the Sort-tile-recursive (STR) algorithm for R-tree packing")
public class STRPartitioner extends SpatialPartitioner {

  /**Keep the partitions disjoint by replicating objects to all overlapping partitions*/
  private boolean disjoint;

  /**The degree of the STR tree (number of children for each node).*/
  private int treeDegree;

  /**Number of levels in the tree*/
  private int numTreeLevels;

  /**The splits that define the STR tree stored in level-order traversal.*/
  protected double[] splitCoords;

  /**The level of each node in the in-order traversal. While it can be computed, it is more compute-efficient to cache*/
  protected int[] nodeLevel;

  /**Number of dimensions for the partitions*/
  protected int numDimensions;

  /**Total number of internal nodes in the STR tree. The leaf nodes represent the partitions*/
  protected transient int numInternalNodes;

  /**The envelope of the underlying dataset*/
  protected EnvelopeNDLite envelope = new EnvelopeNDLite();

  @Override
  public void setup(BeastOptions conf, boolean disjoint) {
    this.disjoint = disjoint;
  }

  @Override
  public void construct(Summary summary, @Required double[][] sample, AbstractHistogram histogram, int numBuckets) {
    this.envelope.set(summary);
    this.numDimensions = sample.length;
    final int sampleCount = sample[0].length;
    if (numBuckets == 1) {
      numTreeLevels = 0;
      return; // Special case. No need to make any partitions or splits
    }
    this.treeDegree = (int) Math.ceil(Math.pow(numBuckets, 1.0 / numDimensions));
    // TODO: Make a shorter tree (fewer levels) when treeDegree^numDimensions >> desired # of partitions
    this.numTreeLevels = numDimensions;
    numInternalNodes = (int) ((Math.pow(treeDegree, numDimensions) - 1) / (treeDegree - 1));
    LOG.info(String.format("Creating an STR tree with %d levels and a degree of %d. Total number of nodes will be %d",
        numDimensions, treeDegree, numInternalNodes));
    nodeLevel = new int[numInternalNodes];
    int totalNumberOfSplits = numInternalNodes * (treeDegree - 1);
    // Reserve an array of all split coordinates
    splitCoords = new double[totalNumberOfSplits];

    assert totalNumberOfSplits <= sampleCount;

    // Recursively split the points along each axis
    class AxisSorter implements IndexedSortable {
      int axis;

      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(sample[axis][i] - sample[axis][j]);
      }

      @Override
      public void swap(int i, int j) {
        double t;
        for (int d = 0; d < numDimensions; d++) {
          t = sample[d][i];
          sample[d][i] = sample[d][j];
          sample[d][j] = t;
        }
      }
    }
    AxisSorter sorter = new AxisSorter();
    QuickSort quickSort = new QuickSort();

    class SplitRange {
      int start, end;
      int splitAxis;
      /**The ID of the partition being split. The root partition (input MBR) has an ID of zero*/
      int partitionID;

      SplitRange(int partitionID, int s, int e, int x) {
        this.partitionID = partitionID;
        this.start = s;
        this.end = e;
        this.splitAxis = x;
      }
    }
    // Recursively split the input space starting at the root with the first dimensions
    Queue<SplitRange> toSplit = new ArrayDeque<>();
    toSplit.add(new SplitRange(0, 0, sampleCount, 0));
    while (!toSplit.isEmpty()) {
      SplitRange rangeToSplit = toSplit.remove();
      // Sort along this axis
      sorter.axis = rangeToSplit.splitAxis;
      nodeLevel[rangeToSplit.partitionID] = rangeToSplit.splitAxis;
      quickSort.sort(sorter, rangeToSplit.start, rangeToSplit.end);
      // Split into the desired number of splits. Number of splits = tree degree - 1
      // i1: The index of the first point in the split
      int indexOfTheFirstSplitForThisPartition = rangeToSplit.partitionID * (treeDegree - 1);
      int i1 = rangeToSplit.start;
      for (int iSplit = 1; iSplit < treeDegree; iSplit++) {
        // i2: The index after the last point in the split
        int i2 = (rangeToSplit.start * (treeDegree - iSplit) + (rangeToSplit.end + 1) * iSplit) / treeDegree;
        // The split is located at the coordinate between the last point in the split, and the next point
        splitCoords[indexOfTheFirstSplitForThisPartition + iSplit - 1] = (sample[rangeToSplit.splitAxis][i2-1] + sample[rangeToSplit.splitAxis][i2]) / 2.0;
        if (rangeToSplit.splitAxis < (numDimensions - 1)) // Need further split
          toSplit.add(new SplitRange(rangeToSplit.partitionID * treeDegree + iSplit, i1, i2, rangeToSplit.splitAxis + 1));
        i1 = i2;
      }
      // Add the last partition to split
      if (rangeToSplit.splitAxis < (numDimensions - 1))
        toSplit.add(new SplitRange(rangeToSplit.partitionID * treeDegree + treeDegree, i1, rangeToSplit.end, rangeToSplit.splitAxis + 1));
    }
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    assert mbr.getCoordinateDimension() == this.getCoordinateDimension();
    // Reuse the given array (matchedPartitions) as a search stack
    matchedPartitions.append(0); // Start searching at the root
    if (treeDegree == 0)
      return; // No splits. Just one big partition (id = 0).
    int numNodesToSearch = 1;
    while (numNodesToSearch > 0) {
      int nodeID = matchedPartitions.pop();
      numNodesToSearch--;
      do {
        // Since we expect most rectangles to overlap a few nodes, we binary search to find the first overlapping
        // node and then linearly advance to find any additional overlapping nodes
        int axis = nodeLevel[nodeID];
        // Binary search to find the correct child that contains the lower point of the MBR
        int s = 0;
        int e = treeDegree - 1; // There are only treeDegree - 1 splits
        int firstSplitIndex = nodeID * (treeDegree - 1);
        while (s < e) {
          int m = (s + e) / 2;
          double diff = mbr.getMinCoord(axis) - splitCoords[firstSplitIndex + m];
          if (diff < 0)
            e = m;
          else
            s = m + 1;
        }
        assert s == e;
        // Now, advance e to point to the index after the last node that overlaps the given MBR
        while (e < (treeDegree - 1) && mbr.getMaxCoord(axis) > splitCoords[firstSplitIndex + e])
          e++;

        // We go directly into the first matching node, and keep the others in the stack
        while (e > s) {
          int childNodeID = nodeID * treeDegree + e + 1;
          if (childNodeID < nodeLevel.length) {
            // The child is an internal node, recursively search through it
            matchedPartitions.append(childNodeID - numInternalNodes);
            numNodesToSearch++;
          } else {
            // The child is a leaf node, add to matching partitions
            matchedPartitions.insert(numNodesToSearch, childNodeID - numInternalNodes);
          }
          e--;
        }
        nodeID = nodeID * treeDegree + s + 1;
        if (nodeID >= nodeLevel.length) {
          // Reached a leaf node, add to matching partitions
          matchedPartitions.insert(numNodesToSearch, nodeID - numInternalNodes);
          // Mark the end of the search for this node.
          nodeID = -1;
        }
      } while (nodeID != -1);
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    if (treeDegree == 0)
      return 0; // Special handling for one partition (no splits)
    assert mbr.getCoordinateDimension() == this.getCoordinateDimension();
    // Navigate to the single partition that contains the center of the given MBR
    int nodeID = 0; // Start searching at the root
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      double mbrCenter = (mbr.getMinCoord(d) + mbr.getMaxCoord(d)) / 2.0;
      // Binary search to find the correct child
      int s = 0;
      int e = treeDegree - 1; // There are only treeDegree - 1 splits
      int firstSplitIndex = nodeID * (treeDegree - 1);
      while (s < e) {
        int m = (s + e) / 2;
        double diff = mbrCenter - splitCoords[firstSplitIndex + m];
        if (diff < 0)
          e = m;
        else
          s = m + 1;
      }
      assert s == e;
      nodeID = nodeID * treeDegree + s + 1;
    }
    return nodeID - numInternalNodes;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    partitionID += numInternalNodes;
    assert mbr.getCoordinateDimension() == this.getCoordinateDimension();
    if (treeDegree == 0) {
      // No splits! Return an infinite rectangle
      for (int d = 0; d < getCoordinateDimension(); d++) {
        mbr.setMinCoord(d, Double.NEGATIVE_INFINITY);
        mbr.setMaxCoord(d, Double.POSITIVE_INFINITY);
      }
      return;
    }
    // Navigate up from the given partitionID (nodeID) to the root while setting the boundaries of the envelope
    assert partitionID >= nodeLevel.length;
    while (partitionID > 0) {
      int parentNodeID = (partitionID - 1) / treeDegree;
      int childIndex = partitionID % treeDegree - 1;
      if (childIndex == -1)
        childIndex += treeDegree;
      int axis = nodeLevel[parentNodeID];
      mbr.setMinCoord(axis, childIndex == 0? Double.NEGATIVE_INFINITY : splitCoords[parentNodeID * (treeDegree - 1) + childIndex - 1]);
      mbr.setMaxCoord(axis, childIndex == (treeDegree - 1)? Double.POSITIVE_INFINITY : splitCoords[parentNodeID * (treeDegree - 1) + childIndex]);
      partitionID = parentNodeID;
    }
  }

  @Override
  public int numPartitions() {
    // The tree is a full tree. All nodes have the same degree (treeDegree)
    int numPartitions = 1;
    for (int $l = 0; $l < numTreeLevels; $l++)
      numPartitions *= treeDegree;
    return numPartitions;
  }

  @Override
  public boolean isDisjoint() {
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    return this.numDimensions;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(envelope, out);
    out.writeInt(treeDegree);
    out.writeInt(numTreeLevels);
    out.writeInt(numDimensions);
    out.writeBoolean(disjoint);
    if (treeDegree > 0) {
      IntArray.writeIntArray(nodeLevel, out);
      for (double x : splitCoords)
        out.writeDouble(x);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(envelope, in);
    this.treeDegree = in.readInt();
    this.numTreeLevels = in.readInt();
    this.numDimensions = in.readInt();
    this.disjoint = in.readBoolean();
    if (treeDegree > 0) {
      nodeLevel = IntArray.readIntArray(nodeLevel, in);
      splitCoords = new double[nodeLevel.length * (treeDegree - 1)];
      for (int i = 0; i < splitCoords.length; i++)
        splitCoords[i] = in.readDouble();
    }
    numInternalNodes = (int) ((Math.pow(treeDegree, numDimensions) - 1) / (treeDegree - 1));
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return envelope;
  }

  @Override
  public String toString() {
    return "STRPartitioner{"+ numPartitions()+","+(disjoint? "disjoint" : "non-disjoint") +'}';
  }
}
