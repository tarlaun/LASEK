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

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.BitArray;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import edu.ucr.cs.bdlab.beast.util.LongArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.locationtech.jts.geom.Geometry;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.function.BiConsumer;

import static edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.getMBRs;
import static edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.getMBRs2;

/**
 * A partial implementation for the original Antonin Guttman R-tree as described
 * in the following paper.
 * Antonin Guttman: R-Trees: A Dynamic Index Structure for Spatial Searching.
 * SIGMOD Conference 1984: 47-57
 *
 * It only contains the implementation of the parts needed for the indexing
 * methods. For example, the delete operation was not implemented as it is
 * not needed. Also, this index is designed mainly to be used to index a sample
 * in memory and use it for the partitioning. So, the disk-based mapping and
 * search were not implemented for simplicity.
 */
public class RTreeGuttman implements Closeable {
  private static final Log LOG = LogFactory.getLog(RTreeGuttman.class);

  /** Maximum capacity of a node (M) */
  protected final int maxCapacity;

  /** Minimum capacity of a node (m) */
  protected final int minCapacity;

  /**
   * The minimum coordinates of all the objects (nodes and entries). First index is for the dimensions and second
   * dimension is for the object number.
   */
  protected double[][] minCoord;

  /**
   * The maximum coordinates of all the objects (nodes and entries). First index is for the dimensions and second
   * dimension is for the object number.
   */
  protected double[][] maxCoord;

  /** Global minimum across all dimensions. Useful when the minimum is -Infinity */
  protected double[] globalMin;

  /** Global maximum across all dimensions. Useful when the maximum is Infinity */
  protected double[] globalMax;

  /**A bit vector that stores which nodes are leaves*/
  protected BitArray isLeaf;

  /**A list of int[] that stores the children of each node*/
  protected List<IntArray> children;

  /**Total number of data entries*/
  protected int numEntries;

  /**Total number of nodes*/
  protected int numNodes;

  /**The index of the root in the list of nodes*/
  protected int root;

  /**Only when processing an on-disk tree. Stores the offset of each data entry in the file*/
  protected int[] entryOffsets;

  /**A deserializer that reads objects stored on disk*/
  private Deserializer<?> deser;

  /**The input stream that points to the underlying file*/
  private FSDataInputStream seekableIn;

  /**When reading the tree from disk. The offset of the beginning of the tree*/
  private long treeStartOffset;

  /**The total size of the data chunk*/
  private int totalDataSize;

  /**A random number generator for choosing random nodes when cannot decide*/
  protected Random random = new Random();

  /**
   * Retrieves the number of dimensions of the tree
   * @return the number of dimensions for data entries.
   */
  public int getNumDimensions() {
    return minCoord.length;
  }

  /**
   * Retrieves the maximum number of objects that can be currently stored in the tree without expansion
   * @return the maximum number of objects that the tree can hold (nodes + data entries).
   */
  protected int getCurrentCapacity() {
    return minCoord[0].length;
  }

  /**
   * Make a room in the data structures to accommodate a new object whether it is a node or a data entry.
   */
  protected void makeRoomForOneMoreObject() {
    int currentSize = numEntries + numNodes;
    if (getCurrentCapacity() <= currentSize) {
      // Expand the coordinate arrays in big chunks to avoid memory copy
      int numDimensions = getNumDimensions();
      int newCapacity = getCurrentCapacity() * 2;
      for (int d = 0; d < numDimensions; d++) {
        double[] newCoords = new double[newCapacity];
        System.arraycopy(minCoord[d], 0, newCoords, 0, currentSize);
        minCoord[d] = newCoords;

        newCoords = new double[newCapacity];
        System.arraycopy(maxCoord[d], 0, newCoords, 0, currentSize);
        maxCoord[d] = newCoords;
      }
      this.isLeaf.resize(newCapacity);
    }
  }

  /**
   * Creates a new node that contains the given object and returns the ID of that node.
   * @param leaf set to true to create a leaf node
   * @param iChildren the indexes of all children in this node
   * @return the id of the new node created.
   */
  protected int Node_createNodeWithChildren(boolean leaf, int ... iChildren) {
    makeRoomForOneMoreObject();
    int iNewNode = numEntries + numNodes;
    this.isLeaf.set(iNewNode, leaf);
    this.children.add(iNewNode, new IntArray());
    this.numNodes++;
    Node_reset(iNewNode, iChildren);
    return iNewNode;
  }

  /**
   * Reset a node to contain a new set of children wiping away the current children.
   * @param iNode the index of the node to reset
   * @param newChildren the new list of child node ids.
   */
  protected void Node_reset(int iNode, int ... newChildren) {
    children.get(iNode).clear();
    children.get(iNode).append(newChildren, 0, newChildren.length);
    Node_recalculateMBR(iNode);
  }

  protected void Node_recalculateMBR(int iNode) {
    for (int d = 0; d < getNumDimensions(); d++) {
      minCoord[d][iNode] = Double.POSITIVE_INFINITY;
      maxCoord[d][iNode] = Double.NEGATIVE_INFINITY;
    }
    for (int iChild : children.get(iNode)) {
      for (int d = 0; d < getNumDimensions(); d++) {
        minCoord[d][iNode] = Math.min(minCoord[d][iNode], minCoord[d][iChild]);
        maxCoord[d][iNode] = Math.max(maxCoord[d][iNode], maxCoord[d][iChild]);
      }
    }
  }

  /**
   * Returns the number of children for the given node.
   * @param iNode the index of the node
   * @return the size of the node in terms of number of children.
   */
  protected int Node_size(int iNode) {
    return children.get(iNode).size();
  }

  /**
   * Calculates the volume of the node
   * @param iNode the ID of the node
   * @return the volume of the given node (e.g., area for two dimensions).
   */
  protected double Node_volume(int iNode) {
    double vol = 1.0;
    for (int d = 0; d < getNumDimensions(); d++)
      vol *= Math.min(globalMax[d], maxCoord[d][iNode]) - Math.max(globalMin[d], minCoord[d][iNode]);
    return vol;
  }

  /**
   * Calculates the volume (area) expansion that will happen if the given object is added to a given node.
   * @param iNode the ID of the node that would be expanded
   * @param iNewChild the ID of the object that would be added to the node
   * @return the expansion of the volume of the given child is added to the given node.
   */
  protected double Node_volumeExpansion(int iNode, int iNewChild) {
    double volBefore = 1.0, volAfter = 1.0;
    for (int d = 0; d < getNumDimensions(); d++) {
      volBefore *= Math.min(maxCoord[d][iNode], globalMax[d]) - Math.max(minCoord[d][iNode], globalMin[d]);
      volAfter *= Math.min(globalMax[d], Math.max(maxCoord[d][iNode], maxCoord[d][iNewChild])) -
          Math.max(globalMin[d], Math.min(minCoord[d][iNode], minCoord[d][iNewChild]));
    }
    return volAfter - volBefore;
  }

  /**
   * Adds a new child to an existing node.
   * @param iNode the index of the parent node
   * @param iNewChild the index of the child node
   */
  protected void Node_addChild(int iNode, int iNewChild) {
    this.children.get(iNode).add(iNewChild);
  }

  /**
   * Expand the MBR of the given node to enclose the given new object
   * @param node the node number
   * @param newObject the index of the object to expand to
   */
  protected void Node_expand(int node, int newObject) {
    // Expand the MBR to enclose the new child
    for (int d = 0; d < getNumDimensions(); d++) {
      minCoord[d][node] = Math.min(minCoord[d][node], minCoord[d][newObject]);
      maxCoord[d][node] = Math.max(maxCoord[d][node], maxCoord[d][newObject]);
    }
  }

  /**
   * Split an existing node around the given separator. Current children from
   * indexes 1 to separator-1 (inclusive) remain in the given node. All remaining
   * children go to a new node. The ID of the new node created that contain the
   * children from separator to end.
   * @param iNode the index of the node to split
   * @param separator the index of the first child to be in the new node
   * @return the ID of the new node created after split
   */
  protected int Node_split(int iNode, int separator) {
    // Create the new node that will hold the entries from separator -> size
    makeRoomForOneMoreObject();
    int iNewNode = numNodes + numEntries;
    this.numNodes++;
    // Make room for the children of the new node
    this.children.add(iNewNode, new IntArray());
    // The new node in the same level so it follow the leaf/non-leaf status of the current node
    isLeaf.set(iNewNode, isLeaf.get(iNode));

    // Split the children around the separator
    children.get(iNewNode).append(children.get(iNode), separator,
        children.get(iNode).size() - separator);
    children.get(iNode).resize(separator);

    // Recalculate the MBRs of the two nodes
    Node_recalculateMBR(iNode);
    Node_recalculateMBR(iNewNode);

    return iNewNode;
  }

  /**
   * Initialize the current R-tree from given data entries
   * @param x1 array of lower coordinates on the x-dimension
   * @param y1 array of lower coordinates on the y-dimension
   * @param x2 array of upper coordinates on the x-dimension
   * @param y2 array of upper coordinates on the y-dimension
   */
  public void initializeFromRects(double[] x1, double[] y1, double[] x2, double[] y2) {
    this.initializeDataEntries(x1, y1, x2, y2);
    this.insertAllDataEntries();
  }

  public void initializeFromBoxes(double[][] min, double[][] max) {
    this.initializeDataEntries(min, max);
    this.insertAllDataEntries();
  }

  /**
   * Initialize the tree from a set of points
   * @param points array of points. First index represents the dimension and second index represents the point number.
   */
  public void initializeFromPoints(double[][] points) {
    this.initializeDataEntries(points, points);
    this.insertAllDataEntries();
  }

  /**
   * Construct a new empty R-tree with the given parameters.
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity - Maximum capacity of a node
   */
  public RTreeGuttman(int minCapacity, int maxCapcity) {
    if (minCapacity > maxCapcity / 2)
      throw new RuntimeException(String.format("Invalid minCapacity=%d and maxCapacity=%d. minCapacity should be at most maxCapacity/2", minCapacity, maxCapcity));
    if (minCapacity == 0)
      throw new RuntimeException("minCapacity cannot be equal to zero");
    this.minCapacity = minCapacity;
    this.maxCapacity = maxCapcity;
  }

  protected void insertAllDataEntries() {
    root = Node_createNodeWithChildren(true, 0);
    // Insert one by one
    for (int i = 1; i < numEntries; i++)
      insertAnExistingDataEntry(i);
  }

  /**
   * Initialize the data entries to a set of point coordinates without actually inserting them into the tree structure.
   * @param coords array of point coordinates for the entries. First index represents the dimension and second index
   *               represents the point.
   */
  protected void initializeDataEntries(double[][] coords) {
    initializeDataEntries(coords, coords);
  }

  protected void initializeDataEntries(double[] xs, double[] ys) {
    initializeDataEntries(new double[][] {xs, ys});
  }

  /**
   * Initialize data entries from a list of minimum bounding boxes MBBs.
   * @param minCoords the minimum coordinates of the MBBs where the first index is the dimension and the second index
   *                  is the object number
   * @param maxCoords the maximum coordinates of the MBBs where the first index is the dimension and the second index
   *                  is the object number
   */
  protected void initializeDataEntries(double[][] minCoords, double[][] maxCoords) {
    this.numEntries = minCoords[0].length;
    this.numNodes = 0; // Initially, no nodes are there
    this.isLeaf = new BitArray(numEntries);
    children = new ArrayList<>(numEntries);
    this.minCoord = new double[minCoords.length][numEntries];
    this.maxCoord = new double[maxCoords.length][numEntries];
    for (int d = 0; d < getNumDimensions(); d++) {
      System.arraycopy(minCoords[d], 0, this.minCoord[d], 0, numEntries);
      System.arraycopy(maxCoords[d], 0, this.maxCoord[d], 0, numEntries);
      // Handle empty points (with NaN coordinates) to an empty box with inverted infinite coordinates
      for (int $i = 0; $i < numEntries; $i++) {
        if (Double.isNaN(minCoords[d][$i]) || Double.isNaN(maxCoords[d][$i])) {
          this.minCoord[d][$i] = Double.POSITIVE_INFINITY;
          this.maxCoord[d][$i] = Double.NEGATIVE_INFINITY;
        }
      }
    }
    // Compute finite global minimum and maximum. Useful when some infinite coordinates are there
    globalMin = new double[getNumDimensions()];
    globalMax = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      globalMin[d] = Double.POSITIVE_INFINITY;
      for (double x : minCoord[d]) {
        if (Double.isFinite(x))
          globalMin[d] = Math.min(globalMin[d], x);
      }
      globalMax[d] = Double.NEGATIVE_INFINITY;
      for (double x : maxCoord[d]) {
        if (Double.isFinite(x))
          globalMax[d] = Math.max(globalMax[d], x);
      }
      // Expand it even more to ensure it includes all potential values
      double diff = globalMax[d] - globalMin[d];
      globalMin[d] -= diff;
      globalMax[d] += diff;
    }
    for (int i = 0; i < numEntries; i++)
      children.add(null);
  }

  /**
   * Initialize the data entries to a set of rectangular coordinates without
   * actually inserting them into the tree structure.
   * @param x1 array of lower x-coordinates of rectangles
   * @param y1 array of lower y-coordinates of rectangles
   * @param x2 array of upper x-coordinates of rectangles
   * @param y2 array of upper y-coordinates of rectangles
   */
  protected void initializeDataEntries(double[] x1, double[] y1, double[] x2, double[] y2) {
    initializeDataEntries(new double[][]{x1, y1}, new double[][]{x2, y2});
  }


  /**
   * Inserts the given data entry into the tree. We assume that the coordinates
   * of this data entry are already stored in the coordinates arrays.
   * @param iEntry - The index of the point in the array of points
   */
  protected void insertAnExistingDataEntry(final int iEntry) {
    // The path from the root to the newly inserted record. Used for splitting.
    IntArray path = new IntArray();
    int iCurrentVisitedNode = root;
    path.add(iCurrentVisitedNode);
    // Descend in the tree until we find a leaf node to add the object to
    while (!isLeaf.get(iCurrentVisitedNode)) {
      // Node is not leaf. Choose a child node
      // Descend to the best child found
      int iBestChild = chooseSubtree(iEntry, iCurrentVisitedNode);
      iCurrentVisitedNode = iBestChild;
      path.add(iCurrentVisitedNode);
    }

    // Now we have a child node. Insert the current element to it and split
    // if necessary
    Node_addChild(iCurrentVisitedNode, iEntry);
    adjustTree(iCurrentVisitedNode, path);
  }

  /**
   * Choose the best subtree to add a data entry to.
   * According to the original R-tree paper, this function chooses the node with
   * the minimum volume expansion, then the one with the smallest volume,
   * then the one with the least number of records, then randomly to any one.
   * @param iEntry the index of the entry to choose a subtree for
   * @param iNode the index of the node to choose from its children
   * @return the index of the child of the given node that the entry would be added to.
   */
  protected int chooseSubtree(int iEntry, int iNode) {
    if (minCoord[0][iEntry] > maxCoord[0][iEntry]) {
      return children.get(iNode).get(random.nextInt(Node_size(iNode)));
    }
    // 1. Choose the child with the minimum expansion
    double minExpansion = Double.POSITIVE_INFINITY;
    int iBestChild = -1;
    for (int iCandidateChild : children.get(iNode)) {
      double expansion = Node_volumeExpansion(iCandidateChild, iEntry);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        iBestChild = iCandidateChild;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Node_volume(iCandidateChild) < Node_volume(iBestChild))
          iBestChild = iCandidateChild;
      }
    }
    assert iBestChild != -1;
    return iBestChild;
  }

  /**
   * Adjust the tree after an insertion by making the necessary splits up to the root.
   * @param leafNode the index of the leaf node where the insertion happened
   * @param path the path that lead to the leafNode from the root. The last element is {@code leafNode}
   */
  protected void adjustTree(int leafNode, IntArray path) {
    int newNode = -1;
    if (Node_size(leafNode) > maxCapacity) {
      // Node full. Split into two
      newNode = split(leafNode, minCapacity);
    }
    // AdjustTree. Ascend from the leaf node L
    for (int $i = path.size() - 1; $i >= 0; $i--) {
      int iNode = path.get($i);
      // Adjust covering rectangle in the node
      Node_expand(iNode, ($i == path.size() - 1) ? children.get(iNode).peek() : path.get($i+1));
      if ($i == 0) {
        // The node is the root (no parent)
        if (newNode != -1) {
          // If the root is split, create a new root
          root = Node_createNodeWithChildren(false, iNode, newNode);
        }
        // If N is the root with no partner NN, stop.
      } else {
        int parent = path.get($i - 1);
        if (newNode != -1) {
          // If N has a partner NN resulting from an earlier split,
          // create a new entry ENN and add to the parent if there is room.
          // Add Enn to P if there is room
          Node_addChild(parent, newNode);
          Node_expand(parent, newNode);
          newNode = -1;
          if (Node_size(parent) >= maxCapacity)
            newNode = split(parent, minCapacity);
        }
      }
    }
  }


  /**
   * Linear splitting algorithm as described on Page 52 of the paper.
   * This function should update the MBR of the given node and the newly created node. This function does not
   * necessarily have to update the MBR of the parent node or add the new node to the parent.
   * @param iNode the index of the node to split
   * @param minSplitSize the minimum split size
   * @return the ID of the newly created node after split
   */
  protected int split(int iNode, int minSplitSize) {
    IntArray nodeChildren = children.get(iNode);
    int[] highestLowSide = new int[getNumDimensions()];
    int[] lowestHighSide = new int[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++)
      highestLowSide[d] = lowestHighSide[d] = nodeChildren.get(0);
    for (int iChild = 1; iChild < nodeChildren.size(); iChild++) {
      int child = nodeChildren.get(iChild);
      for (int d = 0; d < getNumDimensions(); d++) {
        if (minCoord[d][child] > minCoord[d][highestLowSide[d]])
          highestLowSide[d] = child;
        if (maxCoord[d][child] < maxCoord[d][lowestHighSide[d]])
          lowestHighSide[d] = child;
      }
    }
    double[] separations = new double[getNumDimensions()];
    int maxSeparationD = 0;
    for (int d = 0; d < getNumDimensions(); d++) {
      separations[d] = (Math.max(globalMin[d], minCoord[d][highestLowSide[d]]) - Math.min(globalMax[d], maxCoord[d][lowestHighSide[d]])) /
          (Math.min(globalMax[d], maxCoord[d][root]) - Math.max(globalMin[d], minCoord[d][root]));
      if (separations[d] > separations[maxSeparationD])
        maxSeparationD = d;
    }
    // The seed points for the two splits resulting from the split
    int seed1 = highestLowSide[maxSeparationD];
    int seed2 = lowestHighSide[maxSeparationD];

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedNodes = nodeChildren.clone();
    Node_reset(iNode, seed1);
    int iNewNode = Node_createNodeWithChildren(isLeaf.get(iNode), seed2);
    nonAssignedNodes.remove(seed1);
    nonAssignedNodes.remove(seed2);
    int group1 = iNode;
    int group2 = iNewNode;
    for (int child : nonAssignedNodes) {
      // If one group has so few entries that all the rest must be assigned to it
      // in order to have the minimum number minSplitSize, assign them and stop
      if (nonAssignedNodes.size() + Node_size(group1) <= minSplitSize) {
        // Assign all the rest to group1
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group1, iObject);
          Node_expand(group1, iObject);
        }
        break;
      } else if (nonAssignedNodes.size() + Node_size(group2) <= minSplitSize) {
        // Assign all the rest to group2
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group2, iObject);
          Node_expand(group2, iObject);
        }
        break;
      } else {
        double d1 = Node_volumeExpansion(group1, child);
        double d2 = Node_volumeExpansion(group2, child);
        if (d1 == d2) {
          // Resolve ties by adding the entry to theh gorup with samller area
          d1 = Node_volume(group1);
          d2 = Node_volume(group2);
          if (d1 == d2) {
            // then to the one wih fewer entries
            d1 = Node_size(group1);
            d2 = Node_size(group2);
            if (d1 == d2) {
              // ... then to either
              d1 = 0.5;
              d2 = Math.random();
            }
          }
        }
        if (d1 < d2) {
          Node_addChild(group1, child);
          Node_expand(group1, child);
        } else if (d1 > d2) {
          Node_addChild(group2, child);
          Node_expand(group2, child);
        }
      }
    }
    return  iNewNode;
  }

  /**
   * Search for all the entries that overlap a given query rectangle
   * @param min the coordinate of the minimum corner
   * @param max the coordinate of the maximum corner
   * @param results the results as a list of entry IDs as given in the construction function
   */
  public void search(double[] min, double[] max, IntArray results) {
    results.clear();
    IntArray nodesToSearch = new IntArray();
    nodesToSearch.add(root);
    while (!nodesToSearch.isEmpty()) {
      int nodeToSearch = nodesToSearch.pop();
      if (isLeaf.get(nodeToSearch)) {
        // Search and return all the entries in the leaf node
        for (int iEntry : children.get(nodeToSearch)) {
          if (Object_overlaps(iEntry, min, max))
            results.add(iEntry);
        }
      } else {
        // A non-leaf node, expand the search to all overlapping children
        for (int iChild : children.get(nodeToSearch)) {
          if (Object_overlaps(iChild, min, max))
            nodesToSearch.add(iChild);
        }
      }
    }
  }

  public Iterable<Entry> search(EnvelopeNDLite e) {
    return new SearchIterator(e);
  }

  public Iterable<Entry> search(double x1, double y1, double x2, double y2) {
    return search(new EnvelopeNDLite(2, x1, y1, x2, y2));
  }

  /**
   * Tests if an object (entry or node) overlaps with a rectangle
   * @param iEntry the inex of the entry
   * @param min the coordinates of the minimum corner of the search box
   * @param max the coordinates of the maximum corner of the search box
   * @return {@code true} if the entry overlaps the given rectangle
   */
  protected boolean Object_overlaps(int iEntry, double[] min, double[] max) {
    for (int d = 0; d < getNumDimensions(); d++) {
      // The first condition addresses the case of an envelope that represents a point that coincides with the
      // lower edge of another envelope
      if (min[d] != minCoord[d][iEntry] &&
          (min[d] >= maxCoord[d][iEntry] || minCoord[d][iEntry] >= max[d]))
        return false;
    }
    return true;
  }

  protected static boolean Object_overlaps(RTreeGuttman rtree1, int iEntry1, RTreeGuttman rtree2, int iEntry2) {
    assert rtree1.getNumDimensions() == rtree2.getNumDimensions() : "Incompatible dimensions";
    for (int d = 0; d < rtree1.getNumDimensions(); d++)
      if (rtree1.minCoord[d][iEntry1] >= rtree2.maxCoord[d][iEntry2] ||
          rtree2.minCoord[d][iEntry2] >= rtree1.maxCoord[d][iEntry1])
        return false;
    return true;
  }

  /**
   * Total number of objects in the tree.
   * @return the number of data entries in the tree
   */
  public int numOfDataEntries() {
    return numEntries;
  }

  /**
   * Returns number of nodes in the tree.
   * @return the number of nodes in the tree
   */
  public int numOfNodes() {
    return numNodes;
  }

  /**
   * Computes the height of the tree which is defined as the number of edges
   * on the path from the root to the deepest node. Sine the R-tree is perfectly
   * balanced, it is enough to measure the length of the path from the root to
   * any node, e.g., the left-most node.
   * @return the height of the tree (number of levels - 1)
   */
  public int getHeight() {
    if (numNodes == 0)
      return 0;
    // Compute the height of the tree by traversing any path from the root
    // to the leaf.
    // Since the tree is balanced, any path would work
    int height = 0;
    int iNode = root;
    while (!isLeaf.get(iNode)) {
      height++;
      iNode = children.get(iNode).get(0);
    }
    return height;
  }

  /**
   * The total number of leaf nodes.
   * @return the total number of leaf nodes
   */
  public int getNumLeaves() {
    return (int) isLeaf.countOnes();
  }

  /**
   * Retrieve all the leaf nodes in the tree.
   * @return an Iterable over all leaf nodes
   */
  public Iterable<Node> getAllLeaves() {
    return new LeafNodeIterable();
  }

  /**
   * Creates an R-tree that contains only nodes (no data entries). The given
   * coordinates are used for the leaf nodes. This tree is used to model an R-tree
   * and use the different R-tree algorithms to test where an entry would end up
   * in the R-tree (without really inserting it).
   * @param x1 the lower x-coordinates of nodes
   * @param y1 the lower y-coordinates of nodes
   * @param x2 the upper x-coordinates of nodes
   * @param y2 the upper y-coordinates of nodes
   * @see #noInsert(double[], double[])
   */
  protected void initializeHollowRTree(double[] x1, double[] y1, double[] x2, double[] y2) {
    // Create a regular R-tree with the given rectangles as data entries.
    initializeFromRects(x1, y1, x2, y2);

    // Make sure we have a room for an extra object which will be used in noInsert
    makeRoomForOneMoreObject();
  }

  /**
   * Simulates an insertion of a record and returns the ID of the object that either
   * contains the given boundaries or will be its sibling.
   * @param min the minimum corner of the box to insert
   * @param max the maximum corner of the box to insert
   * @return the ID of the node that the object would be inserted to
   */
  protected int noInsert(double[] min, double[] max) {
    int i = numEntries + numNodes;
    for (int d = 0; d < getNumDimensions(); d++) {
      minCoord[d][i] = min[d];
      maxCoord[d][i] = max[d];
    }

    // Descend from the root until reaching a data entry
    // The range of IDs for data entries is [0, numEntries[
    // All node IDs is in the rnage [numEntries, numEntries + numNodes[
    int p = root;
    while (p >= numOfDataEntries())
      p = chooseSubtree(i, p);

    // Return the index of the leaf node without really inserting the element
    return p;
  }

  /**
   * Only when the tree is read from file, return the total size of the data part
   * in bytes.
   * @return the total size of data in the file
   */
  public int getTotalDataSize() {
    return totalDataSize;
  }

  public static int spatialJoin(RTreeGuttman rtree1, RTreeGuttman rtree2, BiConsumer<Integer, Integer> result) {
    int numResults = 0;
    assert rtree1.getNumDimensions() == rtree2.getNumDimensions() : "Incompatible dimensions";
    // If the roots are disjoint, end right away
    if (!Object_overlaps(rtree1, rtree1.root, rtree2, rtree2.root))
      return numResults;
    LongArray pairsToTest = new LongArray();
    pairsToTest.add(((long) rtree1.root << 32) | rtree2.root);
    while (!pairsToTest.isEmpty()) {
      long pairToTest = pairsToTest.pop();
      int node1 = (int) (pairToTest >>> 32L);
      int node2 = (int) (pairToTest & 0xffffffff);
      if (rtree1.isLeaf.get(node1) && rtree2.isLeaf.get(node2)) {
        // Both are leaves, test all entries
        for (int child1 : rtree1.children.get(node1)) {
          for (int child2 : rtree2.children.get(node2)) {
            if (Object_overlaps(rtree1, child1, rtree2, child2)) {
              if (result != null)
                result.accept(child1, child2);
              numResults++;
            }
          }
        }
      } else if (rtree1.isLeaf.get(node1)) {
        // First node is leaf but second node is not leaf. Test first node with all children of second node
        for (int child2 : rtree2.children.get(node2)) {
          if (Object_overlaps(rtree1, node1, rtree2, child2))
            pairsToTest.add(((long) node1 << 32) | child2);
        }
      } else if (rtree2.isLeaf.get(node2)) {
        // First node is non-leaf but second node is leaf
        for (int child1 : rtree1.children.get(node1)) {
          if (Object_overlaps(rtree1, child1, rtree2, node2))
            pairsToTest.add(((long) child1 << 32) | node2);
        }
      } else {
        // Both are non-leaves. Cross product all children
        for (int child1 : rtree1.children.get(node1)) {
          for (int child2 : rtree2.children.get(node2)) {
            if (Object_overlaps(rtree1, child1, rtree2, child2)) {
              pairsToTest.add(((long) child1 << 32) | child2);
            }
          }
        }
      }
    }
    return numResults;
  }

  /**
   * A class used to iterate over the data entries in the R-tree
   */
  public class Entry {
    /**The ID of the entry*/
    public int id;
    /**The minimum corner of the box of the entry*/
    public double[] min;
    /**The maximum corner of the box of the entry*/
    public double[] max;

    protected Entry() {
      min = new double[getNumDimensions()];
      max = new double[getNumDimensions()];
    }

    @Override
    public String toString() {
      return String.format("Entry #%d (%f, %f, %f, %f)", id, min[0], min[1], max[0], max[1]);
    }

    public Object getObject() throws IOException {
      if (deser == null)
        return null;
      smartSeek(seekableIn, treeStartOffset + entryOffsets[id]);
      return deser.deserialize(seekableIn/*, entryOffsets[id+1] - entryOffsets[id]*/);
    }
  }

  /**
   * An iterable and iterator that traverses all data entries in the tree.
   */
  protected class EntryIterator implements Iterable<Entry>, Iterator<Entry> {
    private int iNextEntry = 0;
    private final Entry entry = new Entry();

    protected EntryIterator() {}

    @Override
    public Iterator<Entry> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextEntry < RTreeGuttman.this.numEntries;
    }

    @Override
    public Entry next() {
      entry.id = iNextEntry;
      for (int d = 0; d < getNumDimensions(); d++) {
        entry.min[d] = minCoord[d][iNextEntry];
        entry.max[d] = maxCoord[d][iNextEntry];
      }
      iNextEntry++;
      return entry;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  /**
   * Returns an iterable on all data entries in the tree.
   * @return an iterator to all entries in the file
   */
  public Iterable<Entry> entrySet() {
    return new EntryIterator();
  }

  /**
   * An iterator for range query search results
   */
  protected class SearchIterator implements Iterable<Entry>, Iterator<Entry> {
    /**The list of nodes yet to be searched*/
    private IntArray nodesToSearch;

    /**The ID of the entry to return on the next call*/
    private int iNextEntry;

    /**The object used to return all search results*/
    private Entry entry;

    /**The minimum corner of the search range*/
    private double[] min;

    /**The maximum corner of the search range*/
    private double[] max;

    protected SearchIterator(EnvelopeNDLite e) {
      this.min = new double[e.getCoordinateDimension()];
      this.max = new double[e.getCoordinateDimension()];
      for (int $d = 0; $d < e.getCoordinateDimension(); $d++) {
        this.min[$d] = e.getMinCoord($d);
        this.max[$d] = e.getMaxCoord($d);
      }
      searchFirst();
    }

    /**
     * Search for the first element in the result
     */
    protected void searchFirst() {
      nodesToSearch = new IntArray();
      nodesToSearch.add(root);
      entry = new Entry();
      while (!nodesToSearch.isEmpty()) {
        // We keep the top of the stack for the subsequent next calls
        int iNodeToSearch = nodesToSearch.peek();
        if (isLeaf.get(iNodeToSearch)) {
          for (iNextEntry = 0; iNextEntry < Node_size(iNodeToSearch); iNextEntry++) {
            // Found a matching element in a leaf node
            if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), min, max))
              return;
          }
          // Node did not match any records, no longer needed
          nodesToSearch.pop();
        } else {
          // Found a matching non-leaf node, visit its children
          nodesToSearch.pop(); // No longer needed
          for (int iChild : children.get(iNodeToSearch)) {
            if (Object_overlaps(iChild, min, max))
              nodesToSearch.add(iChild);
          }
        }
      }
      iNextEntry = -1;
    }

    protected void prefetchNext() {
      int iNodeToSearch = nodesToSearch.peek();
      while (++iNextEntry < Node_size(iNodeToSearch)) {
        if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), min, max))
          return;
      }
      // Done with the current leaf node. Continue searching for the next leaf
      nodesToSearch.pop();
      while (!nodesToSearch.isEmpty()) {
        iNodeToSearch = nodesToSearch.peek();
        if (isLeaf.get(iNodeToSearch)) {
          for (iNextEntry = 0; iNextEntry < Node_size(iNodeToSearch); iNextEntry++) {
            // Found a matching element in a leaf node
            if (Object_overlaps(children.get(iNodeToSearch).get(iNextEntry), min, max))
              return;
          }
          // Node did not match any records, no longer needed
          nodesToSearch.pop();
        } else {
          // Found a matching non-leaf node, visit its children
          nodesToSearch.pop(); // No longer needed
          for (int iChild : children.get(iNodeToSearch)) {
            if (Object_overlaps(iChild, min, max))
              nodesToSearch.add(iChild);
          }
        }
      }
      iNextEntry = -1; // No more entries to search
    }

    @Override
    public Iterator<Entry> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextEntry != -1;
    }

    @Override
    public Entry next() {
      int iEntry = children.get(nodesToSearch.peek()).get(iNextEntry);
      entry.id = iEntry;
      for (int d = 0; d < getNumDimensions(); d++) {
        entry.min[d] = minCoord[d][iEntry];
        entry.max[d] = maxCoord[d][iEntry];
      }
      prefetchNext();
      return entry;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  /**
   * A class that holds information about one node in the tree.
   */
  public static class Node {
    /**The internal ID of the node*/
    public int id;

    /**Whether this is a leaf node or not*/
    public boolean isLeaf;

    /**The coordinate of the minimum corner of the node*/
    public double[] min;
    /**The coordinate of the maximum corner of the node*/
    public double[] max;

    protected Node(int numDimensions){
      min = new double[numDimensions];
      max = new double[numDimensions];
    }

    public String toWKT() {
      return String.format("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
          min[0], min[1], min[0], max[1], max[0], max[1], max[0], min[1], min[0], min[1]);
    }
  }

  protected class NodeIterable implements Iterable<Node>, Iterator<Node> {
    /**The ID of the next node to be returned*/
    protected int iNextNode;

    /**Current node pointed by the iterator*/
    protected Node currentNode;

    protected NodeIterable() {
      currentNode = new Node(getNumDimensions());
      iNextNode = numEntries - 1;
      prefetchNext();
    }

    protected void prefetchNext() {
      if (iNextNode >= numEntries + numNodes)
        return;
      iNextNode++;
    }

    @Override
    public Iterator<Node> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return iNextNode < numEntries + numNodes;
    }

    @Override
    public Node next() {
      for (int d = 0; d < getNumDimensions(); d++) {
        currentNode.min[d] = minCoord[d][iNextNode];
        currentNode.max[d] = maxCoord[d][iNextNode];
      }
      currentNode.isLeaf = isLeaf.get(iNextNode);
      currentNode.id = iNextNode;
      prefetchNext();
      return currentNode;
    }

    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }

  protected class LeafNodeIterable extends NodeIterable {
    protected void prefetchNext() {
      if (iNextNode >= numEntries + numNodes)
        return;
      do {
        iNextNode++;
      } while (iNextNode < numEntries + numNodes && !isLeaf.get(iNextNode));
    }
  }

  /**
   * An interface for serializing objects given their entry number
   */
  public interface Serializer {
    /**
     * Serializes teh given object to the DataOutput and returns the total number of bytes written
     * @param out the output to serialize this object to
     * @param iObject the index of the object to serialize
     * @return the number of bytes written
     * @throws IOException if an error happens while writing the output
     */
    int serialize(DataOutput out, int iObject) throws IOException;
  }

  public interface Deserializer<O> {
    O deserialize(DataInput in) throws IOException;
  }

  /**
   * Serializes the tree and its data entries to an output stream. Notice that
   * this is not supposed to be used as an external tree format where you can insert
   * and delete entries. Rather, it is like a static copy of the tree where you
   * can search or load back in memory. The format of the tree on disk is as
   * described below.
   * <ul>
   *   <li>
   *     Data Entries: First, all data entries are written in an order that is consistent
   *   with the R-tree structure. This order will guarantee that all data entries
   *   under any node (from the root to leaves) will be adjacent in that order.
   *   </li>
   *   <li>
   *     Tree structure: This part contains the structure of the tree represented
   *   by its nodes. The nodes are stored in a level order traversal. This guarantees
   *   that the root will always be the first node and that all siblings will be
   *   stored consecutively. Each node contains the following information:
   *   (1) (n) Number of children as a 32-bit integer,
   *   (2) n tuples of the form (child offset, MBR=(x1, y1, x2, y2), start offset of data, end offset of data).
   *   The child offset is the offset of the beginning of the child record (node or data entry) in the
   *   tree where 0 is the offset of the first data entry. [Start offset, end offset[ is the range of data entries
   *   that are stored in the subtree under this node.
   *   </li>
   *   <li>
   *     Tree footer: This section contains some meta data about the tree as
   *     follows. All integers are 32-bits.
   *     (1) MBR of the root as (x1, y1, x2, y2),
   *     (2) Number of data entries,
   *     (3) Number of non-leaf nodes,
   *     (4) Number of leaf nodes,
   *     (5) Tree structure offset: offset of the beginning of the tree structure section
   *     (6) Footer offset: offset of the beginning of the footer as a 32-bit integer.
   *     (7) Tree size: Total tree size in bytes including data+structure+footer
   *   </li>
   *
   * </ul>
   * @param out the output to write to
   * @param ser the object serializer
   * @throws IOException if an error happens while writing the file
   */
  public void write(DataOutput out, Serializer ser) throws IOException {
    // Tree data: write the data entries in the tree order
    // Since we write the data first, we will have to traverse the tree twice first time to visit and write
    // the data entries in the tree order, and second time to visit and write the tree nodes in the tree order.
    Deque<Integer> nodesToVisit = new ArrayDeque<>();
    nodesToVisit.add(root);
    int[] objectStartOffsets = new int[numOfDataEntries() + numOfNodes()];
    int[] objectSizes = new int[numOfDataEntries() + numOfNodes()];
    // Keep track of the offset of each data object from the beginning of the data section
    int dataOffset = 0;
    // Keep track of the offset of each node from the beginning of the tree structure section
    int nodeOffset = 0;
    while (!nodesToVisit.isEmpty()) {
      int node = nodesToVisit.removeFirst();
      // The node is supposed to be written in this order.
      // Measure its offset and accumulate its size
      objectStartOffsets[node] = nodeOffset;
      // nodeSizeInBytes = (Number of children) + ((Child Offset + MBR(2k coords) + Data start + Data End) * Number of children)
      int nodeSizeInBytes = 4 + (4 + 8 * 2 * this.getNumDimensions() + 4 + 4) * Node_size(node);
      objectSizes[node] = nodeSizeInBytes;
      nodeOffset += nodeSizeInBytes;

      if (isLeaf.get(node)) {
        // Leaf node, write the data entries in order
        for (int child : children.get(node)) {
          objectStartOffsets[child] = dataOffset;
          if (ser != null) {
            objectSizes[child] = ser.serialize(out, child);;
            dataOffset += objectSizes[child];
          }
        }
      } else {
        // Internal node, recursively traverse its children
        for (int child : children.get(node))
          nodesToVisit.addLast(child);
      }
    }
    LOG.debug(String.format("Total data size in R-tree: %d bytes and %d records", dataOffset, numOfDataEntries()));
    // Update node offsets as they are written after the data entries
    for (int i = 0; i < numNodes; i++)
      objectStartOffsets[i + numEntries] += dataOffset;

    // Tree structure: Write the nodes in tree order
    nodesToVisit.add(root);
    while (!nodesToVisit.isEmpty()) {
      int node = nodesToVisit.removeFirst();
      // (1) Number of children
      out.writeInt(Node_size(node));
      for (int child : children.get(node)) {
        // (2) Write the offset of the child
        out.writeInt(objectStartOffsets[child]);
        // (3) Write the MBR of each child
        for (int d = 0; d < getNumDimensions(); d++) {
          out.writeDouble(minCoord[d][child]);
          out.writeDouble(maxCoord[d][child]);
        }
        // (4) Write the start and end offset of the data under this node
        int i$ = child;
        while (i$ >= numOfDataEntries())
          i$ = children.get(i$).get(0);
        // Write start offset (Start offset of the left-most child)
        out.writeInt(objectStartOffsets[i$]);
        // Write end offset (End offset of the right-most child)
        i$ = child;
        while (i$ >= numOfDataEntries())
          i$ = children.get(i$).peek();
        out.writeInt(objectStartOffsets[i$] + objectSizes[i$]);
      }
      // If node is internal, add its children to the nodes to be visited
      if (!isLeaf.get(node)) {
        for (int child : children.get(node))
          nodesToVisit.addLast(child);
      }
    }

    LOG.debug("Total size of R-tree structure: "+nodeOffset);

    // Tree footer
    int footerOffset = dataOffset + nodeOffset;
    // (1) MBR of the root
    // The dimension is written once in the footer
    out.writeInt(getNumDimensions());
    for (int d = 0; d < getNumDimensions(); d++) {
      out.writeDouble(minCoord[d][root]);
      out.writeDouble(maxCoord[d][root]);
    }
    // (2) Number of data entries
    out.writeInt(numOfDataEntries());
    // (3) Number of non-leaf nodes
    out.writeInt((int) (numOfNodes() - isLeaf.countOnes()));
    // (4) Number of leaf nodes
    out.writeInt((int) isLeaf.countOnes());
    // (5) Offset of the tree structure section
    out.writeInt(dataOffset);
    // (6) Offset of the footer
    out.writeInt(footerOffset);
    // (7) Size of the entire tree
    int footerSize = 4 + 8 * 2 * getNumDimensions() + 4 * 5;
    out.writeInt(footerOffset + footerSize);
  }

  /**
   * Read an R-tree stored using the method {@link #write(DataOutput, Serializer)}
   * @param in the input stream to read from
   * @param length the length of the R-tree in bytes
   * @param deser the deserializer that reads the data objects
   * @throws IOException if an error happens while reading the file
   */
  public void readFields(FSDataInputStream in, long length, Deserializer<?> deser) throws IOException {
    this.seekableIn = in;
    this.deser = deser;
    this.treeStartOffset = seekableIn.getPos();
    seekableIn.seek(treeStartOffset + length - 8);
    int footerOffset = seekableIn.readInt();
    seekableIn.seek(treeStartOffset + footerOffset);
    int numDimensions = seekableIn.readInt();
    double[] rootMin = new double[numDimensions];
    double[] rootMax = new double[numDimensions];
    for (int d = 0; d < numDimensions; d++) {
      rootMin[d] = seekableIn.readDouble();
      rootMax[d] = seekableIn.readDouble();
    }
    this.numEntries = seekableIn.readInt();
    int numNonLeaves = seekableIn.readInt();
    int numLeaves = seekableIn.readInt();
    this.numNodes = numNonLeaves + numLeaves;
    int treeStructureOffset = seekableIn.readInt();
    this.totalDataSize = treeStructureOffset;

    // Initialize the data structures to store objects
    this.minCoord = new double[numDimensions][numEntries + numNodes];
    this.maxCoord = new double[numDimensions][numEntries + numNodes];
    this.isLeaf = new BitArray(numEntries + numNodes);
    this.children = new ArrayList<IntArray>(numEntries + numNodes);
    for (int i = 0; i < numEntries + numNodes; i++)
      this.children.add(null);

    // Read the tree structure and keep it all in memory
    // First, scan the nodes once to map node offsets to IDs
    // Map the offset of some nodes to their index in the node list
    Map<Integer, Integer> nodeOffsetToIndex = new HashMap<Integer, Integer>();
    seekableIn.seek(treeStartOffset + treeStructureOffset);
    int nodeID = numEntries;
    this.root = nodeID; // First node is always the root
    // Store root MBR
    for (int d = 0; d < numDimensions; d++) {
      minCoord[d][root] = rootMin[d];
      maxCoord[d][root] = rootMax[d];
    }

    while (nodeID < this.numNodes + this.numEntries) {
      int nodeOffset = (int) (seekableIn.getPos() - treeStartOffset);
      nodeOffsetToIndex.put(nodeOffset, nodeID);
      int nodeSize = seekableIn.readInt();
      seekableIn.skipBytes(nodeSize * (4 + 8 * 2 * numDimensions + 8)); // Skip offset, MBR, and data [start,end[ offsets
      nodeID++;
    }
    // Second, read nodes data and store them in the object
    seekableIn.seek(treeStartOffset + treeStructureOffset);
    nodeID = numEntries;
    int entryID = 0; // Number entries starting at zero
    entryOffsets = new int[numEntries+1];
    while (nodeID < this.numNodes + numEntries) {
      boolean leafNode = nodeID >= (numEntries + numNonLeaves);
      isLeaf.set(nodeID, leafNode);
      // (1) Node size
      int nodeSize = seekableIn.readInt();
      // (2) Offset of the first child
      IntArray nodeChildren = new IntArray();
      children.set(nodeID, nodeChildren);
      for (int i = 0; i < nodeSize; i++) {
        int childOffset = seekableIn.readInt();
        int childID = leafNode ? entryID++ : nodeOffsetToIndex.get(childOffset);
        if (leafNode)
          entryOffsets[childID] = childOffset;
        nodeChildren.add(childID);
        // (3) Child MBR
        // TODO read the entire array instead of looping over them one by one
        for (int d = 0; d < numDimensions; d++) {
          minCoord[d][childID] = seekableIn.readDouble();
          maxCoord[d][childID] = seekableIn.readDouble();
        }
        seekableIn.skipBytes(8); // Skip start and end offsets
      }
      nodeID++;
    }
    entryOffsets[entryID] = treeStructureOffset;
  }

  public void close() throws IOException {
    if (seekableIn != null)
      seekableIn.close();
  }

  public static<T> Iterable<T> search(FSDataInputStream in, long treeLength, double[] minCoord, double[] maxCoord,
                                       Deserializer<T> deser) throws IOException {
    return new DiskSearchIterator<T>(in, treeLength, minCoord, maxCoord, deser);
  }

  public static<T> Iterable<T> readAll(FSDataInputStream in, long treeLength, Deserializer<T> deser) throws IOException {
    return new DiskSearchIterator<T>(in, treeLength, deser);
  }

  /**
   * Seek to the given position or skip bytes to avoid seek. Helps with reading from a remote file system
   * where the seek operation is very expensive.
   * @param in the input stream to seek
   * @param newPos the position to seek to
   * @throws IOException if an error happens while reading the file
   */
  private static void smartSeek(FSDataInputStream in, long newPos) throws IOException {
    if (in.getPos() < newPos)
      in.skip(newPos - in.getPos());
    else
      in.seek(newPos);
    assert in.getPos() == newPos : "Seek failed";
  }


  /**
   * Searches data on disk.
   */
  public static class DiskSearchIterator<T> implements Iterable<T>, Iterator<T>, Closeable {

    /**The starting offset of the tree*/
    final long treeStartOffset;

    /**The data ranges to return as a result. Every pair of offsets make a [start, end[ range to return*/
    final IntArray rangesToReturn;

    /**An input stream to the underlying R-tree*/
    final FSDataInputStream in;

    /**The range where the next record belongs to*/
    protected int rangeOfNextElement;

    /**The deserializer that parses data entries*/
    final Deserializer<T> deser;

    public DiskSearchIterator(FSDataInputStream in, long length, Deserializer<T> deser) throws IOException {
      this.in  = in;
      rangesToReturn = new IntArray();
      this.deser = deser;
      // Read the header (or whatever is needed out of it)
      this.treeStartOffset = in.getPos();
      in.seek(treeStartOffset + length - 8);
      int footerOffset = in.readInt();
      in.seek(treeStartOffset + footerOffset);
      int numDimensions = in.readInt();
      in.skip(8 * numDimensions * 2); // Skip root MBR
      in.skip(4 + 4 + 4); // Skip number elements and nodes (leaf and non-leaf)
      int treeStructureOffset = in.readInt();
      rangesToReturn.add(0);
      rangesToReturn.add(treeStructureOffset);
      // Move the offset to the first data element (if exists)
      if (rangeOfNextElement < rangesToReturn.size())
        in.seek(treeStartOffset + rangesToReturn.get(rangeOfNextElement));
    }

    public DiskSearchIterator(FSDataInputStream in, long length, double[] searchBoxMin, double[] searchBoxMax, Deserializer<T> deser)
        throws IOException {
      this.in = in;
      rangesToReturn = new IntArray();
      this.deser = deser;
      // Read the header (or whatever is needed out of it)
      this.treeStartOffset = in.getPos();
      in.seek(treeStartOffset + length - 8);
      int footerOffset = in.readInt();
      in.seek(treeStartOffset + footerOffset);
      int numDimensions = in.readInt();
      assert numDimensions == searchBoxMin.length;
      boolean overlaps = true; // The search box overlaps the node
      boolean contains = true; // THe search box contains the node
      for (int d$ = 0; d$ < numDimensions; d$++) {
        double nodeMin = in.readDouble();
        double nodeMax = in.readDouble();
        if (!(nodeMin >= searchBoxMin[d$] && nodeMax <= searchBoxMax[d$]))
          contains = false;
        if (nodeMin >= searchBoxMax[d$] || searchBoxMin[d$] >= nodeMax)
          overlaps = false;
      }
      if (!overlaps)
        // No results found
        return;
      /*this.numEntries = */in.readInt();
      int numNonLeaves = in.readInt();
      int numLeaves = in.readInt();
      int numNodes = numNonLeaves + numLeaves;
      int treeStructureOffset = in.readInt();

      if (contains) {
        // The root is fully contained in the search box, all results match
        this.rangesToReturn.add(0);
        this.rangesToReturn.add(treeStructureOffset);
        // Seek to the first element
        rangeOfNextElement = 0;
        in.seek(treeStartOffset + rangesToReturn.get(rangeOfNextElement));
        return;
      }
      // The root overlaps the search box. Search the R-tree starting at the root.
      Deque<Integer> nodesToSearch = new ArrayDeque<>();
      nodesToSearch.addLast(treeStructureOffset); // Start at the root node (first node)
      while (!nodesToSearch.isEmpty()) {
        int nodeToSearch = nodesToSearch.removeFirst();
        smartSeek(in, treeStartOffset + nodeToSearch);
        int numChildren = in.readInt();
        while (numChildren-- > 0) {
          int childOffset = in.readInt();
          overlaps = true; // The search box overlaps the node
          contains = true; // The search box contains the node
          for (int d$ = 0; d$ < numDimensions; d$++) {
            double nodeMin = in.readDouble();
            double nodeMax = in.readDouble();
            if (!(nodeMin >= searchBoxMin[d$] && nodeMax <= searchBoxMax[d$]))
              contains = false;
            if (nodeMin >= searchBoxMax[d$] || searchBoxMin[d$] >= nodeMax)
              overlaps = false;
          }
          int dataStartOffset = in.readInt();
          int dataEndOffset = in.readInt();
          if (contains) {
            // All data contents match
            rangesToReturn.add(dataStartOffset);
            rangesToReturn.add(dataEndOffset);
          } else if (overlaps) {
            // Need to further search this child
            if (childOffset >= treeStructureOffset) {
              // The child is a node
              nodesToSearch.addLast(childOffset);
            } else {
              // This is not a node, it is an object
              rangesToReturn.add(dataStartOffset);
              rangesToReturn.add(dataEndOffset);
            }
          }
        }
      }
      // Move the offset to the first data element (if exists)
      if (rangeOfNextElement < rangesToReturn.size())
        in.seek(treeStartOffset + rangesToReturn.get(rangeOfNextElement));
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return rangeOfNextElement < rangesToReturn.size();
    }

    @Override
    public T next() {
      // If the iterator has already reached the end
      if (rangeOfNextElement >= rangesToReturn.size())
        return null;
      try {
        T result = deser.deserialize(in);
        // Move on to the next element
        int offsetOfNextElement = (int) (in.getPos() - treeStartOffset);
        if (offsetOfNextElement >= rangesToReturn.get(rangeOfNextElement + 1)) {
          // Done with the current range, go to the next range
          rangeOfNextElement += 2;
          if (rangeOfNextElement < rangesToReturn.size()) {
            // More elements to return on the next call
            smartSeek(in, treeStartOffset + rangesToReturn.get(rangeOfNextElement));
          }
        }
        // In all cases, return the current element
        return result;
      } catch (IOException e) {
        throw new RuntimeException("Error reading data from the R-tree", e);
      }
    }

    /**
     * Returns the progress in the range [0,1]. Helpful to report the progress with MapReduce.
     * @return the progress of reading. Can be used to report the progress of a long job.
     * @throws IOException if an error happens while getting the position at the current file
     */
    public float getProgress() throws IOException {
      float progressOfCurrentRange = (in.getPos() - treeStartOffset) /
          (float) (rangesToReturn.get(rangeOfNextElement + 1) - rangesToReturn.get(rangeOfNextElement));
      float progress = (float) rangeOfNextElement / rangesToReturn.size()
          + progressOfCurrentRange * 2 / rangesToReturn.size();
      return progress;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /**
   * Writes all nodes of the tree in a WKT format to be visualized in QGIS.
   * @param out the print stream to write the output to, e.g., System.out
   */
  public void toWKT(PrintStream out) {
    for (Node node : new NodeIterable()) {
      out.printf("%d\t%s\tPOLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))\n",
          node.id,
          isLeaf.get(node.id) ? "leaf" : "inner",
          node.min[0], node.min[1],
          node.min[0], node.max[1],
          node.max[0], node.max[1],
          node.max[0], node.min[1],
          node.min[0], node.min[1]
      );
    }
    for (int i = 0; i < numOfDataEntries(); i++)
      out.printf("%d\t%s\tPOLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))\n",
          i,
          "data",
          minCoord[0][i], minCoord[1][i],
          minCoord[0][i], maxCoord[1][i],
          maxCoord[0][i], maxCoord[1][i],
          maxCoord[0][i], minCoord[1][i],
          minCoord[0][i], minCoord[1][i]
      );
  }

  /**
   * Assigns an entry to a group based on the R-tree paper (Page 52, Step QS3)
   * @param mbr1 the MBR of the first group
   * @param size1 the size of the first group
   * @param mbr2 the MBR of the second group
   * @param size2 the size of the second group
   * @param coords the two-dimensional array of all coordinates
   * @param iPoint the index of the point to consider
   * @return the index of the group to assign the record to, 0 or 1
   */
  static int chooseGroup(EnvelopeNDLite mbr1, int size1, EnvelopeNDLite mbr2, int size2, double[][] coords, int iPoint) {
    // Compute the volume for each group before and after adding the given point
    double vol1Before = 1.0, vol1After = 1.0;
    double vol2Before = 1.0, vol2After = 1.0;
    for (int d = 0; d < coords.length; d++) {
      // Compute the side length before/after adding the given point for the first group
      double sideLength = mbr1.getSideLength(d);
      vol1Before *= mbr1.getSideLength(d);
      if (coords[d][iPoint] < mbr1.getMinCoord(d))
        sideLength += mbr1.getMinCoord(d) - coords[d][iPoint];
      if (coords[d][iPoint] > mbr1.getMaxCoord(d))
        sideLength += coords[d][iPoint] - mbr1.getMaxCoord(d);
      vol1After *= sideLength;

      // Do the same for the other group
      sideLength = mbr2.getSideLength(d);
      vol2Before *= mbr2.getSideLength(d);
      if (coords[d][iPoint] < mbr2.getMinCoord(d))
        sideLength += mbr2.getMinCoord(d) - coords[d][iPoint];
      if (coords[d][iPoint] > mbr2.getMaxCoord(d))
        sideLength += coords[d][iPoint] - mbr2.getMaxCoord(d);
      vol2After *= sideLength;
    }
    // Compute the volume expansion for each group
    double d1 = vol1After - vol1Before;
    double d2 = vol2After - vol2Before;
    if (d1 == d2) {
      // Resolve ties by adding the entry to the group with smaller area
      d1 = vol1Before;
      d2 = vol2Before;
      if (d1 == d2) {
        // then to the one wih fewer entries
        d1 = size1;
        d2 = size2;
        if (d1 == d2) {
          // ... then to either
          d1 = 0.5;
          d2 = Math.random();
        }
      }
    }
    return d1 < d2 ? 0 : 1;
  }

  /**
   * Partitions the given set of points using the linear split algorithm to produce a set of partitions where each one
   * has between minPartitionSize and maxPartitionSize points, inclusively. The parameter fractionMinSplitSize can be
   * set to any number
   * @param coords a two-dimensional array of coordinates [axis][point#]
   * @param minPartitionSize the minimum partition size (inclusive)
   * @param maxPartitionSize the maximum partition size (inclusive)
   * @param fractionMinSplitSize the fraction of the minimum split size [0, 0.5]
   * @return the boundaries of the partitions
   */
  static EnvelopeNDLite[] partitionPoints(double[][] coords, int minPartitionSize,
                                          int maxPartitionSize, float fractionMinSplitSize) {
    class Range {
      /**The range of points to partition [start, end)*/
      int start, end;

      Range(int s, int e) {
        this.start = s;
        this.end = e;
      }
    }

    // The ranges that might need to be split
    int numDimensions = coords.length;
    int numPoints = coords[0].length;
    assert RStarTree.isValid(numPoints, minPartitionSize, maxPartitionSize);
    Stack<Range> rangesToSplit = new Stack<Range>();
    rangesToSplit.push(new Range(0, numPoints));

    // The output list of partitions
    List<EnvelopeNDLite> partitions = new ArrayList<>();

    // Compute the MBR of all points to be able to normalize the separation
    EnvelopeNDLite mbr = null;

    // Create the temporary objects here to avoid creating them multiple times
    double[] tempCoord = new double[numDimensions];
    int[] pointWithMinCoord = new int[numDimensions];
    int[] pointWithMaxCoord = new int[numDimensions];
    EnvelopeNDLite mbr1 = new EnvelopeNDLite(numDimensions);
    EnvelopeNDLite mbr2 = new EnvelopeNDLite(numDimensions);

    while (!rangesToSplit.empty()) {
      Range r = rangesToSplit.pop();
      if (r.end - r.start <= maxPartitionSize) {
        // No need to further split this range. Report it to the answer.
        EnvelopeNDLite partition = new EnvelopeNDLite(numDimensions);
        for (int d = 0; d < numDimensions; d++) {
          for (int iPoint = r.start; iPoint < r.end; iPoint++) {
            partition.setMinCoord(d, Math.min(partition.getMinCoord(d), coords[d][iPoint]));
            partition.setMaxCoord(d, Math.max(partition.getMaxCoord(d), coords[d][iPoint]));
          }
        }
        partitions.add(partition);
      } else {
        // Apply the linear-time R-tree splitting algorithm
        // First, pick the two seeds the have the largest separation
        for (int d = 0; d < numDimensions; d++) {
          pointWithMinCoord[d] = pointWithMaxCoord[d] = r.start;
          for (int iPoint = r.start+1; iPoint < r.end; iPoint++) {
            if (coords[d][iPoint] < coords[d][pointWithMinCoord[d]])
              pointWithMinCoord[d] = iPoint;
            if (coords[d][iPoint] > coords[d][pointWithMaxCoord[d]])
              pointWithMaxCoord[d] = iPoint;
          }
        }
        // Compute the MBR for the very first group to normalize separations
        if (mbr == null) {
          mbr = new EnvelopeNDLite(numDimensions);
          for (int d = 0; d < numDimensions; d++) {
            mbr.setMinCoord(d, coords[d][pointWithMinCoord[d]]);
            mbr.setMaxCoord(d, coords[d][pointWithMaxCoord[d]]);
          }
        }
        // Pick the two seeds that have the largest normalized separation
        int seed1 = -1, seed2 = -1;
        double largestNormalizedSeparation = Double.NEGATIVE_INFINITY;
        for (int d = 0; d < numDimensions; d++) {
          double normalizedSeparation = (coords[d][pointWithMaxCoord[d]] - coords[d][pointWithMinCoord[d]]) /
              mbr.getSideLength(d);
          if (normalizedSeparation > largestNormalizedSeparation) {
            seed1 = pointWithMinCoord[d];
            seed2 = pointWithMaxCoord[d];
            largestNormalizedSeparation = normalizedSeparation;
          }
        }
        assert seed1 != -1 && seed2 != -1;
        // Swap seed1 with range.start and seed2 with range.end-1
        for (int d = 0; d < numDimensions; d++) {
          double temp = coords[d][r.start];
          coords[d][r.start] = coords[d][seed1];
          coords[d][seed1] = temp;

          temp = coords[d][r.end-1];
          coords[d][r.end-1] = coords[d][seed2];
          coords[d][seed2] = temp;
        }

        // Initialize the MBR of the two groups
        mbr1.setEmpty();
        mbr2.setEmpty();
        for (int d = 0; d < numDimensions; d++) {
          mbr1.setMinCoord(d, coords[d][r.start]);
          mbr1.setMaxCoord(d, coords[d][r.start]);
          mbr2.setMinCoord(d, coords[d][r.end-1]);
          mbr2.setMaxCoord(d, coords[d][r.end-1]);
        }
        // Split the range [r.start, r.end) so that the first group is [r.start, i] and the second group is
        // [j, r.end)
        int i = r.start;
        int j = r.end - 1;
        while (i < j) {
          int group;
          // Advance i as long as the element at i belongs to the first group
          while (i < j && chooseGroup(mbr1, i - r.start, mbr2, r.end - j, coords, i) ==  0) {
            for (int d = 0; d < numDimensions; d++)
              tempCoord[d] = coords[d][i];
            mbr1.merge(tempCoord);
            i++;
          }
          // Decrease j as long as the element at j belongs to the second group
          while (i < j && chooseGroup(mbr1, i - r.start, mbr2, r.end - j, coords, j) ==  1) {
            for (int d = 0; d < numDimensions; d++)
              tempCoord[d] = coords[d][j];
            mbr2.merge(tempCoord);
            j--;
          }
          // Swap the elements at i and j and continue
          if (i < j) {
            double temp;
            for (int d = 0; d < numDimensions; d++) {
              temp = coords[d][i];
              coords[d][i] = coords[d][j];
              coords[d][j] = temp;
            }
          }
          // Check if all the remaining items need to be assigned to one group to  meet the minimum size constraint
          if (r.end - i <= minPartitionSize)
            j=i;
          else if (j - r.start <= minPartitionSize)
            i = j;
        }
        // Now split around i and j (notice that i == j at this point)
        // Ensure that the two partitions are valid
        int separator = i;
        int diff = r.end - separator > separator - r.start?+1 : -1;
        while (!RStarTree.isValid(separator - r.start, minPartitionSize, maxPartitionSize) ||
            !RStarTree.isValid(r.end - separator, minPartitionSize, maxPartitionSize)) {
          separator += diff;
        }

        Range newRange = new Range(separator, r.end);
        r.end = separator;
        rangesToSplit.push(r);
        rangesToSplit.push(newRange);
      }
    }

    return partitions.toArray(new EnvelopeNDLite[partitions.size()]);
  }


  public static int spatialJoinIntersectsIndex(List<Geometry> r, List<Geometry> s, SpatialJoinAlgorithms.SJResult results) {
    if (r.size() == 0 || s.size() == 0)
      return 0;
    assert GeometryHelper.getCoordinateDimension(r.get(0)) == 2 : "R should be 2D geometries";
    assert GeometryHelper.getCoordinateDimension(s.get(0)) == 2 : "S should be 2D geometries";
    double[][] coordsR = getMBRs(r);
    double[][] coordsS = getMBRs(s);
    RTreeGuttman rtree1 = new RRStarTree(10, 40);
    rtree1.initializeFromRects(coordsR[0], coordsR[1], coordsR[2], coordsR[3]);
    RTreeGuttman rtree2 = new RRStarTree(10, 40);
    rtree2.initializeFromRects(coordsS[0], coordsS[1], coordsS[2], coordsS[3]);
    double[] refPoint = new double[2];
    return RTreeGuttman.spatialJoin(rtree1, rtree2, (i,j) -> {
      // Compute the reference point
      if (r.get(i).intersects((s.get(j)))) {
        refPoint[0] = Math.max(coordsR[0][i], coordsS[0][j]);
        refPoint[1] = Math.max(coordsR[1][i], coordsS[1][j]);
        results.accept(i, j, refPoint);
      }
    });
  }

  public static int spatialJoinIntersectsIndexFeatures(List<IFeature> r, List<IFeature> s, SpatialJoinAlgorithms.SJResult results) {
    if (r.size() == 0 || s.size() == 0)
      return 0;
    assert GeometryHelper.getCoordinateDimension(r.get(0).getGeometry()) == 2 : "R should be 2D geometries";
    assert GeometryHelper.getCoordinateDimension(s.get(0).getGeometry()) == 2 : "S should be 2D geometries";
    long t1 = System.nanoTime();
    double[][] coordsR = getMBRs2(r);
    double[][] coordsS = getMBRs2(s);
    RTreeGuttman rtree1 = new RRStarTree(10, 40);
    rtree1.initializeFromRects(coordsR[0], coordsR[1], coordsR[2], coordsR[3]);
    RTreeGuttman rtree2 = new RRStarTree(10, 40);
    rtree2.initializeFromRects(coordsS[0], coordsS[1], coordsS[2], coordsS[3]);
    long t2 = System.nanoTime();
    double[] refPoint = new double[2];
    int resultSize = RTreeGuttman.spatialJoin(rtree1, rtree2, (i, j) -> {
      // Compute the reference point
      refPoint[0] = Math.max(coordsR[0][i], coordsS[0][j]);
      refPoint[1] = Math.max(coordsR[1][i], coordsS[1][j]);
      results.accept(i, j, refPoint);
    });
    long t3 = System.nanoTime();
    LOG.info(String.format("Built two R-trees of sizes %d and %d in %f seconds and joined in %f seconds to produce %d results",
            rtree1.numOfDataEntries(), rtree2.numOfDataEntries(), (t2-t1)*1E-9, (t3-t2)*1E-9, resultSize));
    return resultSize;
  }
}
