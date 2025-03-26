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
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * An R*-tree implementation based on the design in the following paper.
 *
 * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
 * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles.
 * SIGMOD Conference 1990: 322-331
 */
public class RStarTree extends RTreeGuttman {
  private static final Log LOG = LogFactory.getLog(RStarTree.class);

  /**Number of entries to delete and reinsert if a forced re-insert is needed*/
  protected int p;

  /**A flag set to true while a reinserting is in action to avoid cascade reinsertions*/
  protected boolean reinserting;

  public RStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
    p = maxCapcity * 3 / 10;
  }

  /**
   * Tests if the MBR of a node fully contains an object
   * @param node the ID of the node
   * @param object the ID of the object
   * @return {@code true} if the object is contained in the node boundary
   */
  protected boolean Node_contains(int node, int object) {
    for (int d = 0; d < getNumDimensions(); d++) {
      if (minCoord[d][object] < minCoord[d][node] || maxCoord[d][object] > maxCoord[d][node])
        return false;
    }
    return true;
  }

  /**
   * Compute the enlargement of the overlap of two nodes if an object is added to the first node.
   * In other words, it computes Volume((T U O) ^ J) - Volume(T ^ J),
   * where T and J are the first and second noes, respectively, and O is the object
   * to be added to T.
   * @param nodeT the ID of the node T
   * @param object the ID of the object
   * @param nodeJ the ID of the node J
   * @return the overlap volume enlargement
   */
  protected double Node_overlapVolumeEnlargement(int nodeT, int object, int nodeJ) {
    double volTJ = 1.0; // Volume (T ^ J)
    double volTOJ = 1.0; // Volume ((T U O) ^ J)

    for (int d = 0; d < getNumDimensions(); d++) {
      double lengthTJ = Math.min(maxCoord[d][nodeJ], maxCoord[d][nodeT]) -
          Math.max(minCoord[d][nodeJ], minCoord[d][nodeT]);
      double lengthTOJ = Math.min(Math.max(maxCoord[d][nodeT], maxCoord[d][object]), maxCoord[d][nodeJ]) -
          Math.max(Math.min(minCoord[d][nodeT], minCoord[d][object]), minCoord[d][nodeJ]);

      volTJ *= Math.max(0.0, lengthTJ);
      volTOJ *= Math.max(0.0, lengthTOJ);
    }
    return volTOJ - volTJ;
  }

  @Override
  protected int chooseSubtree(final int entry, int node) {
    assert !isLeaf.get(node);
    // If the child pointers in N do not point to leaves,
    // determine the minimum area cost (as in regular R-tree)
    if (!isLeaf.get(children.get(node).peek()))
      return super.chooseSubtree(entry, node);

    // If the child pointers in N point ot leaves, determine the minimum
    // overlap cost
    int bestChild = -1;
    double minVolume = Double.POSITIVE_INFINITY;
    final IntArray nodeChildren = children.get(node);
    // If there are any nodes that completely covers the entry, choose the one
    // with the least area
    for (int child : nodeChildren) {
      if (Node_contains(child, entry)) {
        double volume = Node_volume(child);
        if (volume < minVolume) {
          bestChild = child;
          minVolume = volume;
        }
      }
    }
    // Found a zero-enlargement child with least volume
    if (bestChild != -1)
      return bestChild;

    // From this point on, we know that ALL children have to be expanded

    // Sort the children by their increasing order of area enlargements so that
    // we can reduce the processing by considering the first P=32 children
    final double[] volumeEnlargements = new double[nodeChildren.size()];
    for (int iChild = 0; iChild < nodeChildren.size(); iChild++)
      volumeEnlargements[iChild] = Node_volumeExpansion(nodeChildren.get(iChild), entry);
    IndexedSortable volEnlargementsSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = volumeEnlargements[i] - volumeEnlargements[j];
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
        double temp = volumeEnlargements[i];
        volumeEnlargements[i] = volumeEnlargements[j];
        volumeEnlargements[j] = temp;
      }
    };
    new QuickSort().sort(volEnlargementsSortable, 0, nodeChildren.size());
    // Choose the entry N whose rectangle needs least overlap enlargement
    // to include the new data rectangle.
    double minOverlapEnlargement = Double.POSITIVE_INFINITY;
    double minVolumeEnlargement = Double.POSITIVE_INFINITY;

    // For efficiency, only consider the first 32 children in the sorted order
    // of volume expansion
    for (int iChild = 0; iChild < Math.min(32, nodeChildren.size()); iChild++) {
      int child = nodeChildren.get(iChild);
      double ovlpEnlargement = 0.0;
      double volumeEnlargement = volumeEnlargements[iChild];
      // If the MBB of the node expands, there could be some enlargement
      for (int child2 : nodeChildren) {
        if (child != child2) {
          // Add the overlap and volume enlargements of this pair
          ovlpEnlargement += Node_overlapVolumeEnlargement(child, entry, child2);
        }
      }
      if (ovlpEnlargement < minOverlapEnlargement) {
        // Choose the entry whose rectangle needs least overlap enlargement to
        // include the new data rectangle
        bestChild = child;
        minOverlapEnlargement = ovlpEnlargement;
        minVolumeEnlargement = volumeEnlargement;
      } else if (ovlpEnlargement == minOverlapEnlargement) {
        // Resolve ties by choosing the entry whose rectangle needs least area enlargement
        if (volumeEnlargement < minVolumeEnlargement) {
          minVolumeEnlargement = volumeEnlargement;
          bestChild = child;
        } else if (volumeEnlargement == minVolumeEnlargement) {
          // then the entry with rectangle of smallest area
          if (Node_volume(child) < Node_volume(bestChild))
            bestChild = child;
        }
      }
    }
    assert bestChild != -1;
    return bestChild;
  }

  /**
   * Treats a node that ran out of space by either forced reinsert of some entries or splitting.
   * @param iLeafNode the leaf node that overflew
   * @return the index of the newly created node if the overflow is treated by splitting. Otherwise, -1
   */
  protected int overflowTreatment(int iLeafNode) {
    if (iLeafNode != root && !reinserting) {
      // If the level is not the root level and this is the first call of
      // overflowTreatment in the given level during the insertion of one entry
      // invoke reInsert
      reInsert(iLeafNode);
      // Return -1 which indicates no split happened.
      // Although we call insert recursively which might result in another split,
      // even in the same node, the recursive call will handle its split correctly
      // As long as the ID of the given node and the path to the root do not
      // change, this method should work fine.
      return -1;
    } else {
      return split(iLeafNode, minCapacity);
    }
  }

  /**
   * Delete and reinsert p elements from the given overflowing leaf node.
   * Described in Beckmann et al'90 Page 327
   * @param node the ID of the node
   */
  protected void reInsert(int node) {
    reinserting = true;
    final IntArray nodeChildren = children.get(node);
    // Remove the last element (the one that caused the expansion)
    int overflowEelement = nodeChildren.pop();
    // RI1 For all M+1 entries of a node N, compute the distance between
    // the centers of their rectangles and the center of the MBR of N

    final double[] distances = new double[nodeChildren.size()];
    for (int d = 0; d < getNumDimensions(); d++) {
      double nodeCenter = (minCoord[d][node] + maxCoord[d][node]) / 2.0;
      for (int iChild = 0; iChild < nodeChildren.size(); iChild++) {
        int child = nodeChildren.get(iChild);
        double childCenter = (minCoord[d][child] + maxCoord[d][child]) / 2.0;
        distances[iChild] += (childCenter - nodeCenter) * (childCenter - nodeCenter);
      }
    }
    // RI2 Sort the entries in decreasing order of their distances
    // Eldawy: We choose to sort them by increasing order and removing the
    // the last p entries because removing the last elements from an array
    // is simpler
    IndexedSortable distanceSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff = distances[i] - distances[j];
        if (diff > 0) return -1;
        if (diff < 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        nodeChildren.swap(i, j);
        double temp = distances[i];
        distances[i] = distances[j];
        distances[j] = temp;
      }
    };
    new QuickSort().sort(distanceSortable, 0, nodeChildren.size());

    // RI3 Remove the first p entries from N and adjust the MBR of N
    // Eldawy: We chose to sort them by (increasing) distance and remove
    // the last p elements since deletion from the tail of the list is faster
    IntArray entriesToReInsert = new IntArray();
    entriesToReInsert.append(nodeChildren, nodeChildren.size() - p, p);
    nodeChildren.resize(nodeChildren.size() - p);

    // RI4: In the sort, defined in RI2, starting with the minimum distance
    // (=close reinsert), invoke Insert to reinsert the entries
    for (int iEntryToReinsert : entriesToReInsert)
      insertAnExistingDataEntry(iEntryToReinsert);
    // Now, we can reinsert the overflow element again
    insertAnExistingDataEntry(overflowEelement);
    reinserting = false;
  }

  abstract static class MultiIndexedSortable implements IndexedSortable {
    /**A flag that is set to true if the desired sort order is based on the maximum coordinate*/
    boolean max;
    /**The index of the dimension to sort on*/
    int sortAttr;
    void setAttribute(int dim, boolean isMax) {
      this.sortAttr = dim;
      this.max = isMax;
    }
  }

  /**
   * The R* split algorithm operating on a leaf node as described in the
   * following paper, Page 326.
   * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
   * The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles. SIGMOD Conference 1990: 322-331
   * @param iNode the index of the node to split
   * @param minSplitSize Minimum size for each split
   * @return the index of the new node created as a result of the split
   */
  @Override
  protected int split(int iNode, int minSplitSize) {
    int nodeSize = Node_size(iNode);
    final int[] nodeChildren = children.get(iNode).underlyingArray();
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by all dimensions by both min and max
    MultiIndexedSortable sorter = new MultiIndexedSortable() {
      @Override
      public int compare(int i, int j) {
        double diff;
        if (max)
          diff = maxCoord[sortAttr][nodeChildren[i]] - maxCoord[sortAttr][nodeChildren[j]];
        else
          diff = minCoord[sortAttr][nodeChildren[i]] - minCoord[sortAttr][nodeChildren[j]];
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
      }

      @Override
      public void swap(int i, int j) {
        int t = nodeChildren[i];
        nodeChildren[i] = nodeChildren[j];
        nodeChildren[j] = t;
      }
    };
    double minSumMargin = Double.POSITIVE_INFINITY;
    QuickSort quickSort = new QuickSort();
    int bestAxis = 0;
    boolean maxAxis = false;

    for (sorter.sortAttr = 0; sorter.sortAttr < getNumDimensions(); sorter.sortAttr++) {
      sorter.max = false;
      do {
        quickSort.sort(sorter, 0, nodeSize-1);
        double sumMargin = computeSumMargin(iNode, minSplitSize);

        if (sumMargin < minSumMargin) {
          minSumMargin = sumMargin;
          bestAxis = sorter.sortAttr;
          maxAxis = sorter.max;
        }
        sorter.max = !sorter.max;
      } while (sorter.max != false);
    }

    // Choose the axis with the minimum S as split axis.
    if (bestAxis != sorter.sortAttr || maxAxis != sorter.max) {
      sorter.sortAttr = bestAxis;
      sorter.max = maxAxis;
      quickSort.sort(sorter, 0, nodeSize);
    }

    // Along the chosen axis, choose the distribution with the minimum overlap value.
    // Initialize the MBB of the first group to the minimum group size
    double[] mbb1Min = new double[getNumDimensions()];
    double[] mbb1Max = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb1Min[d] = Double.POSITIVE_INFINITY;
      mbb1Max[d] = Double.NEGATIVE_INFINITY;
    }
    for (int iChild = 0; iChild < minSplitSize; iChild++){
      int child = nodeChildren[iChild];
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][child]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][child]);
      }
    }

    // Pre-cache the MBBs for groups that start at position i and end at the end
    double[] mbb2Min = new double[getNumDimensions()];
    double[] mbb2Max = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb2Min[d] = Double.POSITIVE_INFINITY;
      mbb2Max[d] = Double.NEGATIVE_INFINITY;
    }
    double[][] mbb2MinCached = new double[getNumDimensions()][nodeSize];
    double[][] mbb2MaxCached = new double[getNumDimensions()][nodeSize];
    for (int i = nodeSize - 1; i >= minSplitSize; i--) {
      int child = nodeChildren[i];
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb2Min[d] = Math.min(mbb2Min[d], minCoord[d][child]);
        mbb2Max[d] = Math.max(mbb2Max[d], maxCoord[d][child]);
        mbb2MinCached[d][i] = mbb2Min[d];
        mbb2MaxCached[d][i] = mbb2Max[d];
      }
    }

    double minOverlapVol = Double.POSITIVE_INFINITY;
    double minVol = Double.POSITIVE_INFINITY;
    int chosenK = -1;

    // # of possible splits = current size - 2 * minSplitSize + 1
    int numPossibleSplits = Node_size(iNode) - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][nodeChildren[separator-1]]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][nodeChildren[separator-1]]);
      }

      double overlapVol = 1.0;
      for (int d = 0; d < getNumDimensions(); d++) {
        double intersectionLength = Math.min(mbb1Max[d], mbb2MaxCached[d][separator])
            - Math.max(mbb1Min[d], mbb2MinCached[d][separator]);
        overlapVol *= Math.max(0.0, intersectionLength);
      }

      if (overlapVol < minOverlapVol) {
        minOverlapVol = overlapVol;
        double vol1 = 1.0, vol2 = 1.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          vol1 *= mbb1Max[d] - mbb1Min[d];
          vol2 *= mbb2MaxCached[d][separator] - mbb2MinCached[d][separator];
        }
        minVol = vol1 + vol2;
        chosenK = k;
      } else if (overlapVol == minOverlapVol) {
        // Resolve ties by choosing the distribution with minimum area-value
        double vol1 = 1.0, vol2 = 1.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          vol1 *= mbb1Max[d] - mbb1Min[d];
          vol2 *= mbb2MaxCached[d][separator] - mbb2MinCached[d][separator];
        }
        double vol = vol1 + vol2;
        if (vol < minVol) {
          minVol = vol;
          chosenK = k;
        }
      }
    }

    // Split at the chosenK
    int separator = minSplitSize - 1 + chosenK;
    int iNewNode = Node_split(iNode, separator);
    return iNewNode;
  }

  /**
   * Compute the sum margin of the given node assuming that the children have
   * been already sorted along one of the dimensions.
   * @param iNode the index of the node to compute for
   * @param minSplitSize the minimum split size to consider
   * @return
   */
  private double computeSumMargin(int iNode, int minSplitSize) {
    IntArray nodeChildren = children.get(iNode);
    int nodeSize = nodeChildren.size() - 1; // -1 excludes the overflow object
    double sumMargin = 0.0;
    double[] mbb1Min = new double[getNumDimensions()];
    double[] mbb1Max = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb1Min[d] = Double.POSITIVE_INFINITY;
      mbb1Max[d] = Double.NEGATIVE_INFINITY;
    }
    // Initialize the MBR of the first group to the minimum group size
    for (int iChild = 0; iChild < minSplitSize; iChild++) {
      int child = nodeChildren.get(iChild);
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][child]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][child]);
      }
    }
    // Pre-cache the MBBs for groups that start at position i and end at the end
    double[] mbb2Min = new double[getNumDimensions()];
    double[] mbb2Max = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb2Min[d] = Double.POSITIVE_INFINITY;
      mbb2Max[d] = Double.NEGATIVE_INFINITY;
    }
    double[][] mbb2MinCached = new double[getNumDimensions()][nodeSize];
    double[][] mbb2MaxCached = new double[getNumDimensions()][nodeSize];
    for (int i = nodeSize - 1; i >= minSplitSize; i--) {
      int child = nodeChildren.get(i);
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb2Min[d] = Math.min(mbb2Min[d], minCoord[d][child]);
        mbb2Max[d] = Math.max(mbb2Max[d], maxCoord[d][child]);
        mbb2MinCached[d][i] = mbb2Min[d];
        mbb2MaxCached[d][i] = mbb2Max[d];
      }
    }

    int numPossibleSplits = nodeSize - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][nodeChildren.get(separator-1)]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][nodeChildren.get(separator-1)]);
      }

      for (int d = 0; d < getNumDimensions(); d++) {
        sumMargin += mbb1Max[d] - mbb1Min[d];
        sumMargin += mbb2MaxCached[d][separator] - mbb2MinCached[d][separator];
      }
    }
    return sumMargin;
  }
  
  enum MinimizationFunction {PERIMETER, AREA};

  /**
   * Compute the margins (length on each dimension) for the given set of points in the given order. This is used as a
   * basis to measure the sum or margins, or choose the distribution with the minimum margin, or area.
   * However, it cannot be used to measure the distribution with the minimum overlap area because it does not keep the
   * actual MBR.
   * @param coords the array of point coordinates
   * @param start the index of the first point to consider
   * @param end the index after the last point to consider
   * @param marginsLeft the output array that stores the margins of the set of points starting at the index {@code start}
   * @param marginsRight the output array that stores the margins of the set of points ending at the index {@code end}
   */
  static void computeMargins(double[][] coords, int start, int end, double[][] marginsLeft, double[][] marginsRight) {
    final int numDimensions = coords.length;
    assert numDimensions == marginsLeft.length && numDimensions == marginsRight.length;
    final int numPoints = end - start;
    for (int d = 0; d < numDimensions; d++) {
      double minLeft = Double.POSITIVE_INFINITY, minRight = Double.POSITIVE_INFINITY;
      double maxLeft = Double.NEGATIVE_INFINITY, maxRight = Double.NEGATIVE_INFINITY;
      for (int i = 0; i < numPoints; i++) {
        minLeft = Math.min(minLeft, coords[d][start + i]);
        maxLeft = Math.max(maxLeft, coords[d][start + i]);
        minRight = Math.min(minRight, coords[d][end - 1 - i]);
        maxRight = Math.max(maxRight, coords[d][end - 1 - i]);
        marginsLeft[d][i] = maxLeft - minLeft;
        marginsRight[d][numPoints - 1 - i] = maxRight - minRight;
      }
    }
  }

  static class SplitTask {
    /**The range of points to partition*/
    int start, end;
    /**The coordinate of the minimum corner of the MBB that covers these points*/
    double[] min;
    /**The coordinate of the maximum corner of the MBB that covers these points*/
    double[] max;
    /**Where the separation happens in the range [start, end)*/
    int separator;
    /**The coordinate along where the separation happened*/
    double splitCoord;
    /**The axis along where the separation happened. 0 for X and 1 for Y*/
    int axis;

    SplitTask(int s, int e, int numDimensions) {
      this.start = s;
      this.end = e;
      this.min = new double[numDimensions];
      this.max = new double[numDimensions];
    }
  }


  /**
   * Use the R*-tree improved splitting algorithm to split the given set of points
   * such that each split does not exceed the given capacity.
   * Returns the MBRs of the splits. This method is a standalone static method
   * that does not use the RStarTree class but uses a similar algorithm used
   * in {@link #split(int, int)}
   * 
   * The algorithm works in two steps.
   * <ol>
   *   <li>
   *   Choose a split axis <em>a</em> with the minimum sum of perimeters
   *   for all possible splits
   *   </li>
   *   <li>
   *   Since all splits are overlap free, the second step chooses the split
   *   with the minimum function <i>f</i> where <i>f</i> can be either the
   *   area or the perimeter.
   *   </li>
   * </ol>
   * Notice that the first step computes the sum of all perimeters and choose the minimum
   * while the second step chooses the single split with the minimum <i>f</i> along the chosen axis.
   * So, even if <i>f</i> is the perimeter function, we cannot simply choose
   * the split with the minimum <i>f</i> along all splits in the two axis.
   * 
   * @param coords the coordinates of the points to partition
   * @param minPartitionSize minimum number of points to include in a partition
   * @param maxPartitionSize maximum number of point in a partition
   * @param expandToInf when set to true, the returned partitions are expanded
   *                    to cover the entire space from, Negative Infinity to
   *                    Positive Infinity, in all dimensions.
   * @param fractionMinSplitSize the minimum fraction of a split [0,1]. If set to zero, all possible splits are considered
   *                             which might result in a bad performance. Setting this parameter to a larger fraction,
   *                             e.g., 0.3,improves the running time but might result in a sub-optimal partitioning.
   * @param aux If not set to <code>null</code>, this will be filled with some
   *            auxiliary information to help efficiently search through the
   *            partitions.
   * @return a list of envelopes that represent the partitions
   */
  static EnvelopeNDLite[] partitionPoints(final double[][] coords,
                                          final int minPartitionSize,
                                          final int maxPartitionSize,
                                          final boolean expandToInf,
                                          final AuxiliarySearchStructure aux,
                                          final double fractionMinSplitSize,
                                          final MinimizationFunction f) {
    final int numDimensions = coords.length;
    final int numPoints = coords[0].length;
    class SorterDim implements IndexedSortable {
      int sortAxis;

      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(coords[sortAxis][i] - coords[sortAxis][j]);
      }

      @Override
      public void swap(int i, int j) {
        for (int d = 0; d < numDimensions; d++) {
          double t = coords[d][i];
          coords[d][i] = coords[d][j];
          coords[d][j] = t;
        }
      }
    }
    Stack<SplitTask> rangesToSplit = new Stack<SplitTask>();
    Map<Long, SplitTask> rangesAlreadySplit = new HashMap<Long,SplitTask>();
    SplitTask firstRange = new SplitTask(0, numPoints, numDimensions);
    // The MBR of the first range covers the entire space (-Infinity, Infinity)
    for (int d = 0; d < numDimensions; d++) {
      firstRange.min[d] = Double.NEGATIVE_INFINITY;
      firstRange.max[d] = Double.POSITIVE_INFINITY;
    }
    rangesToSplit.push(firstRange);
    List<EnvelopeNDLite> finalizedSplits = new ArrayList<>();

    // Create the temporary arrays once to avoid the overhead of recreating them in each loop iteration

    // Store all the values at each split point so that we can choose the minimum at the end
    double[][] marginsLeft = new double[numDimensions][numPoints];
    double[][] marginsRight = new double[numDimensions][numPoints];

    while (!rangesToSplit.isEmpty()) {
      SplitTask range = rangesToSplit.pop();

      if (range.end - range.start <= maxPartitionSize) {
        // No further splitting needed. Create a final partition
        EnvelopeNDLite partitionMBR = new EnvelopeNDLite(numDimensions);
        partitionMBR.setEmpty();
        if (expandToInf) {
          for (int d = 0; d < numDimensions; d++) {
            partitionMBR.merge(range.min);
            partitionMBR.merge(range.max);
          }
        } else {
          double[] point = new double[numDimensions];
          for (int i = range.start; i < range.end; i++) {
            for (int d = 0; d < numDimensions; d++) {
              point[d] = coords[d][i];
            }
            partitionMBR.merge(point);
          }
        }
        // Mark the range as a leaf partition by setting the separator to
        // a negative number x. The partition ID is -x-1
        range.separator = -finalizedSplits.size()-1;
        rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
        finalizedSplits.add(partitionMBR);
        continue;
      }

      // ChooseSplitAxis
      // Sort the entries by each dimension and compute S, the sum of all margin-values of the different distributions
      final int minSplitSize = Math.max(minPartitionSize, (int)((range.end - range.start) * fractionMinSplitSize));
      final int numPossibleSplits = (range.end - range.start) - 2 * minSplitSize + 1;

      QuickSort quickSort = new QuickSort();
      SorterDim sorter = new SorterDim();

      // Choose the axis with the minimum sum of margin
      int chosenAxis = 0;
      double minMargin = Double.POSITIVE_INFINITY;

      // Keep the sumMargins for all sort dimensions
      for (sorter.sortAxis = 0; sorter.sortAxis < numDimensions; sorter.sortAxis++) {
        // Sort then compute sum margin.
        quickSort.sort(sorter, range.start, range.end);

        computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);

        double margin = 0.0;
        for (int d = 0; d < numDimensions; d++)
          for (int k = 1; k <= numPossibleSplits; k++)
            margin += marginsLeft[d][minSplitSize + k - 1 - 1] + marginsRight[d][minSplitSize + k - 1 - 1];

        if (margin < minMargin) {
          minMargin = margin;
          chosenAxis = sorter.sortAxis;
        }
      }

      // Repeat the sorting along the chosen axis if needed
      if (sorter.sortAxis != chosenAxis) {
        sorter.sortAxis = chosenAxis;
        quickSort.sort(sorter, range.start, range.end);
        // Recompute the margins based on the new sorting order
        computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);
      }

      // Along the chosen axis, choose the distribution with the minimum area (or perimeter)
      // Note: Since we partition points, the overlap is always zero and we ought to choose based on the total volume

      int chosenK = -1;
      double minValue = Double.POSITIVE_INFINITY;
      for (int k = 1; k <= numPossibleSplits; k++) {
        // Skip if k is invalid (either side induce an invalid size)
        int size1 = minSplitSize + k - 1;
        if (!isValid(size1, minPartitionSize, maxPartitionSize)) {
          continue;
        }

        int size2 = range.end - range.start - size1;
        if (!isValid(size2, minPartitionSize, maxPartitionSize)) {
          continue;
        }

        double splitValue = 0.0;
        switch (f) {
          case AREA:
            double vol1 = 1.0, vol2 = 1.0;
            for (int d = 0; d < numDimensions; d++) {
              vol1 *= marginsLeft[d][minSplitSize + k - 1 - 1];
              vol2 *= marginsRight[d][minSplitSize + k - 1 - 1];
            }
            splitValue = vol1 + vol2;
            break;
          case PERIMETER:
            for (int d = 0; d < numDimensions; d++) {
              splitValue += marginsLeft[d][minSplitSize + k - 1 - 1];
              splitValue += marginsRight[d][minSplitSize + k - 1 - 1];
            }
            break;
          default:
              throw new RuntimeException("Unsupported function " + f);
        }
        if (splitValue < minValue) {
          chosenK = k;
          minValue = splitValue;
        }
      }

      // Split at the chosenK
      range.separator = range.start + minSplitSize - 1 + chosenK;

      // Create two sub-ranges
      // Sub-range 1 covers the range [rangeStart, separator)
      // Sub-range 2 covers the range [separator, rangeEnd)
      SplitTask range1 = new SplitTask(range.start, range.separator, numDimensions);
      SplitTask range2 = new SplitTask(range.separator, range.end, numDimensions);
      // Set the MBB of both to split along the chosen value
      for (int d = 0; d < numDimensions; d++) {
        range1.min[d] = range2.min[d] = range.min[d];
        range1.max[d] = range2.max[d] = range.max[d];
      }
      range.axis = chosenAxis;
      range.splitCoord = range1.max[chosenAxis] = range2.min[chosenAxis] = coords[chosenAxis][range.separator];
      rangesToSplit.add(range1);
      rangesToSplit.add(range2);
      rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
    }

    if (aux != null) {
      // Assign an ID for each partition
      Map<Long, Integer> partitionIDs = new HashMap<Long, Integer>();
      int seq = 0;
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator >= 0)
          partitionIDs.put(entry.getKey(), seq++);
        else
          partitionIDs.put(entry.getKey(), entry.getValue().separator);
      }
      // Build the search data structure
      int numOfSplitAxis = rangesAlreadySplit.size() - finalizedSplits.size();
      assert seq == numOfSplitAxis;
      aux.partitionGreaterThanOrEqual = new int[numOfSplitAxis];
      aux.partitionLessThan = new int[numOfSplitAxis];
      aux.splitAxis = new IntArray(numOfSplitAxis);
      aux.splitCoords = new double[numOfSplitAxis];
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator > 0) {
          int id = partitionIDs.get(entry.getKey());
          SplitTask range = entry.getValue();
          aux.splitCoords[id] = range.splitCoord;
          aux.splitAxis.set(id, range.axis);
          long p1 = (((long)range.separator) << 32) | range.start;
          aux.partitionLessThan[id] = partitionIDs.get(p1);
          long p2 = (((long)range.end) << 32) | range.separator;
          aux.partitionGreaterThanOrEqual[id] = partitionIDs.get(p2);
          if (range.start == 0 && range.end == numPoints)
            aux.rootSplit = id;
        }
      }
    }

    return finalizedSplits.toArray(new EnvelopeNDLite[finalizedSplits.size()]);
  }

  /**
   * Similar to {@link #partitionPoints(double[][], int, int, boolean, AuxiliarySearchStructure, double, MinimizationFunction)}
   * but takes the weights of points into account so that the weight of each partition is within the given limits.
   * Notice that it could be impossible to find partitions that satisfy the given size constraints. Only if this is
   * the case, minimal modifications to the weights are applied to ensure a correct answer.
   * @param coords the coordinates of the points to partition
   * @param weights the weights assigned to points to calculate the partition sizes correctly
   * @param minPartitionSize minimum number of points to include in a partition
   * @param maxPartitionSize maximum number of point in a partition
   * @param expandToInf when set to true, the returned partitions are expanded
   *                    to cover the entire space from, Negative Infinity to
   *                    Positive Infinity, in all dimensions.
   * @param aux If not set to <code>null</code>, this will be filled with some
   *            auxiliary information to help efficiently search through the
   *            partitions.
   * @param fractionMinSplitSize the minimum fraction of a split [0,1]. If set to zero, all possible splits are considered
   *                             which might result in a bad performance. Setting this parameter to a larger fraction,
   *                             e.g., 0.3,improves the running time but might result in a sub-optimal partitioning.
   * @param f the minimization function to use when calculating the quality of partitions
   * @return
   */
  static EnvelopeNDLite[] partitionWeightedPoints(final double[][] coords,
                                                  final long[] weights,
                                                  final long minPartitionSize,
                                                  final long maxPartitionSize,
                                                  final boolean expandToInf,
                                                  final AuxiliarySearchStructure aux,
                                                  final double fractionMinSplitSize,
                                                  final MinimizationFunction f) {
    final int numDimensions = coords.length;
    final int numPoints = coords[0].length;
    class SorterDim implements IndexedSortable {
      int sortAxis;

      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(coords[sortAxis][i] - coords[sortAxis][j]);
      }

      @Override
      public void swap(int i, int j) {
        // Swap the coordinates
        for (int d = 0; d < numDimensions; d++) {
          double t = coords[d][i];
          coords[d][i] = coords[d][j];
          coords[d][j] = t;
        }
        // Swap the weights
        long t = weights[i];
        weights[i] = weights[j];
        weights[j] = t;
      }
    }
    Stack<SplitTask> rangesToSplit = new Stack<SplitTask>();
    Map<Long, SplitTask> rangesAlreadySplit = new HashMap<Long,SplitTask>();
    SplitTask firstRange = new SplitTask(0, numPoints, numDimensions);
    // The MBR of the first range covers the entire space (-Infinity, Infinity)
    for (int d = 0; d < numDimensions; d++) {
      firstRange.min[d] = Double.NEGATIVE_INFINITY;
      firstRange.max[d] = Double.POSITIVE_INFINITY;
    }
    rangesToSplit.push(firstRange);
    List<EnvelopeNDLite> finalizedSplits = new ArrayList<>();

    // Create the temporary arrays once to avoid the overhead of recreating them in each loop iteration

    // Store all the values at each split point so that we can choose the minimum at the end
    double[][] marginsLeft = new double[numDimensions][numPoints];
    double[][] marginsRight = new double[numDimensions][numPoints];

    while (!rangesToSplit.isEmpty()) {
      SplitTask range = rangesToSplit.pop();

      long totalWeight = 0;
      for (int $i = range.start; $i < range.end; $i++)
        totalWeight += weights[$i];

      assert isValid(totalWeight, minPartitionSize, maxPartitionSize) :
          String.format("Invalid size encountered size=%d, m=%d, M=%d", totalWeight, minPartitionSize, maxPartitionSize);

      if (totalWeight <= maxPartitionSize || range.end - range.start == 1) {
        // No further splitting needed. Create a final partition
        EnvelopeNDLite partitionMBR = new EnvelopeNDLite(numDimensions);
        partitionMBR.setEmpty();
        if (expandToInf) {
          for (int d = 0; d < numDimensions; d++) {
            partitionMBR.merge(range.min);
            partitionMBR.merge(range.max);
          }
        } else {
          double[] point = new double[numDimensions];
          for (int i = range.start; i < range.end; i++) {
            for (int d = 0; d < numDimensions; d++) {
              point[d] = coords[d][i];
            }
            partitionMBR.merge(point);
          }
        }
        // Mark the range as a leaf partition by setting the separator to
        // a negative number x. The partition ID is -x-1
        range.separator = -finalizedSplits.size()-1;
        rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
        finalizedSplits.add(partitionMBR);
        continue;
      }

      // ChooseSplitAxis
      // Sort the entries by each dimension and compute S, the sum of all margin-values of the different distributions
      int minSplitSize = Math.max(1, (int)((range.end - range.start) * fractionMinSplitSize));
      int numPossibleSplits = (range.end - range.start) - 2 * minSplitSize + 1;

      QuickSort quickSort = new QuickSort();
      SorterDim sorter = new SorterDim();

      // Choose the axis with the minimum sum of margin
      int chosenAxis = 0;
      double minMargin = Double.POSITIVE_INFINITY;

      // Keep the sumMargins for all sort dimensions
      for (sorter.sortAxis = 0; sorter.sortAxis < numDimensions; sorter.sortAxis++) {
        // Sort then compute sum margin.
        quickSort.sort(sorter, range.start, range.end);

        computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);

        double margin = 0.0;
        for (int d = 0; d < numDimensions; d++)
          for (int k = 1; k <= numPossibleSplits; k++)
            margin += marginsLeft[d][minSplitSize + k - 1 - 1] + marginsRight[d][minSplitSize + k - 1 - 1];

        if (margin < minMargin) {
          minMargin = margin;
          chosenAxis = sorter.sortAxis;
        }
      }

      // Repeat the sorting along the chosen axis if needed
      if (sorter.sortAxis != chosenAxis) {
        sorter.sortAxis = chosenAxis;
        quickSort.sort(sorter, range.start, range.end);
        // Recompute the margins based on the new sorting order
        computeMargins(coords, range.start, range.end, marginsLeft, marginsRight);
      }

      // Along the chosen axis, choose the distribution with the minimum area (or perimeter)
      // Note: Since we partition points, the overlap is always zero and we ought to choose based on the total volume

      int chosenK = -1;
      boolean weightsCorrected = false;
      do {
        // Accumulate the size of the points from start to the separator
        long accumulatedWeight = 0;
        for (int $i = 0; $i < minSplitSize - 1; $i++)
          accumulatedWeight += weights[range.start + $i];
        double minValue = Double.POSITIVE_INFINITY;
        for (int k = 1; k <= numPossibleSplits; k++) {
          // Skip if k is invalid (either side induce an invalid size)
          accumulatedWeight += weights[range.start + minSplitSize + k - 2];
          if (!isValid(accumulatedWeight, minPartitionSize, maxPartitionSize))
            continue;
          if (!isValid(totalWeight - accumulatedWeight, minPartitionSize, maxPartitionSize))
            continue;

          double splitValue = 0.0;
          switch (f) {
            case AREA:
              double vol1 = 1.0, vol2 = 1.0;
              for (int d = 0; d < numDimensions; d++) {
                vol1 *= marginsLeft[d][minSplitSize + k - 1 - 1];
                vol2 *= marginsRight[d][minSplitSize + k - 1 - 1];
              }
              splitValue = vol1 + vol2;
              break;
            case PERIMETER:
              for (int d = 0; d < numDimensions; d++) {
                splitValue += marginsLeft[d][minSplitSize + k - 1 - 1];
                splitValue += marginsRight[d][minSplitSize + k - 1 - 1];
              }
              break;
            default:
              throw new RuntimeException("Unsupported function " + f);
          }
          if (splitValue < minValue) {
            chosenK = k;
            minValue = splitValue;
          }
        }
        assert !(weightsCorrected && chosenK == -1) : "Should always find a valid k after the weights are corrected";
        if (chosenK == -1) {
          // No valid split point. Correct the weights to induce some valid split points
          minSplitSize = 1;
          numPossibleSplits = (range.end - range.start) - 2 * minSplitSize + 1;
          accumulatedWeight = weights[range.start];

          int k = 1;

          int numValidLeftRanges = (int) (totalWeight / minPartitionSize);
          // Iterate over all valid left ranges (i.e., ranges where the partition to the left is of a valid weight)
          for (int i = 1; i <= numValidLeftRanges; i++) {
            long leftRangeStart = i * minPartitionSize;
            long leftRangeEnd = i * maxPartitionSize;
            // Find all valid right ranges that overlap this valid left range
            int j1 = (int) Math.ceil(((double) totalWeight - i * maxPartitionSize) / maxPartitionSize);
            int j2 = (int) Math.floor(((double) totalWeight - i * minPartitionSize) / minPartitionSize);
            while (j1 <= j2 && j2 > 0) {
              // Found an overlapping valid right range. Now, find the intersection between the two ranges.
              long rightRangeStart = totalWeight - j2 * maxPartitionSize;
              long rightRangeEnd = totalWeight - j2 * minPartitionSize;
              long rangeStart = Math.max(leftRangeStart, rightRangeStart);
              long rangeEnd = Math.min(leftRangeEnd, rightRangeEnd);
              assert rangeStart <= rangeEnd : "The valid range should not be inverted";
              // Advance the split point (k) until it is about to pass over the valid range
              while (k <= numPossibleSplits && accumulatedWeight + weights[range.start + minSplitSize + k + 1 - 2] < rangeStart) {
                k++;
                accumulatedWeight += weights[range.start + minSplitSize + k - 2];
              }
              assert k <= numPossibleSplits : "Should not pass over all the points while correcting the weight";
              // Found the last point before the valid range. Adjust its weight so that it falls in the range
              long diff = (rangeStart + rangeEnd) / 2 - accumulatedWeight;
              weights[range.start + minSplitSize + k - 2] += diff;
              weights[range.start + minSplitSize + k + 1 - 2] -= diff;
              accumulatedWeight += diff;
              assert isValid(accumulatedWeight, minPartitionSize, maxPartitionSize) :
                  String.format("The weight should be valid after correction, weight=%d, m=%d, M=%d",
                      accumulatedWeight, minPartitionSize, maxPartitionSize);
              // Go the next valid range
              j2--;
            }
          }

          weightsCorrected = true;
        }
      } while (chosenK == -1);

      assert chosenK != -1 : "Could not find any valid split points";

      // Split at the chosenK
      range.separator = range.start + minSplitSize - 1 + chosenK;

      // Create two sub-ranges
      // Sub-range 1 covers the range [rangeStart, separator)
      // Sub-range 2 covers the range [separator, rangeEnd)
      SplitTask range1 = new SplitTask(range.start, range.separator, numDimensions);
      SplitTask range2 = new SplitTask(range.separator, range.end, numDimensions);
      // Set the MBB of both to split along the chosen value
      for (int d = 0; d < numDimensions; d++) {
        range1.min[d] = range2.min[d] = range.min[d];
        range1.max[d] = range2.max[d] = range.max[d];
      }
      range.axis = chosenAxis;
      range.splitCoord = range1.max[chosenAxis] = range2.min[chosenAxis] = coords[chosenAxis][range.separator];
      rangesToSplit.add(range1);
      rangesToSplit.add(range2);
      rangesAlreadySplit.put(((long)range.end << 32) | range.start, range);
    }

    if (aux != null) {
      // Assign an ID for each partition
      Map<Long, Integer> partitionIDs = new HashMap<Long, Integer>();
      int seq = 0;
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator >= 0)
          partitionIDs.put(entry.getKey(), seq++);
        else
          partitionIDs.put(entry.getKey(), entry.getValue().separator);
      }
      // Build the search data structure
      int numOfSplitAxis = rangesAlreadySplit.size() - finalizedSplits.size();
      assert seq == numOfSplitAxis;
      aux.partitionGreaterThanOrEqual = new int[numOfSplitAxis];
      aux.partitionLessThan = new int[numOfSplitAxis];
      aux.splitAxis = new IntArray(numOfSplitAxis);
      aux.splitCoords = new double[numOfSplitAxis];
      for (Map.Entry<Long, SplitTask> entry : rangesAlreadySplit.entrySet()) {
        if (entry.getValue().separator > 0) {
          int id = partitionIDs.get(entry.getKey());
          SplitTask range = entry.getValue();
          aux.splitCoords[id] = range.splitCoord;
          aux.splitAxis.set(id, range.axis);
          long p1 = (((long)range.separator) << 32) | range.start;
          aux.partitionLessThan[id] = partitionIDs.get(p1);
          long p2 = (((long)range.end) << 32) | range.separator;
          aux.partitionGreaterThanOrEqual[id] = partitionIDs.get(p2);
          if (range.start == 0 && range.end == numPoints)
            aux.rootSplit = id;
        }
      }
    }

    return finalizedSplits.toArray(new EnvelopeNDLite[finalizedSplits.size()]);
  }

  /**
   * Checks if a given partition size is valid according to the min and max partition sizes.
   * A valid partition can be split into smaller partitions such that each one has a size in the given range.
   * @param size the size of the partition to test
   * @param minPartitionSize the minimum partition size (inclusive)
   * @param maxPartitionSize the maximum partition size (inclusive)
   * @return {@code true} if the given size is valid.
   */
  protected static boolean isValid(long size, long minPartitionSize, long maxPartitionSize) {
    int lowerBound = (int) Math.ceil((double) size/ maxPartitionSize);
    int upperBound = (int) Math.floor((double) size / minPartitionSize);
    return lowerBound <= upperBound;
  }

  /**
   * Partitions the given set of points using the improved R*-tree split algorithm.
   * Calls the function {@link #partitionPoints(double[][], int, int, boolean, AuxiliarySearchStructure, double, MinimizationFunction)}
   * with the last parameter as {@code MinimizationFunction.AREA}
   * @param coords the coordinates of the points in all dimensions
   * @param minPartitionSize the minimum number of records per partition
   * @param maxPartitionSize the maximum number of records per partition
   * @param expandToInf set to {@code true} to expand the boundaries of border partitions to cover the entire input space
   * @param aux (output) an auxiliary structure that is filled with information to speed up the search for partitions
   * @return the boundaries of partitions
   */
  static EnvelopeNDLite[] partitionPoints(double[][] coords, int minPartitionSize, int maxPartitionSize, boolean expandToInf,
                                      double fractionMinSplitSize, AuxiliarySearchStructure aux) {
    return partitionPoints(coords, minPartitionSize, maxPartitionSize, expandToInf, aux, fractionMinSplitSize,
        MinimizationFunction.AREA);
  }

  static EnvelopeNDLite[] partitionWeightedPoints(double[][] coords, long[] weights, long minPartitionSize,
                                              long maxPartitionSize, boolean expandToInf,
                                              double fractionMinSplitSize, AuxiliarySearchStructure aux) {
    return partitionWeightedPoints(coords, weights, minPartitionSize, maxPartitionSize, expandToInf, aux, fractionMinSplitSize,
        MinimizationFunction.AREA);
  }
}
