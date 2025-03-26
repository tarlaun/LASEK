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
import org.apache.hadoop.util.QuickSort;

import java.util.Arrays;
import java.util.Comparator;

/**
 * An implementation of the RR*-tree as described in the paper below.
 * Norbert Beckmann, Bernhard Seeger,
 * A Revised R*-tree in Comparison with Related Index Structures. SIGMOD 2009: 799-812
 *
 * It makes the following two changes to the original R-tree by Guttman.
 * <ol>
 *   <li>While inserting, it uses a new strategy for selecting the subtree at
 *   each level which takes into account the area increase, perimiter, and
 *   overlap</li>
 *   <li>It uses a new splitting strategy which takes into account the deviation
 *   of the MBR of a node since it was first created.</li>
 * </ol>
 *
 * Notice that this implementation is closer to the original R-tree rather than
 * the R*-tree and this is why it extends directly from the
 * {@link RTreeGuttman} class rather than the {@link RStarTree}.
 */
public class RRStarTree extends RTreeGuttman {

  /**The coordinates of the center of each node at the time it was created*/
  protected double[][] oBox;

  /**The tree-wide parameters used to calculate the weighting function*/
  protected static final double s = 0.5;
  protected static final double y1 = Math.exp(-1 / (s * s));
  protected static final double ys = 1 / (1 - y1);

  /**
   * Construct a new empty RR*-tree with the given parameters.
   *
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity  - Maximum capacity of a node
   */
  public RRStarTree(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
  }

  /**
   * Tests if the MBR of a node fully contains an object
   * @param node the ID of the node to check
   * @param object the ID of the object that needs to be tested if it is inside the node
   * @return {@code true} if the object is completely inside the node boundaries.
   */
  protected boolean Node_contains(int node, int object) {
    for (int d = 0; d < getNumDimensions(); d++) {
      if (minCoord[d][object] < minCoord[d][node] || maxCoord[d][object] > maxCoord[d][node])
        return false;
    }
    return true;
  }

  @Override
  protected int Node_createNodeWithChildren(boolean leaf, int... iChildren) {
    int nodeID = super.Node_createNodeWithChildren(leaf, iChildren);
    for (int d = 0; d < getNumDimensions(); d++)
      oBox[d][nodeID] = (minCoord[d][nodeID] + maxCoord[d][nodeID]) / 2.0;
    return nodeID;
  }

  @Override
  protected int Node_split(int nodeID, int separator) {
    int newNodeID = super.Node_split(nodeID, separator);
    for (int d = 0; d < getNumDimensions(); d++) {
      oBox[d][nodeID] = (minCoord[d][nodeID] + maxCoord[d][nodeID]) / 2.0;
      oBox[d][newNodeID] = (minCoord[d][newNodeID] + maxCoord[d][newNodeID]) / 2.0;
    }
    return newNodeID;
  }

  @Override
  protected void makeRoomForOneMoreObject() {
    super.makeRoomForOneMoreObject();
    if (minCoord[0].length != oBox[0].length) {
      for (int d = 0; d < getNumDimensions(); d++) {
        double[] newCoords = new double[minCoord[0].length];
        System.arraycopy(oBox[d], 0, newCoords, 0, oBox[d].length);
        oBox[d] = newCoords;
      }
    }
  }

  @Override
  protected void initializeDataEntries(double[] x1, double[] y1, double[] x2, double[] y2) {
    super.initializeDataEntries(x1, y1, x2, y2);
    oBox = new double[minCoord.length][minCoord[0].length];
  }

  @Override
  protected void initializeDataEntries(double[] xs, double[] ys) {
    super.initializeDataEntries(xs, ys);
    oBox = new double[minCoord.length][minCoord[0].length];
  }

  protected void initializeDataEntries(double[][] minCoords, double[][] maxCoords) {
    super.initializeDataEntries(minCoords, maxCoords);
    oBox = new double[minCoord.length][minCoord[0].length];
  }

  /**
   * Chooses the best subtree to add a new data entry.
   * This function implements the CSRevised algorithm on Page 802 in the paper
   * @param object the ID of the object to insert
   * @param node the ID of the internal node to insert to
   * @return the ID of the subtree (child node ID) to insert the node to.
   */
  @Override
  protected int chooseSubtree(final int object, final int node) {
    // cov, the set of entries that entirely cover the new object
    IntArray cov = new IntArray();
    for (int child : children.get(node)) {
      if (Node_contains(child, object))
        cov.add(child);
    }

    if (cov.size() == 1)
      return cov.peek();

    if (!cov.isEmpty()) {
      // There are some nodes that do not need to be expanded to accommodate the object
      // If there are some children with zero volume (area), return the one with the smallest perimeter
      // Otherwise, return the child with the minimum volume (area)
      // This is effectively the same as returning the first child when sorted
      // lexicographically by (volume, perimeter)
      int bestChild = -1;
      double minVol = Double.POSITIVE_INFINITY;
      double minPerim = Double.POSITIVE_INFINITY;
      for (int iChild : cov) {
        double vol = Node_volume(iChild);
        if (vol < minVol) {
          minVol = vol;
          minPerim = Node_perimeter(iChild);
          bestChild = iChild;
        } else if (vol == minVol) {
          // This also covers the case of vol == minVol == 0
          if (Node_perimeter(iChild) < minPerim) {
            minPerim = Node_perimeter(iChild);
            bestChild = iChild;
          }
        }
      }
      return bestChild;
    }

    // A node has to be enlarged to accommodate the object
    // Sort the children of the node in ascending order of their delta_perim
    // For simplicity, we use insertion sort since the node size is small
    // TODO we can speed this step up by precaching delta_perim values
    children.get(node).insertionSort(new Comparator<Integer>() {
      @Override
      public int compare(Integer child1, Integer child2) {
        double dPerim1 = Node_dPerimeter(child1, object);
        double dPerim2 = Node_dPerimeter(child2, object);
        if (dPerim1 < dPerim2) return -1;
        if (dPerim1 > dPerim2) return +1;
        return 0;
      }
    });

    // If dOvlpPerim = 0 between the first entry and all remaining entries
    // return the first entry
    IntArray nodeChildren = children.get(node);
    // Try to achieve an overlap optimized choice
    int p = 0;
    for (int iChild = 1; iChild < nodeChildren.size(); iChild++) {
      if (dOvlp(nodeChildren.get(0), object, nodeChildren.get(iChild), AggregateFunction.PERIMETER) > 0)
        p = iChild;
    }
    if (p == 0) {
      // dOvlpPerim = 0 between the first entry and all remaining entries.
      // return the first entry
      return nodeChildren.get(0);
    }
    assert cov.isEmpty();
    int c;
    // If there is an index i with vol(MBB(Ri U object)) = 0
    int iChildWithZeroVolExpansion = -1;
    for (int iChild = 0; iChild <= p; iChild++) {
      if (Node_volumeExpansion(nodeChildren.get(iChild), object) == 0) {
        iChildWithZeroVolExpansion = iChild;
        break;
      }
    }
    IntArray cand = cov; // reuse the same IntArray for efficiency
    // checkComp will fill in the deltaOverlap array with the computed value
    // for each candidate
    double[] sumDeltaOverlap = new double[nodeChildren.size()];
    if (iChildWithZeroVolExpansion != -1) {
      c = checkComp(0, AggregateFunction.PERIMETER, cand, sumDeltaOverlap, p, object, nodeChildren);
    } else {
      c = checkComp(0, AggregateFunction.VOLUME, cand, sumDeltaOverlap, p, object, nodeChildren);
    }
    if (c != -1) // if (success)
      return nodeChildren.get(c);

    int iMinDeltaOverlap = -1;
    double minDeltaOverlap = Double.POSITIVE_INFINITY;
    for (int i : cand) {
      if (sumDeltaOverlap[i] < minDeltaOverlap) {
        minDeltaOverlap = sumDeltaOverlap[i];
        iMinDeltaOverlap = i;
      }
    }
    assert iMinDeltaOverlap != -1 : "Couldn't choose a subtree. sumDeltaOverlap=" + Arrays.toString(sumDeltaOverlap)
        + " and function: "+AggregateFunction.PERIMETER;
    return nodeChildren.get(iMinDeltaOverlap);
  }

  enum AggregateFunction {PERIMETER, VOLUME};

  protected int checkComp(int t, AggregateFunction f, IntArray cand, double[] sumDeltaOverlap,
                          int p, int object, IntArray nodeChildren) {
    cand.add(t);
    sumDeltaOverlap[t] = 0; // the accumulation of dOvlp(t, [0, p))
    int c = -1;
    for (int j = 0; j <= p; j++) {
      if (j == t)
        continue;
      double ovlpPerimTJ = dOvlp(nodeChildren.get(t), object, nodeChildren.get(j), f);
      assert Double.isFinite(ovlpPerimTJ) : "Invalid ovlpPerimTJ";
      sumDeltaOverlap[t] += ovlpPerimTJ;
      if (ovlpPerimTJ != 0 && !cand.contains(j)) {
        c = checkComp(j, f, cand, sumDeltaOverlap, p, object, nodeChildren);
        if (c != -1)
          break;
      }
    }

    if (sumDeltaOverlap[t] == 0) // i.e. delta Ovlp f t, [0, p) = 0
      return t;
    return c;
  }

  /**
   * Compute the perimeter of a node (or an object)
   * @param node the ID of the node to compute its perimeter
   * @return the perimeter of the given node (sum of side lengths)
   */
  protected double Node_perimeter(int node) {
    double perimeter = 0.0;
    for (int d = 0; d < getNumDimensions(); d++)
      perimeter += maxCoord[d][node] - minCoord[d][node];
    return perimeter;
  }

  /**
   * Computes the difference in the perimeter if the given object is added to the node
   * @param node the ID of the node to compute its perimeter difference
   * @param newChild the ID of the object that might be added to the node to cause the difference
   * @return the difference of the perimeter
   */
  protected double Node_dPerimeter(int node, int newChild) {
    double perimeterB4 = 0.0, perimiterAfter = 0.0;
    for (int d = 0; d < getNumDimensions(); d++) {
      perimeterB4 += maxCoord[d][node] - minCoord[d][node];
      perimiterAfter += Math.max(maxCoord[d][node], maxCoord[d][newChild]) -
          Math.min(minCoord[d][node], minCoord[d][newChild]);
    }
    return perimiterAfter - perimeterB4;
  }

  /**
   * Computes the increase of the function f of the common overlap between two
   * nodes (t, j) if a new object (iObject) is added to the node t
   * @param nodeT ID of node t
   * @param object ID of the object that could be added to t
   * @param nodeJ ID of node j
   * @param f the function to compute the difference for, i.e., perimiter or volume.
   * @return the difference in the overlap between node T and node J if the object is added to node T
   */
  protected double dOvlp(int nodeT, int object, int nodeJ, AggregateFunction f) {
    double fTJ, fTOJ;
    switch (f) {
      case VOLUME:
        fTJ = fTOJ = 1.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          // Compute the width of (T ^ J) along the current dimension
          double widthTJ = Math.min(globalMax[d], Math.min(maxCoord[d][nodeT], maxCoord[d][nodeJ])) -
              Math.max(globalMin[d], Math.max(minCoord[d][nodeT], minCoord[d][nodeJ]));
          fTJ *= Math.max(0.0, widthTJ);
          // Compute the width of (T U O) ^ J along the current dimension
          double widthTOJ = Math.min(globalMax[d], Math.min(Math.max(maxCoord[d][nodeT], maxCoord[d][object]), maxCoord[d][nodeJ])) -
              Math.max(globalMin[d], Math.max(Math.min(minCoord[d][nodeT], minCoord[d][object]), minCoord[d][nodeJ]));
          fTOJ *= Math.max(0.0, widthTOJ);
        }
        break;
      case PERIMETER:
        fTJ = fTOJ = 0.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          // Compute the width of (T ^ J) along the current dimension
          double widthTJ = Math.min(globalMax[d], Math.min(maxCoord[d][nodeT], maxCoord[d][nodeJ])) -
              Math.max(globalMin[d], Math.max(minCoord[d][nodeT], minCoord[d][nodeJ]));
          fTJ += Math.max(0.0, widthTJ);
          // Compute the width of (T U O) ^ J along the current dimension
          double widthTOJ = Math.min(globalMax[d], Math.min(Math.max(maxCoord[d][nodeT], maxCoord[d][object]), maxCoord[d][nodeJ])) -
              Math.max(globalMin[d], Math.max(Math.min(minCoord[d][nodeT], minCoord[d][object]), minCoord[d][nodeJ]));
          fTOJ += Math.max(0.0, widthTOJ);
        }
        break;
      default:
        throw new RuntimeException("Unsupported function " + f);
    }
    assert Double.isFinite(fTOJ) : "invalid fTOJ "+fTOJ;
    assert Double.isFinite(fTJ) : "invalid fTJ "+fTJ;
    return fTOJ - fTJ;
  }

  /**
   * Split an overflow node with the given minimum size of each split
   * @param iNode the index of the node to split
   * @param minSplitSize Minimum size of each split, typically, {@link #minCapacity}
   * @return the ID of the newly created node.
   */
  @Override
  protected int split(int iNode, int minSplitSize) {
    if (isLeaf.get(iNode))
      return splitLeaf(iNode, minSplitSize);
    else
      return splitNonLeaf(iNode, minSplitSize);
  }

  /**
   * Split a leaf node. This algorithm works in two steps as described on Page 803 of the paper.
   * <ol>
   *   <li>Compute the split axis <em>a</em> as the one with the minimum sum of perimeter for all split candidates</li>
   *   <li>If there are overlap-free split candidates, choose the one with minimum perimeter.
   *   Otherwise, choose the candidate with minimum overlap (volume of perimeter)</li>
   * </ol>
   * @param node the ID of the node to split
   * @param minSplitSize the minimum split size
   * @return the ID of the newly created node as a result of the split
   */
  protected int splitLeaf(int node, int minSplitSize) {
    int nodeSize = Node_size(node);
    final int[] nodeChildren = children.get(node).underlyingArray();
    // ChooseSplitAxis
    // Sort the entries by each axis and compute S, the sum of all margin-values
    // of the different distributions

    // Sort by all dimensions by both min and max
    RStarTree.MultiIndexedSortable sorter = new RStarTree.MultiIndexedSortable() {
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
        quickSort.sort(sorter, 0, nodeSize);
        double sumMargin = computeSumMargin(node, minSplitSize);

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

    // Calculate the common terms for the weighting function along the chosen axis
    double nodeCenter, nodeOrigin, nodeLength;
    nodeCenter = (minCoord[bestAxis][node] + maxCoord[bestAxis][node]) / 2.0;
    nodeOrigin = oBox[bestAxis][node];
    nodeLength = maxCoord[bestAxis][node] - minCoord[bestAxis][node];

    // Disable asymptotic splitting when splitting the root for the first time (i.e., root is leaf)
    // This function is only called for splitting a leaf node
    double asym = node == root ? 0 : 2.0 * (nodeCenter - nodeOrigin) / nodeLength;
    double mu = (1.0 - 2.0 * minSplitSize / (maxCapacity + 1)) * asym;
    double sigma = s * (1.0 + Math.abs(mu));

    // Along the chosen axis, choose the distribution with the minimum overlap value.
    int chosenK = chooseSplitPoint(node, minSplitSize, mu, sigma, nodeSize, nodeChildren);

    if (chosenK == -1) {
      // Could not find a split point due to children with empty MBR
      int numSplitPoints = nodeSize - 2 * minSplitSize + 1;
      chosenK = random.nextInt(numSplitPoints);
    }
    assert chosenK != -1;

    // Split at the chosenK
    int separator = minSplitSize - 1 + chosenK;
    int iNewNode = Node_split(node, separator);
    return iNewNode;
  }

  // TODO remove this variable
  double minWeightFoundByLastCallOfChooseSplitPoint;

  /**
   * Choose the split point along a chosen axis. This function assumes that the
   * entries are already sorted along a chosen axis.
   * @param node the overfilled node being split
   * @param minSplitSize
   * @param mu
   * @param sigma
   * @param numEntriesToConsider number of entries to consider for splitting
   * @param entries
   * @return
   */
  private int chooseSplitPoint(int node, int minSplitSize, double mu, double sigma, int numEntriesToConsider, int[] entries) {
    // Initialize the MBR of the first group to the minimum group size
    double[] mbb1Min = new double[getNumDimensions()];
    double[] mbb1Max = new double[getNumDimensions()];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb1Min[d] = Double.POSITIVE_INFINITY;
      mbb1Max[d] = Double.NEGATIVE_INFINITY;
    }
    for (int i = 0; i < minSplitSize; i++){
      int iChild = entries[i];
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][iChild]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][iChild]);
      }
    }

    // Pre-cache the MBBs for groups that start at position i and end at the end
    double[][] mbb2Min = new double[getNumDimensions()][numEntriesToConsider+1];
    double[][] mbb2Max = new double[getNumDimensions()][numEntriesToConsider+1];
    for (int d = 0; d < getNumDimensions(); d++) {
      mbb2Min[d][numEntriesToConsider] = Double.POSITIVE_INFINITY;
      mbb2Max[d][numEntriesToConsider] = Double.NEGATIVE_INFINITY;
    }
    for (int i = numEntriesToConsider - 1; i >= minSplitSize; i--) {
      int child = entries[i];
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb2Min[d][i] = Math.min(mbb2Min[d][i + 1], minCoord[d][child]);
        mbb2Max[d][i] = Math.max(mbb2Max[d][i + 1], maxCoord[d][child]);
      }
    }

    // Switch from volume-based optimization strategy to a perimeter-based if ...
    // ... vol(MBB(F_{m,a})) = 0 OR ...
    double volLeftMostPart = 1.0;
    // vol(MBB(S_{M+1-m,a})) = 0
    double volRightMostPart = 1.0;
    for (int d = 0; d < getNumDimensions(); d++) {
      volLeftMostPart *= mbb1Max[d] - mbb1Min[d];
      volRightMostPart *= mbb2Max[d][numEntriesToConsider - minSplitSize] - mbb2Min[d][numEntriesToConsider - minSplitSize];
    }
    AggregateFunction f = volLeftMostPart == 0 || volRightMostPart == 0 ?
        AggregateFunction.PERIMETER : AggregateFunction.VOLUME;

    // Calculate perim_{max} as described on the top of page 805 in the paper
    double maxPerimeter;
    {
      double sumPerim = 0.0, smallestEdge = Double.POSITIVE_INFINITY;
      for (int d = 0; d < getNumDimensions(); d++) {
        double edgeLength = maxCoord[d][node] - minCoord[d][node];
        sumPerim += edgeLength;
        if (edgeLength < smallestEdge)
          smallestEdge = edgeLength;
      }
      maxPerimeter = 2 * sumPerim - smallestEdge;
    }

    // A flag that is raised once an overlap-free candidate is found
    double minWeight = Double.POSITIVE_INFINITY;
    int chosenK = -1;

    // # of possible splits = current size - 2 * minSplitSize + 1
    int numPossibleSplits = numEntriesToConsider - 2 * minSplitSize + 1;
    for (int k = 1; k <= numPossibleSplits; k++) {
      int separator = minSplitSize + k - 1; // Separator = size of first group
      // Compute wf for this value of k (referred to as i in the RR*-tree paper)
      double xi = 2.0 * separator / (maxCapacity + 1.0) - 1.0;
      double gaussianTerm = (xi - mu) / sigma;
      double wf = ys * (Math.exp(-gaussianTerm * gaussianTerm) - y1);

      // Update the MBB of the first group
      for (int d = 0; d < getNumDimensions(); d++) {
        mbb1Min[d] = Math.min(mbb1Min[d], minCoord[d][entries[separator - 1]]);
        mbb1Max[d] = Math.max(mbb1Max[d], maxCoord[d][entries[separator - 1]]);
      }

      boolean overlapFree = false;
      for (int d = 0; d < getNumDimensions() && !overlapFree; d++) {
        overlapFree = mbb1Max[d] <= mbb2Max[d][separator] || mbb2Max[d][separator] <= mbb1Max[d];
      }

      if (overlapFree) {
        // If there are overlap-free split candidates on split axis a,
        // thereof, choose the candidate with minimum perimeter

        // This is an overlap-free candidate, calculate its perimeter
        double splitPerimeter = 0.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          splitPerimeter += mbb1Max[d] - mbb1Min[d];
          splitPerimeter += mbb2Max[d][separator] - mbb2Min[d][separator];
        }
        double wg = splitPerimeter - maxPerimeter;
        double w = wg * wf;
        if (w < minWeight) {
          minWeight = w;
          chosenK = k;
        }
      } else {
        double wg;
        switch (f) {
          case VOLUME:
            wg = 1.0;
            for (int d = 0; d < getNumDimensions(); d++) {
              double ovlpLength = Math.min(mbb1Max[d], mbb2Max[d][separator]) - Math.max(mbb1Min[d], mbb2Min[d][separator]);
              wg *= ovlpLength;
            }
            break;
          case PERIMETER:
            wg = 0.0;
            for (int d = 0; d < getNumDimensions(); d++) {
              double ovlpLength = Math.min(mbb1Max[d], mbb2Max[d][separator]) - Math.max(mbb1Min[d], mbb2Min[d][separator]);
              wg += ovlpLength;
            }
            break;
          default:
            throw new RuntimeException("Unsupported function "+f);
        }
        // choose the candidate with minimum overlap function (volume or perimeter)
        double w = wg / wf;
        if (w < minWeight) {
          chosenK = k;
          minWeight = w;
        }
      }
    }
    minWeightFoundByLastCallOfChooseSplitPoint = minWeight;
    return chosenK;
  }

  protected int splitNonLeaf(int node, int minSplitSize) {
    int nodeSize = Node_size(node);
    final int[] nodeChildren = children.get(node).underlyingArray();
    // Try all possible splits and choose the one with the best value

    // Sort by all dimensions by both min and max
    RStarTree.MultiIndexedSortable sorter = new RStarTree.MultiIndexedSortable() {
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
    double minWeightAmongAllAxes = Double.POSITIVE_INFINITY;
    int kWithMinWeight = -1;
    int chosenAxis = -1;
    boolean chosenMax = false;
    QuickSort quickSort = new QuickSort();

    for (sorter.sortAttr = 0; sorter.sortAttr < getNumDimensions(); sorter.sortAttr++) {
      sorter.max = false;
      do {
        quickSort.sort(sorter, 0, nodeSize-1);

        // Calculate the common terms for the weighting function along the chosen axis
        double nodeCenter, nodeOrigin, nodeLength;
        nodeCenter = (minCoord[sorter.sortAttr][node] + maxCoord[sorter.sortAttr][node]) / 2.0;
        nodeOrigin = oBox[sorter.sortAttr][node];
        nodeLength = maxCoord[sorter.sortAttr][node] - minCoord[sorter.sortAttr][node];

        // For non-leaf nodes, always use asymmetric splitting
        double asym = 2.0 * (nodeCenter - nodeOrigin) / nodeLength;
        double mu = (1.0 - 2.0 * minSplitSize / (maxCapacity + 1)) * asym;
        double sigma = s * (1.0 + Math.abs(mu));

        // Along the chosen axis, choose the distribution with the minimum overlap value.
        int bestKAlongThisAxis = chooseSplitPoint(node, minSplitSize, mu, sigma, nodeSize, nodeChildren);
        double bestWeightAlongThisAxis = minWeightFoundByLastCallOfChooseSplitPoint;
        if (bestWeightAlongThisAxis < minWeightAmongAllAxes) {
          minWeightAmongAllAxes = bestWeightAlongThisAxis;
          chosenAxis = sorter.sortAttr;
          kWithMinWeight = bestKAlongThisAxis;
          chosenMax = sorter.max;
        }

        sorter.max = !sorter.max;
      } while (sorter.max != false);
    }

    if (chosenAxis == -1) {
      // Could not find a split due to children with empty boundaries.
      // Choose a random axis and random split point
      chosenAxis = random.nextInt(getNumDimensions());
      int numSplits = nodeSize - 2 * minSplitSize + 1;
      kWithMinWeight = random.nextInt(numSplits);
    }

    // Choose the axis with the minimum S as split axis.
    if (chosenAxis != sorter.sortAttr || chosenMax != sorter.max) {
      sorter.sortAttr = chosenAxis;
      sorter.max = chosenMax;
      quickSort.sort(sorter, 0, nodeSize);
    }

    // Split at the chosenK
    int separator = minSplitSize - 1 + kWithMinWeight;
    int iNewNode = Node_split(node, separator);
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
    int nodeSize = nodeChildren.size();
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


  /**
   * Partitions the given set of points using the improved RR*-tree split algorithm.
   * Calls the function{@link RStarTree#partitionPoints(double[][], int, int, boolean, AuxiliarySearchStructure,
   * double, RStarTree.MinimizationFunction)}
   * with the last parameter as {@code MinimizationFunction.PERIMETER}
   * @param coords the coordinates of the points to partition
   * @param minPartitionSize
   * @param maxPartitionSize
   * @param expandToInf
   * @param aux
   * @return
   */
  static EnvelopeNDLite[] partitionPoints(double[][] coords, int minPartitionSize, int maxPartitionSize,
                                          boolean expandToInf, double fractionMinSplitSize, AuxiliarySearchStructure aux) {
    return RStarTree.partitionPoints(coords, minPartitionSize, maxPartitionSize,
        expandToInf, aux, fractionMinSplitSize, RStarTree.MinimizationFunction.PERIMETER);
  }
}
