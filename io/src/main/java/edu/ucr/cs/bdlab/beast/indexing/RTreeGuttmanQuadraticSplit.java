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

import edu.ucr.cs.bdlab.beast.util.IntArray;

/**
 * R-Tree with quadratic split
 */
public class RTreeGuttmanQuadraticSplit extends RTreeGuttman {

  /**
   * Construct a new empty R-tree with the given parameters.
   *
   * @param minCapacity - Minimum capacity of a node
   * @param maxCapcity  - Maximum capacity of a node
   */
  public RTreeGuttmanQuadraticSplit(int minCapacity, int maxCapcity) {
    super(minCapacity, maxCapcity);
  }

  @Override
  protected int split(int iNode, int minSplitSize) {
    IntArray nodeChildren = children.get(iNode);
    // Pick seeds
    // Indexes of the objects to be picked as seeds in the arrays xs and ys
    // Select two entries to be the first elements of the groups
    int seed1 = -1, seed2 = -1;
    double maxD = Double.NEGATIVE_INFINITY;
    for (int i1 = 0; i1 < nodeChildren.size(); i1++) {
      int iChild1 = nodeChildren.get(i1);
      for (int i2 = i1 + 1; i2 < nodeChildren.size(); i2++) {
        int iChild2 = nodeChildren.get(i2);
        // For each pair of entries, compose a rectangle J including both of
        // them and calculate d = area(J) - area(entry1) - area(entry2)
        // Choose the most wasteful pair. Choose the pair with the largest d
        double expandedVolume = 1.0;
        for (int d = 0; d < getNumDimensions(); d++) {
          expandedVolume *= Math.max(maxCoord[d][iChild1], maxCoord[d][iChild2]) -
              Math.min(minCoord[d][iChild1], minCoord[d][iChild2]);
        }
        double diff = expandedVolume - Node_volume(iChild1) - Node_volume(iChild2);
        if (diff > maxD) {
          maxD = diff;
          seed1 = iChild1;
          seed2 = iChild2;
        }
      }
    }

    // After picking the seeds, we will start picking next elements one-by-one
    IntArray nonAssignedNodes = nodeChildren.clone();
    Node_reset(iNode, seed1);
    int iNewNode = Node_createNodeWithChildren(isLeaf.get(iNode), seed2);
    nonAssignedNodes.remove(seed1);
    nonAssignedNodes.remove(seed2);
    int group1 = iNode;
    int group2 = iNewNode;
    while (nonAssignedNodes.size() > 0) {
      // If one group has so few entries that all the rest must be assigned to it
      // in order to have the minimum number minSplitSize, assign them and stop
      if (nonAssignedNodes.size() + Node_size(group1) <= minSplitSize) {
        // Assign all the rest to group1
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group1, iObject);
          Node_expand(group1, iObject);
        }
        nonAssignedNodes.clear();
      } else if (nonAssignedNodes.size() + Node_size(group2) <= minSplitSize) {
        // Assign all the rest to group2
        for (int iObject : nonAssignedNodes) {
          Node_addChild(group2, iObject);
          Node_expand(group2, iObject);
        }
        nonAssignedNodes.clear();
      } else {
        // Invoke the algorithm  PickNext to choose the next entry to assign.
        int nextEntry = -1;
        double maxDiff = Double.NEGATIVE_INFINITY;
        for (int nonAssignedEntry : nonAssignedNodes) {
          double d1 = Node_volumeExpansion(group1, nonAssignedEntry);
          double d2 = Node_volumeExpansion(group2, nonAssignedEntry);
          double diff = d1 - d2;
          if (nextEntry == -1 || Math.abs(diff) > Math.abs(maxDiff)) {
            maxDiff = diff;
            nextEntry = nonAssignedEntry;
          }
        }

        // Choose which node to add the next entry to
        int iChosenNode;
        // Add it to the group whose covering rectangle will have to be enlarged
        // least to accommodate it
        if (maxDiff < 0) {
          iChosenNode = group1;
        } else if (maxDiff > 0) {
          iChosenNode = group2;
        } else {
          // Resolve ties by adding the entry to the group with smaller area
          double diffArea = Node_volume(group1) - Node_volume(group2);
          if (diffArea < 0) {
            iChosenNode = group1;
          } else if (diffArea > 0) {
            iChosenNode = group2;
          } else {
            // ... then to the one with fewer entries
            double diffSize = Node_size(group1) - Node_size(group2);
            if (diffSize < 0) {
              iChosenNode = group1;
            } else if (diffSize > 0) {
              iChosenNode = group2;
            } else {
              // ... then to either
              iChosenNode = Math.random() < 0.5? group1 : group2;
            }
          }
        }
        Node_addChild(iChosenNode, nextEntry);
        Node_expand(iChosenNode, nextEntry);
        nonAssignedNodes.remove(nextEntry);
      }
    }
    // Add the new node to the list of nodes and return its index
    return iNewNode;
  }
}
