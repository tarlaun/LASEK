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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A class that stores an auxiliary data structure used to search through
 * the partitions created using the functions
 * {@link RStarTree#partitionPoints(double[][], int, int, boolean, double, AuxiliarySearchStructure)}
 * and
 * {@link RRStarTree#partitionPoints(double[][], int, int, boolean, double, AuxiliarySearchStructure)}
 */
public class AuxiliarySearchStructure implements Externalizable {
  /**The first split to consider*/
  public int rootSplit;
  /**The coordinate along where the split happens*/
  public double[] splitCoords;
  /**The axis along where the partition happened.*/
  public IntArray splitAxis;
  /**
   * The next partition to consider if the search point is less than the
   * split line. If the number is negative, it indicates that the search is
   * terminated and a partition is matched. The partition index is (-x-1)
   **/
  public int[] partitionLessThan;
  /**
   * The next partition to consider if the search point is greater than or
   * equal to the split line.
   */
  public int[] partitionGreaterThanOrEqual;

  /**
   * Returns the ID of the partition that contain the given point
   * @param coord the coordinate of the point to search
   * @return the index of the partition that contains the given point
   */
  public int search(double[] coord) {
    if (splitCoords.length == 0)
      return 0;
    int iSplit = rootSplit;
    while (iSplit >= 0) {
      // Choose which coordinate to work with depending on the split axis
      double coordToConsider = coord[splitAxis.get(iSplit)];
      iSplit = (coordToConsider < splitCoords[iSplit] ? partitionLessThan : partitionGreaterThanOrEqual)[iSplit];
    }
    // Found the terminal partition, return its correct ID
    return -iSplit - 1;
  }

  /**
   * Find all the overlapping partitions for a given rectangular area
   * @param e the envelope (MBR) of the search region
   * @param ps the array of results
   */
  public void search(EnvelopeNDLite e, IntArray ps) {
    // We to keep a stack of splits to consider.
    // For efficiency, we reuse the given IntArray (ps) where we store the
    // the matching partitions from one end and the splits to be considered
    // from the other end
    // A negative number indicates a matching partition
    ps.clear();
    if (splitCoords.length == 0) {
      // No splits. Always return 0 which is the only partition we have
      ps.add(0);
      return;
    }
    IntArray splitsToSearch = ps;
    int iSplit = rootSplit;
    while (iSplit >= 0) {
      double coordMin = e.getMinCoord(splitAxis.get(iSplit));
      double coordMax = e.getMaxCoord(splitAxis.get(iSplit));
      if (coordMax < splitCoords[iSplit]) {
        // Only the first half-space matches
        iSplit = partitionLessThan[iSplit];
      } else if (coordMin >= splitCoords[iSplit]) {
        // Only the second half-space matches
        iSplit = partitionGreaterThanOrEqual[iSplit];
      } else {
        // The entire space is still to be considered
        if (partitionGreaterThanOrEqual[iSplit] >= 0) {
          // A split that needs to be further considered
          splitsToSearch.add(partitionGreaterThanOrEqual[iSplit]);
        } else {
          // A terminal partition that should be matched
          splitsToSearch.insert(0, partitionGreaterThanOrEqual[iSplit]);
        }
        iSplit = partitionLessThan[iSplit];
      }
      // If iSplit reaches a terminal partitions, add it to the answer and
      // move on to the next split
      while (iSplit < 0) {
        ps.insert(0, iSplit);
        if (splitsToSearch.peek() >= 0)
          iSplit = splitsToSearch.pop();
        else
          break;
      }
    }
    // Convert the matching splits from their negative number to the correct
    // partitionID
    for (int i = 0; i < ps.size(); i++)
      ps.set(i, -ps.get(i) - 1);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(rootSplit);
    out.writeInt(splitCoords.length);
    for (int i = 0; i < splitCoords.length; i++) {
      out.writeDouble(splitCoords[i]);
      out.writeInt(partitionLessThan[i]);
      out.writeInt(partitionGreaterThanOrEqual[i]);
    }
    splitAxis.writeExternal(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    rootSplit = in.readInt();
    int numOfSplits = in.readInt();
    if (splitCoords == null) splitCoords = new double[numOfSplits];
    if (partitionLessThan == null) partitionLessThan = new int[numOfSplits];
    if (partitionGreaterThanOrEqual == null) partitionGreaterThanOrEqual = new int[numOfSplits];
    for (int i = 0; i < numOfSplits; i++) {
      splitCoords[i] = in.readDouble();
      partitionLessThan[i] = in.readInt();
      partitionGreaterThanOrEqual[i] = in.readInt();
    }
    if (splitAxis == null) splitAxis = new IntArray();
    splitAxis.readExternal(in);
  }
}