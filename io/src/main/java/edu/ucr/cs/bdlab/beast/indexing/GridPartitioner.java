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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * A partitioner that partitions data using a uniform grid.
 * If a shape overlaps multiple grids, it replicates it to all overlapping
 * partitions.
 * @author Ahmed Eldawy
 *
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "grid",
    description = "Partitions the space using a uniform grid with roughly square cells")
public class GridPartitioner extends SpatialPartitioner {
  /**The MBR of the region to be partitioned*/
  protected final EnvelopeNDLite gridMBR = new EnvelopeNDLite();

  /**Total number of partitions along each dimension*/
  public int[] numPartitions;

  /**Create disjoint partitions*/
  private boolean disjointPartitions;

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public GridPartitioner() {
  }

  public GridPartitioner(EnvelopeNDLite mbr, int[] numPartitionsPerAxis) {
    assert mbr.getCoordinateDimension() == numPartitionsPerAxis.length;
    this.gridMBR.set(mbr);
    this.numPartitions = Arrays.copyOf(numPartitionsPerAxis, numPartitionsPerAxis.length);
    this.disjointPartitions = true;
  }

  public GridPartitioner(EnvelopeNDLite mbr, int numCells) {
    this.numPartitions = new int[mbr.getCoordinateDimension()];
    computeNumberOfPartitionsAlongAxes(new EnvelopeNDLite(mbr), numCells, this.numPartitions);
    this.gridMBR.set(mbr);
    this.disjointPartitions = true;
  }

  @Override
  public void setup(BeastOptions conf, boolean disjoint) {
    this.disjointPartitions = disjoint;
  }

  @Override
  public void construct(Summary summary, double[][] sample, AbstractHistogram histogram, int numBuckets) {
    this.gridMBR.set(summary);
    numPartitions = new int[summary.getCoordinateDimension()];
    computeNumberOfPartitionsAlongAxes(summary, numBuckets, numPartitions);
  }

  public void setDisjointPartitions(boolean disjoint) {
    this.disjointPartitions = disjoint;
  }

  /**
   * Computes the total number of partitions along each axis in order to produce square cells of roughly equal volume
   * while adhering to the given number of partitions. It first computes the volume of the input space and divides it
   * by the desired number of partitions to compute the volume of each cell. Then, it takes the kth root to compute the
   * side length. Finally, it iterates over the dimensions to compute the desired number of partitions along each axis.
   * While doing that final step, it keeps into account that the total number of partitions should not exceed the given
   * number.
   * @param mbr the MBR of the input space
   * @param numCells the desired number of cells
   * @param numPartitions (out) the computed number of partitions along each axis
   */
  public static void computeNumberOfPartitionsAlongAxes(EnvelopeNDLite mbr, int numCells, int[] numPartitions) {
    // Divide the space into equi-sized square cells
    double cellSideLength = Math.pow(mbr.getArea() / numCells, 1.0/mbr.getCoordinateDimension());
    int totalNumPartitions = 1;
    for (int d = 1; d < mbr.getCoordinateDimension(); d++) {
      numPartitions[d] = Math.max(1, (int) Math.floor(mbr.getSideLength(d) / cellSideLength));
      totalNumPartitions *= numPartitions[d];
    }
    numPartitions[0] = Math.max(1, (int) Math.floor((double) numCells / totalNumPartitions));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(gridMBR, out);
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++)
      out.writeInt(numPartitions[d]);
    out.writeBoolean(disjointPartitions);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(gridMBR, in);
    if (numPartitions == null || numPartitions.length != gridMBR.getCoordinateDimension())
      numPartitions = new int[gridMBR.getCoordinateDimension()];
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++)
      numPartitions[d] = in.readInt();
    this.disjointPartitions = in.readBoolean();
  }

  @Override
  public int numPartitions() {
    int count = 1;
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++)
      count *= numPartitions[d];
    return count;
  }

  @Override
  public boolean isDisjoint() {
    return this.disjointPartitions;
  }

  @Override
  public int getCoordinateDimension() {
    return numPartitions.length;
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite recordMBR, IntArray matchedPartitions) {
    if (gridMBR.isEmpty())
      return;
    int[] overlapMin = new int[gridMBR.getCoordinateDimension()];
    int[] overlapMax = new int[gridMBR.getCoordinateDimension()];
    int numResults = 1;
    matchedPartitions.clear();
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++) {
      if (recordMBR.getMinCoord(d) == recordMBR.getMaxCoord(d) && gridMBR.getMaxCoord(d) == recordMBR.getMaxCoord(d)) {
        // Special case for a point record that is exactly at the upper boundary of the grid
        overlapMin[d] = numPartitions[d] - 1;
        overlapMax[d] = numPartitions[d];
      } else {
        // Find overlapping partitions
        overlapMin[d] = (int) Math.floor((recordMBR.getMinCoord(d) - gridMBR.getMinCoord(d)) * numPartitions[d] / gridMBR.getSideLength(d));
        overlapMax[d] = (int) Math.ceil((recordMBR.getMaxCoord(d) - gridMBR.getMinCoord(d)) * numPartitions[d] / gridMBR.getSideLength(d));
        if (overlapMax[d] == overlapMin[d]) {
          // Special case when the coordinate perfectly aligns with the grid boundary
          overlapMax[d]++;
        }
        if (overlapMin[d] < 0)
          overlapMin[d] = 0;
        if (overlapMax[d] > numPartitions[d])
          overlapMax[d] = numPartitions[d];
      }
      // An inverted range indicates no ovelrap
      if (overlapMax[d] <= overlapMin[d])
        return;
      numResults *= overlapMax[d] - overlapMin[d];
    }

    // The partition number is internally represented as a d-dimensional number
    // TODO avoid creating this array for each call of this function
    int[] partitionNumber = new int[gridMBR.getCoordinateDimension()];

    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++)
      partitionNumber[d] = overlapMin[d];

    for (int p = 0; p < numResults; p++) {
      int combinedPartitionNumber = 0;
      int d = gridMBR.getCoordinateDimension();
      while (d-- > 0) {
        combinedPartitionNumber *= numPartitions[d];
        combinedPartitionNumber += partitionNumber[d];
      }
      matchedPartitions.add(combinedPartitionNumber);
      // Move to next partition
      d = 0;
      while (d < partitionNumber.length && ++partitionNumber[d] >= overlapMax[d]) {
        partitionNumber[d] = overlapMin[d];
        d++;
      }
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    int combinedPartitionNumber = 0;
    int d = gridMBR.getCoordinateDimension();
    while (d-- > 0) {
      int pos = (int) Math.floor((mbr.getCenter(d) - gridMBR.getMinCoord(d)) * numPartitions[d] / gridMBR.getSideLength(d));
      pos = Math.min(pos, numPartitions[d] - 1);
      combinedPartitionNumber *= numPartitions[d];
      combinedPartitionNumber += pos;
    }
    return combinedPartitionNumber;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    mbr.setCoordinateDimension(gridMBR.getCoordinateDimension());
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++) {
      int pos = partitionID % numPartitions[d];
      if (pos == 0)
        mbr.setMinCoord(d, gridMBR.getMinCoord(d));
      else
        mbr.setMinCoord(d, (gridMBR.getMinCoord(d) * (numPartitions[d] - pos) + gridMBR.getMaxCoord(d) * pos) / numPartitions[d]);
      // While the calculation of both cases is similar, this if condition eliminates the numerical error that might
      // result in the last cell along one dimension to not match with the gridMBR
      if (pos == numPartitions[d] - 1)
        mbr.setMaxCoord(d, gridMBR.getMaxCoord(d));
      else
        mbr.setMaxCoord(d, (gridMBR.getMinCoord(d) * (numPartitions[d] - (pos+ 1)) + gridMBR.getMaxCoord(d) * (pos + 1)) / numPartitions[d]);
      partitionID /= numPartitions[d];
    }
  }

  /**
   * Gets the grid position of a given partition ID. The grid position is where this partition is located along
   * all dimensions of the grid. The position on each axis i is in the range [0, num partitions along axis i[.
   * @param partitionID the ID of the partition
   * @return the position of the partition in the grid.
   */
  public int[] getGridPosition(int partitionID) {
    int[] gridPosition = new int[numPartitions.length];
    for (int d = 0; d < gridMBR.getCoordinateDimension(); d++) {
      gridPosition[d] = partitionID % numPartitions[d];
      partitionID /= numPartitions[d];
    }
    return gridPosition;
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return gridMBR;
  }

  @Override
  public String toString() {
    return "GridPartitioner{" +
        "gridMBR=" + gridMBR +
        ", numPartitions=" + Arrays.toString(numPartitions) +
        '}';
  }
}
