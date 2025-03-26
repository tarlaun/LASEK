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
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import edu.ucr.cs.bdlab.beast.util.OperationParam;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * An implementation of the R*-Grove partitioner. This partitioner uses the method
 * {@link RStarTree#partitionPoints(double[][], int, int, boolean, double, AuxiliarySearchStructure)}
 * to partition a sample of points into rectangles.
 * @author Ahmed Eldawy
 *
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "rsgrove",
    description = "A partitioner that uses the R*-tree node splitting algorithm on a sample of points to partition the space"
)
public class RSGrovePartitioner extends SpatialPartitioner {

  @OperationParam(
      description = "The desired ratio between the minimum and maximum partitions sizes ]0,1[",
      defaultValue = "0.95",
      required = false
  )
  public static final String MMRatio = "mmratio";

  @OperationParam(
      description = "The minimum fraction of a split considered by the R*-tree and RR*-tree partitioners",
      defaultValue = "0.0",
      required = false
  )
  public static final String MinSplitRatio = "RSGrove.MinSplitRatio";

  /**Configuration name to expand the R*-grove partitions to infinity upon creation. Default is true. */
  public static final String ExpandToInfinity = "RSGrove.ExpandToInf";

  /**MBR of the points used to partition the space*/
  protected final EnvelopeNDLite mbrPoints = new EnvelopeNDLite();

  /**The coordinates of the minimum corner of each partition*/
  protected double[][] minCoord;
  /**The coordinates of the maximum corner of each partition*/
  protected double[][] maxCoord;

  /**The ratio m/M for partitioning the points*/
  transient protected double mMRatio;

  /**An auxiliary search structure to find matching partitions quickly*/
  protected AuxiliarySearchStructure aux;

  /**The minimum fraction of a split considered by teh R*-tree and RR*-tree partitioners*/
  protected double fractionMinSplitSize;

  /**The produced partitions should be disjoint*/
  private boolean disjointPartitions;

  /**A random number generator to assign empty geometries to random partitions for load balance*/
  transient protected Random random;

  /**Expand generated partitions to infinity to ensure it covers the entire space. Defaults to true*/
  private boolean expandToInf;

  @Override
  public void setup(BeastOptions conf, boolean disjoint) {
    this.disjointPartitions = disjoint;
    mMRatio = conf.getDouble(MMRatio, 0.95);
    this.fractionMinSplitSize = conf.getDouble(MinSplitRatio, 0.0);
    this.random = new Random();
    this.expandToInf = conf.getBoolean(ExpandToInfinity, true);
  }

  protected double Partition_expansion(int iPartition, EnvelopeNDLite env) {
    double volBefore = 1.0, volAfter = 1.0;
    assert env.getCoordinateDimension() == this.getCoordinateDimension();
    for (int d = 0; d < getCoordinateDimension(); d++) {
      volBefore *= Math.min(mbrPoints.getMaxCoord(d), maxCoord[d][iPartition]) -
          Math.max(mbrPoints.getMinCoord(d), minCoord[d][iPartition]);
      volAfter *= Math.min(mbrPoints.getMaxCoord(d), Math.max(maxCoord[d][iPartition], env.getMaxCoord(d))) -
          Math.max(mbrPoints.getMinCoord(d), Math.min(minCoord[d][iPartition], env.getMinCoord(d)));
    }
    return volAfter - volBefore;
  }

  /**
   * Tests if a partition overlaps a given rectangle
   * @param partitionID the ID of the partition to check its overlap
   * @param ienv the envelope to check for its overlap with the partition
   * @return {@code true} iff the envelope overlaps the partition.
   */
  protected boolean Partition_overlap(int partitionID, EnvelopeND ienv) {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (maxCoord[d][partitionID] <= ienv.getMinCoord(d) || ienv.getMaxCoord(d) <= minCoord[d][partitionID])
        return false;
    }
    return true;
  }

  /**
   * Computes the area of a partition.
   * @param partitionID the ID of the partition to compute its volume
   * @return the volume of the given partition
   */
  protected double Partition_volume(int partitionID) {
    double vol = 1.0;
    for (int d = 0; d < getCoordinateDimension(); d++)
      vol *= maxCoord[d][partitionID] - minCoord[d][partitionID];
    return vol;
  }

  @Override
  public void construct(Summary summary, @Required double[][] sample, @Preferred AbstractHistogram histogram, int numPartitions) {
    if (sample.length == 0) {
      // Generate random points to produce uniform partitions without worrying much about changing many part of the code
      sample = new double[summary.getCoordinateDimension()][1000];
      for (int d = 0; d < summary.getCoordinateDimension(); d++) {
        for (int i = 0; i < 1000; i++) {
          sample[d][i] = Math.random() * summary.getSideLength(d) + summary.getMinCoord(d);
        }
      }
    }
    assert mMRatio > 0 : "mMRatio cannot be zero. Make sure you call #setup() before #construct()";
    int numDimensions = sample.length;
    assert summary.getCoordinateDimension() == sample.length;
    mbrPoints.setCoordinateDimension(summary.getCoordinateDimension());
    mbrPoints.merge(summary);
    int numSamplePoints = sample[0].length;
    aux = new AuxiliarySearchStructure();
    EnvelopeNDLite[] partitionMBRs;
    if (histogram == null) {
      // No histogram! Adjust m and M based on number of sample points
      LOG.info(String.format("Partitioning the points without weight into %d partitions", numPartitions));
      int M = (int) Math.ceil((double) numSamplePoints / numPartitions);
      int m = (int) Math.ceil(mMRatio * M);
      partitionMBRs = partitionPoints(sample, M, m, expandToInf);
    } else {
      // Histogram is available and balanced size is desired. Compute points weights and partition based on the weights.
      long[] weights = computePointWeights(sample, histogram);
      long totalSize = 0;
      for (long w : weights)
        totalSize += w;
      // m and M represent data sizes so they need to be 64-bit long to support > 2G input sizes
      long m, M;
      M = (long) Math.ceil((double) totalSize / numPartitions);
      m = (long) (totalSize * mMRatio / numPartitions);
      partitionMBRs = partitionWeightedPoints(sample, weights, M, m, expandToInf);
      LOG.info(String.format("R*-Grove created %d partitions while %d were requsted", partitionMBRs.length, numPartitions));
    }

    minCoord = new double[numDimensions][partitionMBRs.length];
    maxCoord = new double[numDimensions][partitionMBRs.length];
    for (int i = 0; i < partitionMBRs.length; i++) {
      for (int d = 0; d < numDimensions; d++) {
        minCoord[d][i] = partitionMBRs[i].getMinCoord(d);
        maxCoord[d][i] = partitionMBRs[i].getMaxCoord(d);
      }
    }
  }

  /**
   * Computes the weights of points according to the histogram. The total weight of the points should be roughly
   * equal to the total weight in the histogram. Each point is assigned a weight based on its location in the histogram
   * so that it approximates the weight of its vicinity.
   * @param sample a set of points
   * @param histogram a histogram that covers all the points
   * @return an array that assigns a weight to each of the point such that the total weight is roughly equal to the
   * total weight of the histogram.
   */
  protected static long[] computePointWeights(double[][] sample, AbstractHistogram histogram) {
    int numDimensions = sample.length;
    int numPoints = sample[0].length;
    assert numDimensions == histogram.getCoordinateDimension();
    int[] numPointsPerBin = new int[histogram.getNumBins()];
    double[] coords = new double[numDimensions];
    for (int $i = 0; $i < numPoints; $i++) {
      for (int $d = 0; $d < numDimensions; $d++)
        coords[$d] = sample[$d][$i];
      int binID = histogram.getBinID(coords);
      numPointsPerBin[binID]++;
    }
    // Now compute the weight by distributing the total weight of each bucket
    long[] weights = new long[numPoints];
    for (int $i = 0; $i < numPoints; $i++) {
      for (int $d = 0; $d < numDimensions; $d++)
        coords[$d] = sample[$d][$i];
      int binID = histogram.getBinID(coords);
      weights[$i] = histogram.getBinValue(binID) / numPointsPerBin[binID];
    }
    return weights;
  }

  protected EnvelopeNDLite[] partitionPoints(double[][] coords, int max, int min) {
    return RStarTree.partitionPoints(coords, min, max, true, fractionMinSplitSize, aux);
  }

  protected EnvelopeNDLite[] partitionPoints(double[][] coords, int max, int min, boolean expandToInf) {
    return RStarTree.partitionPoints(coords, min, max, expandToInf, fractionMinSplitSize, aux);
  }

  protected EnvelopeNDLite[] partitionWeightedPoints(double[][] coords, long[] weights, long max, long min) {
    return RStarTree.partitionWeightedPoints(coords, weights, min, max, true, fractionMinSplitSize, aux);
  }

  protected EnvelopeNDLite[] partitionWeightedPoints(double[][] coords, long[] weights, long max, long min, boolean expandToInf) {
    return RStarTree.partitionWeightedPoints(coords, weights, min, max, expandToInf, fractionMinSplitSize, aux);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(mbrPoints, out);
    out.writeInt(getCoordinateDimension());
    out.writeInt(numPartitions());
    for (int d = 0; d < getCoordinateDimension(); d++) {
      for (int i = 0; i < numPartitions(); i++) {
        out.writeDouble(minCoord[d][i]);
        out.writeDouble(maxCoord[d][i]);
      }
    }
    aux.writeExternal(out);
    out.writeBoolean(disjointPartitions);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(mbrPoints, in);
    int numDimensions = in.readInt();
    int numPartitions = in.readInt();
    if (minCoord == null || numPartitions() != numPartitions || getCoordinateDimension() != numDimensions) {
      minCoord = new double[numDimensions][numPartitions];
      maxCoord = new double[numDimensions][numPartitions];
    }
    for (int d = 0; d < getCoordinateDimension(); d++) {
      for (int i = 0; i < numPartitions(); i++) {
        minCoord[d][i] = in.readDouble();
        maxCoord[d][i] = in.readDouble();
      }
    }
    if (aux == null)
      aux = new AuxiliarySearchStructure();
    aux.readExternal(in);
    disjointPartitions = in.readBoolean();
    if (random == null)
      random = new Random();
  }
  
  @Override
  public int numPartitions() {
    return minCoord == null? 0 : minCoord[0].length;
  }

  @Override
  public boolean isDisjoint() {
    return this.disjointPartitions;
  }

  @Override
  public int getCoordinateDimension() {
    return minCoord == null ? 0 : minCoord.length;
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    if (mbr.isEmpty())
      matchedPartitions.add(random.nextInt(this.numPartitions()));
    else
      aux.search(mbr, matchedPartitions);
  }

  /**
   * Multiple temporary arrays to use with the method {@link #overlapPartition(EnvelopeND)}, one per running thread
   */
  protected Map<Thread, IntArray> tempIntArrays = new HashMap<>();

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    if (mbr.isEmpty()) {
      // Special case: Assign an empty geometry to a random partition for load balance
      return random.nextInt(this.numPartitions());
    }
    double minExpansion = Double.POSITIVE_INFINITY;
    int chosenPartition = -1;
    IntArray tempPartitions = tempIntArrays.get(Thread.currentThread());
    if (tempPartitions == null) {
      tempPartitions = new IntArray();
      tempIntArrays.put(Thread.currentThread(), tempPartitions);
    }
    aux.search(new EnvelopeNDLite(mbr), tempPartitions);
    // NB tempPartitions cannot be empty because aux covers the entire space (-Infinity,+Infinity)
    if (tempPartitions.size() == 1)
      return tempPartitions.get(0);
    for (int overlappingPartition : tempPartitions) {
      double expansion = Partition_expansion(overlappingPartition, new EnvelopeNDLite(mbr));
      if (expansion < minExpansion) {
        minExpansion = expansion;
        chosenPartition = overlappingPartition;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Partition_volume(overlappingPartition) < Partition_volume(chosenPartition))
          chosenPartition = overlappingPartition;
      }
    }
    assert chosenPartition >= 0;
    return chosenPartition;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    mbr.setCoordinateDimension(getCoordinateDimension());
    mbr.setEmpty();
    // TODO find a way to avoid creating a temporary array (not major .. used only when each partition is finalized)
    double[] coord = new double[getCoordinateDimension()];
    for (int d = 0; d < getCoordinateDimension(); d++)
      coord[d] = minCoord[d][partitionID];
    mbr.merge(coord);
    for (int d = 0; d < getCoordinateDimension(); d++)
      coord[d] = maxCoord[d][partitionID];
    mbr.merge(coord);
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return mbrPoints;
  }
}
