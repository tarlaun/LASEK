/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import edu.ucr.cs.bdlab.beast.util.OperationParam;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A partitioner that uses an existing RTree as a black-box.
 * @see RTreeGuttman
 * @see RTreeGuttman#initializeFromPoints(double[][])
 * @author Ahmed Eldawy
 *
 */
public abstract class AbstractRTreeBBPartitioner extends SpatialPartitioner {
  @OperationParam(
      description = "The desired ratio between the minimum and maximum partitions sizes ]0,1[",
      defaultValue = "0.3",
      required = false
  )
  public static final String MMRatio = "mmratio";

  /**Arrays holding the coordinates*/
  protected double[][] minCoord;
  protected double[][] maxCoord;

  /**The ratio m/M used to construct the R-tree*/
  protected float mMRatio;

  /**MBR of the points used to partition the space*/
  protected final EnvelopeNDLite mbrPoints = new EnvelopeNDLite();

  @Override
  public void setup(BeastOptions conf, boolean disjoint) {
    mMRatio = conf.getFloat(MMRatio, 0.3f);
    if (disjoint)
      throw new RuntimeException("Black-box partitinoer does not support disjoint partitions");
  }

  @Override
  public void construct(Summary summary, @Required double[][] sample, AbstractHistogram histogram, int numPartitions) {
    int numDimensions = sample.length;
    assert summary.getCoordinateDimension() == sample.length;
    mbrPoints.setCoordinateDimension(summary.getCoordinateDimension());
    mbrPoints.merge(summary);
    int numSamplePoints = sample[0].length;
    int M = (int) Math.ceil((double)numSamplePoints / numPartitions);
    int m = (int) Math.ceil(mMRatio * M);

    RTreeGuttman rtree = createRTree(m, M);
    rtree.initializeFromPoints(sample);

    int numLeaves = rtree.getNumLeaves();
    minCoord = new double[numDimensions][numLeaves];
    maxCoord = new double[numDimensions][numLeaves];
    int iLeaf = 0;
    for (RTreeGuttman.Node node : rtree.getAllLeaves()) {
      for (int d = 0; d < numDimensions; d++) {
        minCoord[d][iLeaf] = node.min[d];
        maxCoord[d][iLeaf] = node.max[d];
      }
      iLeaf++;
    }
  }

  /**
   * Create the RTree that will be used to index the sample points
   * @param m the minimum size of an R-tree node
   * @param M the maximum capacity of the R-tree node
   * @return the created tree.
   */
  public abstract RTreeGuttman createRTree(int m, int M);

  @SpatialPartitioner.Metadata(
      disjointSupported = false,
      extension = "rtreebb",
      description = "Loads a sample points into an R-tree and use its leaf nodes as partition boundaries")
  public static class RTreeGuttmanBBPartitioner extends AbstractRTreeBBPartitioner {
    enum SplitMethod {LinearSplit, QuadraticSplit};
    SplitMethod splitMethod;

    @Override
    public void setup(BeastOptions conf, boolean disjoint) {
      super.setup(conf, disjoint);
      if (conf.getString("split", "linear").equalsIgnoreCase("linear")) {
        this.splitMethod = SplitMethod.LinearSplit;
      } else {
        this.splitMethod = SplitMethod.QuadraticSplit;
      }
    }

    public RTreeGuttman createRTree(int m, int M) {
      switch (splitMethod) {
        case LinearSplit: return new RTreeGuttman(m, M);
        case QuadraticSplit: return new RTreeGuttmanQuadraticSplit(m, M);
        default: return new RTreeGuttman(m, M);
      }

    }
  }

  @SpatialPartitioner.Metadata(
      disjointSupported = false,
      extension = "rstreebb",
      description = "Loads a sample points into an R*-tree and use its leaf nodes as partition boundaries")
  public static class RStarTreeBBPartitioner extends AbstractRTreeBBPartitioner {
    public RTreeGuttman createRTree(int m, int M) {
      return new RStarTree(m, M);
    }
  }

  @SpatialPartitioner.Metadata(
      disjointSupported = false,
      extension = "rrstreebb",
      description = "Loads a sample points into an RR*-tree and use its leaf nodes as partition boundaries")
  public static class RRStarTreeBBPartitioner extends AbstractRTreeBBPartitioner {
    public RTreeGuttman createRTree(int m, int M) {
      return new RRStarTree(m, M);
    }
  }

  /**
   * Tests if a partition overlaps a given rectangle
   * @param partitionID the ID of the partition to test its overlap
   * @param ienv the envelope that needs to be tested
   * @return {@code true} iff the envelope overlaps the partition boundaries.
   */
  protected boolean Partition_overlap(int partitionID, EnvelopeNDLite ienv) {
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

  /**
   * Computes the expansion that will happen on an a partition when it is enlarged to enclose a given rectangle.
   * @param iPartition the ID of the partition to compute its expansion
   * @param env the MBR of the object to be added to the partition
   * @return the expansion in the volume if the partition is epxanded to include the given envelope
   */
  protected double Partition_expansion(int iPartition, EnvelopeNDLite env) {
    double volBefore = 1.0, volAfter = 1.0;
    assert env.getCoordinateDimension() == this.getCoordinateDimension();
    for (int d = 0; d < getCoordinateDimension(); d++) {
      volBefore *= maxCoord[d][iPartition] - minCoord[d][iPartition];
      volAfter *= Math.max(maxCoord[d][iPartition], env.getMaxCoord(d)) -
          Math.min(minCoord[d][iPartition], env.getMinCoord(d));
    }
    return volAfter - volBefore;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(getCoordinateDimension());
    out.writeInt(numPartitions());
    for (int d = 0; d < getCoordinateDimension(); d++) {
      for (int i = 0; i < numPartitions(); i++) {
        out.writeDouble(minCoord[d][i]);
        out.writeDouble(maxCoord[d][i]);
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int numDimensions = in.readInt();
    int numPartitions = in.readInt();
    minCoord = new double[numDimensions][numPartitions];
    maxCoord = new double[numDimensions][numPartitions];
    for (int d = 0; d < getCoordinateDimension(); d++) {
      for (int i = 0; i < numPartitions(); i++) {
        minCoord[d][i] = in.readDouble();
        maxCoord[d][i] = in.readDouble();
      }
    }
  }

  @Override
  public boolean isDisjoint() {
    return false;
  }

  @Override
  public int getCoordinateDimension() {
    return minCoord.length;
  }

  @Override
  public int numPartitions() {
    return minCoord == null? 0 : minCoord[0].length;
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    for (int i = 0; i < minCoord[0].length; i++) {
      if (Partition_overlap(i, mbr))
        matchedPartitions.add(i);
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    double minExpansion = Double.POSITIVE_INFINITY;
    int chosenPartition = -1;
    for (int iPartition = 0; iPartition < minCoord[0].length; iPartition++) {
      double expansion = Partition_expansion(iPartition, mbr);
      if (expansion < minExpansion) {
        minExpansion = expansion;
        chosenPartition = iPartition;
      } else if (expansion == minExpansion) {
        // Resolve ties by choosing the entry with the rectangle of smallest area
        if (Partition_volume(iPartition) < Partition_volume(chosenPartition))
          chosenPartition = iPartition;
      }
    }
    return chosenPartition;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      mbr.setMinCoord(d, this.minCoord[d][partitionID]);
      mbr.setMaxCoord(d, this.maxCoord[d][partitionID]);
    }
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return mbrPoints;
  }
}
