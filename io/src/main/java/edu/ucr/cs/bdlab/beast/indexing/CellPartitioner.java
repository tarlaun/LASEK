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
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * A partitioner that partitions a file according to an existing set of cells (MBBs). It internally builds an in-memory
 * {@link RRStarTree} for the partitions to speed up the search.
 * @author Ahmed Eldawy
 *
 */
@SpatialPartitioner.Metadata(
    disjointSupported = false,
    description = "Partitions the space based on existing set of cells, e.g., another file",
    extension = "cells"
)
public class CellPartitioner extends SpatialPartitioner {

  /**An R-tree that indexes the set of existing partition for efficient search*/
  protected RTreeGuttman partitions;

  /**The list of cells that this partitioner can choose from*/
  protected EnvelopeNDLite[] cells;

  /**The degree of the R-Tree index that we use to speed up node lookup.*/
  protected static final int RTreeDegree = 32;

  /**The MBR of the input space*/
  protected final EnvelopeNDLite envelope = new EnvelopeNDLite(2);

  /**A cached value of whether this partitioner is disjoint or not. {@code null} means not set.*/
  protected Boolean disjoint;

  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public CellPartitioner() {
  }
  /**
   * Create the partitioner such that each of the given mbrs is represented as once cell.
   * @param mbrs the mbrs that define the extents of the cells
   */
  public CellPartitioner(EnvelopeNDLite ... mbrs) {
    this.cells = new EnvelopeNDLite[mbrs.length];
    for (int i = 0; i < mbrs.length; i++)
      this.cells[i] = new EnvelopeNDLite(mbrs[i]);
    this.initializeRTree(this.cells);
  }

  /**
   * Initialize the cell partitioner from another partitioner
   * @param partitioner
   */
  public CellPartitioner(SpatialPartitioner partitioner) {
    List<EnvelopeNDLite> partitionMBRs = new ArrayList<>();
    for (int i = 0; i < partitioner.numPartitions(); i++) {
      EnvelopeNDLite partitionMBR = partitioner.getPartitionMBR(i);
      if (!partitionMBRs.contains(partitionMBR))
        partitionMBRs.add(partitionMBR);
    }
    this.cells = new EnvelopeNDLite[partitionMBRs.size()];
    for (int i = 0; i < partitionMBRs.size(); i++)
      this.cells[i] = new EnvelopeNDLite(partitionMBRs.get(i));
    initializeRTree(cells);
  }

  @Override
  public void construct(Summary summary, double[][] sample, AbstractHistogram histogram, int numPartitions) {
    throw new RuntimeException("Not supported! Please use the method initialize(PartitionInfo[]) to create.");
  }

  /**
   * Initialize the R-tree from the list of cells.
   * @param cells
   */
  private void initializeRTree(EnvelopeNDLite[] cells) {
    double[] x1s = new double[cells.length];
    double[] y1s = new double[cells.length];
    double[] x2s = new double[cells.length];
    double[] y2s = new double[cells.length];

    envelope.setEmpty();

    for (int i = 0; i < cells.length; i++) {
      x1s[i] = cells[i].getMinCoord(0);
      y1s[i] = cells[i].getMinCoord(1);
      x2s[i] = cells[i].getMaxCoord(0);
      y2s[i] = cells[i].getMaxCoord(1);
      envelope.setMinCoord(0, Math.min(envelope.getMinCoord(0), x1s[i]));
      envelope.setMinCoord(1, Math.min(envelope.getMinCoord(1), y1s[i]));
      envelope.setMaxCoord(0, Math.min(envelope.getMinCoord(0), x2s[i]));
      envelope.setMaxCoord(1, Math.min(envelope.getMinCoord(1), y2s[i]));
    }

    // The RR*-tree paper recommends setting m = 0.2 M
    partitions = new RRStarTree(RTreeDegree / 5, RTreeDegree);
    partitions.initializeHollowRTree(x1s, y1s, x2s, y2s);
    disjoint = null;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(this.envelope, out);
    out.writeInt(cells.length);
    out.writeInt(envelope.getCoordinateDimension());
    for (EnvelopeNDLite cell : cells) {
      for (int d = 0; d < cell.getCoordinateDimension(); d++) {
        out.writeDouble(cell.getMinCoord(d));
        out.writeDouble(cell.getMaxCoord(d));
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(this.envelope, in);
    int numCells = in.readInt();
    int numDimensions = in.readInt();
    if (cells == null || cells.length != numCells) {
      // Initialize the array of cells only if needed
      cells = new EnvelopeNDLite[numCells];
      for (int i = 0; i < numCells; i++)
        cells[i] = new EnvelopeNDLite();
    }
    for (int i = 0; i < numCells; i++) {
      cells[i].setCoordinateDimension(numDimensions);
      for (int d = 0; d < numDimensions; d++) {
        cells[i].setMinCoord(d, in.readDouble());
        cells[i].setMaxCoord(d, in.readDouble());
      }
    }
    // Re-initialize the R-tree
    initializeRTree(cells);
  }
  
  @Override
  public int numPartitions() {
    return cells == null ? 0 : cells.length;
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    for (RTreeGuttman.Entry e : partitions.search(mbr))
      matchedPartitions.add(e.id);
  }

  @Override
  public int overlapPartition(EnvelopeNDLite geomMBR) {
    // TODO avoid construction of the IntArray multiple times
    IntArray tempPartitions = new IntArray();
    double[] mbrMin = new double[geomMBR.getCoordinateDimension()];
    double[] mbrMax = new double[geomMBR.getCoordinateDimension()];
    for (int $d = 0; $d < geomMBR.getCoordinateDimension(); $d++) {
      mbrMin[$d] = geomMBR.getMinCoord($d);
      mbrMax[$d] = geomMBR.getMaxCoord($d);
    }
    partitions.search(mbrMin, mbrMax, tempPartitions);
    int chosenCellIndex;
    if (tempPartitions.size() == 1) {
      // Only one overlapping node, return it
      chosenCellIndex = tempPartitions.peek();
    } else if (tempPartitions.size() > 0) {
      // More than one overlapping cells, choose the best between them
      chosenCellIndex = -1;
      double minVol = Double.POSITIVE_INFINITY;
      double minPerim = Double.POSITIVE_INFINITY;
      for (int overlappingCellIndex : tempPartitions) {
        EnvelopeNDLite overlappingCell = cells[overlappingCellIndex];
        double vol = overlappingCell.getArea();
        if (vol < minVol) {
          minVol = vol;
          minPerim = overlappingCell.getSideLength(0) + overlappingCell.getSideLength(1);
          chosenCellIndex = overlappingCellIndex;
        } else if (vol == minVol) {
          // This also covers the case of vol == minVol == 0
          double cellPerimeter = overlappingCell.getSideLength(0) + overlappingCell.getSideLength(1);
          if (cellPerimeter < minPerim) {
            minPerim = cellPerimeter;
            chosenCellIndex = overlappingCellIndex;
          }
        }
      }
    } else {
      // No overlapping cells, follow the (fake) insert choice
      chosenCellIndex = partitions.noInsert(new double[] {geomMBR.getMinCoord(0), geomMBR.getMinCoord(1)},
          new double[] {geomMBR.getMaxCoord(0), geomMBR.getMaxCoord(1)});
    }
    return chosenCellIndex;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    mbr.set(cells[partitionID]);
  }

  @Override
  public boolean isDisjoint() {
    if (disjoint == null) {
      // Make a self join between the cells and return true if the result is empty
      for (int i = 0; i < cells.length && disjoint == null; i++) {
        for (int j = i + 1; j < cells.length && disjoint == null; j++) {
          if (cells[i].intersectsEnvelope(cells[j]))
            disjoint = false;
        }
      }
      if (disjoint == null)
        disjoint = true;
    }
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    return cells[0].getCoordinateDimension();
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return envelope;
  }
}
