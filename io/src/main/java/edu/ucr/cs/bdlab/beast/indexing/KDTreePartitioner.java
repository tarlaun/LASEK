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
 * A partitioner that partitions the space using a KD-tree structure.
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "kdtree",
    description = "Recursively partitions the space in half (based on the median) alternating between the dimensions " +
        "until a specific number of partitions is reached.")
public class KDTreePartitioner extends SpatialPartitioner {
  /**Whether disjoint partitions are desired*/
  protected boolean disjoint;

  /**
   * The coordinates of the split lines for the KD-tree. Notice that the KD-tree is a complete binary tree. Each entry
   * in this array indicates the coordinate of the split line along one of the axes. To navigate the tree left or right,
   * we rely on the level order traversal of a binary tree (similar to the heap structure). To access the left and
   * right children of a node {@code i}, we use the expressions {@code 2i+1} and {@code 2i+2}, respectively. Once the
   * index goes beyond the size of the array, it indicates reaching a leaf node (partition) in the tree.
   */
  protected double[] splitCoords;

  /**
   * The axis of each split in the splitCoords array. This value can be computed as {@code numOfSignificantBits(i+1)}
   * but we cache it in this array for efficiency.
   */
  protected int[] splitAxis;

  /**
   * Number of dimensions for this tree
   */
  protected int numDimensions;

  /**The MBR of the input space*/
  protected final EnvelopeNDLite envelope = new EnvelopeNDLite();

  @Override
  public void setup(BeastOptions context, boolean disjoint) {
    this.disjoint = disjoint;
  }

  @Override
  public void construct(Summary summary, @Required double[][] sample, AbstractHistogram histogram, int numPartitions) {
    this.envelope.set(summary);
    numDimensions = sample.length;
    final int sampleCount = sample[0].length;
    assert numDimensions == summary.getCoordinateDimension();
    assert numPartitions > 0;
    assert numPartitions <= sampleCount;
    splitCoords = new double[numPartitions - 1];
    splitAxis = new int[numPartitions - 1];
    if (numPartitions > 1) {
      /**
       * Recursively split the space partition into two alternating over the axis. Since we split the partitions in the
       * order they are created, we end up with a complete binary tree (like the one for heaps). This tree is stored
       * in an array format where the root is at the array index 0. We only store the internal (non-leaf) nodes and
       * the leaf nodes are indicated with a negative index.
       */
      class SplitRange {
        int start, end;
        int splitAxis;
        /**The ID of the split (the node in the KD-tree)*/
        int splitID;

        SplitRange(int splitID, int s, int e, int x) {
          this.splitID = splitID;
          this.start = s;
          this.end = e;
          this.splitAxis = x;
        }
      }

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

      // Recursively split the input space
      Queue<SplitRange> toSplit = new ArrayDeque<>();
      toSplit.add(new SplitRange(0, 0, sampleCount, 0));
      while (!toSplit.isEmpty()) {
        SplitRange rangeToSplit = toSplit.remove();
        sorter.axis = rangeToSplit.splitAxis;
        quickSort.sort(sorter, rangeToSplit.start, rangeToSplit.end);
        int m = (rangeToSplit.start + rangeToSplit.end + 1) / 2;
        splitCoords[rangeToSplit.splitID] = (sample[rangeToSplit.splitAxis][m-1] + sample[rangeToSplit.splitAxis][m]) / 2.0;
        splitAxis[rangeToSplit.splitID] = rangeToSplit.splitAxis;
        // Define the two new possible split tasks
        int lowerSplitID = rangeToSplit.splitID * 2 + 1;
        int upperSplitID = lowerSplitID + 1;
        if (upperSplitID < splitCoords.length) {
          // Two new splits are needed. Will create a new object for the upper split task
          SplitRange rightSplitTask = new SplitRange(upperSplitID, m, rangeToSplit.end, (rangeToSplit.splitAxis + 1) % numDimensions);
          toSplit.add(rightSplitTask);
        }
        if (lowerSplitID < splitCoords.length) {
          // Reuse the current object for the lower split
          rangeToSplit.end = m;
          rangeToSplit.splitID = lowerSplitID;
          rangeToSplit.splitAxis = (rangeToSplit.splitAxis + 1) % numDimensions;
          toSplit.add(rangeToSplit);
        }
      }
    }


  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    // To avoid creating a new array, we reuse the given array as a stack for searching
    int numPartitionsToSearch = 1; // The size of the search stack
    matchedPartitions.add(0); // Start at the root
    while (numPartitionsToSearch > 0) {
      // Pop the next partition to search
      int partitionID = matchedPartitions.pop();
      numPartitionsToSearch--;
      while (partitionID < splitCoords.length) {
        int axis = splitAxis[partitionID];
        if (mbr.getMaxCoord(axis) < splitCoords[partitionID]) {
          // Need to check the lower partition only
          partitionID = 2 * partitionID + 1;
        } else if (mbr.getMinCoord(axis) > splitCoords[partitionID]) {
          // Need to check the upper partition only
          partitionID = 2 * partitionID + 2;
        } else {
          // Need to check both partitions (lower and upper)
          int upperPartitionID = 2 * partitionID + 2;
          if (upperPartitionID >= splitCoords.length) {
            // Add as a matched partition
            matchedPartitions.insert(numPartitionsToSearch, upperPartitionID);
          } else {
            // Non-leaf partition, still needs to be searched
            matchedPartitions.add(upperPartitionID);
            numPartitionsToSearch++;
          }
          partitionID = 2 * partitionID + 1; // Navigate directly to the lower partition
        }
      }
      // Found a leaf partition, add it to the result
      matchedPartitions.insert(numPartitionsToSearch, partitionID);
    }
    for (int $i = 0; $i < matchedPartitions.size(); $i++) {
      matchedPartitions.set($i, matchedPartitions.get($i) - splitCoords.length);
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    int partition = 0; // Start at the root
    while (partition < splitCoords.length) {
      // Compare the center of the MBR along the current axis to the split
      int axis = splitAxis[partition];
      double center = (mbr.getMinCoord(axis) + mbr.getMaxCoord(axis)) / 2.0;
      if (center < splitCoords[partition])
        partition = partition * 2 + 1;
      else
        partition = partition * 2 + 2;
    }
    return partition - splitCoords.length;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    partitionID += splitCoords.length;
    // Initialize the given envelope to cover the entire space from negative infinity to positive infinity
    for (int d = 0; d < mbr.getCoordinateDimension(); d++) {
      mbr.setMinCoord(d, Double.NEGATIVE_INFINITY);
      mbr.setMaxCoord(d, Double.POSITIVE_INFINITY);
    }
    // Navigate the KD-tree up to the root and update the coordinates based on the split coordinates and axes
    while (partitionID > 0) {
      int parentPartitionID = (partitionID - 1) / 2;
      int axis = splitAxis[parentPartitionID];
      if ((partitionID & 1) == 1) {
        // The desired partition is the lower partition, i.e., the split defines the upper coordinate
        // Since the maxCoord could have been defined already by a child partition, we are only allowed to shrink it
        mbr.setMaxCoord(axis, Math.min(mbr.getMaxCoord(axis), splitCoords[parentPartitionID]));
      } else {
        // The desired partition is the upper partition, i.e., the split defines the lower coordinate
        mbr.setMinCoord(axis, Math.max(mbr.getMinCoord(axis), splitCoords[parentPartitionID]));
      }
      partitionID = parentPartitionID;
    }
  }

  @Override
  public int numPartitions() {
    return splitCoords.length + 1;
  }

  @Override
  public boolean isDisjoint() {
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    return numDimensions;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(this.envelope, out);
    out.writeInt(numDimensions);
    out.writeInt(splitCoords.length);
    for (int i = 0; i < splitCoords.length; i++) {
      out.writeDouble(splitCoords[i]);
      out.writeInt(splitAxis[i]);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(this.envelope, in);
    numDimensions = in.readInt();
    int numSplits = in.readInt();
    if (splitCoords == null || numSplits != splitCoords.length) {
      this.splitCoords = new double[numSplits];
      this.splitAxis = new int[numSplits];
    }
    for (int i = 0; i < splitCoords.length; i++) {
      splitCoords[i] = in.readDouble();
      splitAxis[i] = in.readInt();
    }
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return envelope;
  }
}
