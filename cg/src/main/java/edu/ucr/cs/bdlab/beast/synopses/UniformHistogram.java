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
package edu.ucr.cs.bdlab.beast.synopses;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.util.KryoInputToObjectInput;
import edu.ucr.cs.bdlab.beast.util.KryoOutputToObjectOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A uniform grid histogram storing Long values.
 */
public class UniformHistogram extends AbstractHistogram implements Externalizable, KryoSerializable {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(UniformHistogram.class);

  /**The frequency value of the histogram*/
  protected long[] values;

  /**Default constructor is needed for deserialization*/
  public UniformHistogram() { }

  public UniformHistogram(EnvelopeNDLite mbr, int ... numPartitions) {
    this.set(mbr);
    this.numPartitions = Arrays.copyOf(numPartitions, numPartitions.length);
    int totalLength = 1;
    for (int $d = 0; $d < numPartitions.length; $d++)
      totalLength *= numPartitions[$d];
    values = new long[totalLength];
  }

  /**
   * Computes a reasonable number of partitions along each axis to produce at most (but not much lower) than the
   * given number of buckets in the grid. It tries to make the side lengths of each cell as equal as possible.
   * In other words, it tries to produce cells that are closes to squares (or cubes ...).
   * @param mbr the minimum bounding rectangle of the input space
   * @param numBuckets the desired number of buckets to create, does not have to be followed strictly
   * @return the number of partitions along each dimension
   */
  public static int[] computeNumPartitions(EnvelopeNDLite mbr, int numBuckets) {
    double totalVolume = mbr.getArea();
    double cellVolume = totalVolume / numBuckets;
    double cellSideLength = Math.pow(cellVolume, 1.0 / mbr.getCoordinateDimension());
    int[] numPartitions = new int[mbr.getCoordinateDimension()];
    long totalNumPartitions = 1;
    for (int $d = 0; $d < mbr.getCoordinateDimension(); $d++) {
      numPartitions[$d] = Math.max(1, (int) Math.round(mbr.getSideLength($d) / cellSideLength));
      if (totalNumPartitions * numPartitions[$d] > numBuckets)
        numPartitions[$d] = (int) Math.max(1, numBuckets / totalNumPartitions);
      totalNumPartitions *= numPartitions[$d];
    }
    return numPartitions;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(this, out);
    for (int d = 0; d < getCoordinateDimension(); d++)
      out.writeInt(numPartitions[d]);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(baos));
    for (long value : values)
      dos.writeLong(value);
    dos.close();

    long t1 = System.nanoTime();
    byte[] serializedData = baos.toByteArray();
    out.writeInt(serializedData.length);
    out.write(serializedData);
    long t2 = System.nanoTime();
    LOG.debug("Serialized a histogram into "+serializedData.length+" bytes in "+(t2-t1)*1E-9+" seconds");
  }

  @Override
  public void write(Kryo kryo, Output output) {
    try {
      writeExternal(new KryoOutputToObjectOutput(kryo, output));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(this, in);
    this.numPartitions = new int[getCoordinateDimension()];
    int numBins = 1;
    for (int d = 0; d < getCoordinateDimension(); d++) {
      numPartitions[d] = in.readInt();
      numBins *= numPartitions[d];
    }
    if (values == null || numBins != values.length)
      values = new long[numBins];
    int compressedDataLength = in.readInt();
    byte[] compressedData = new byte[compressedDataLength];
    in.readFully(compressedData);
    long t1 = System.nanoTime();
    DataInputStream din = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(compressedData)));
    for (int i = 0; i < numBins; i++)
      values[i] = din.readLong();
    long t2 = System.nanoTime();
    LOG.debug("Deserialized a histogram with "+numBins+" bins in "+(t2-t1)*1E-9+" seconds");
  }

  @Override
  public void read(Kryo kryo, Input input) {
    try {
      readExternal(new KryoInputToObjectInput(kryo, input));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds a value to a specific entry position in the histogram
   * @param position the position of the bin
   * @param size the size to add to the given bin
   */
  public void addEntry(int[] position, long size) {
    assert position.length == getCoordinateDimension();
    int pos = getBinID(position, numPartitions);
    if (pos >= 0 && pos < values.length)
      values[pos] += size;
  }

  public static final int getBinID(int[] position, int[] numPartitions) {
    assert position.length == numPartitions.length;
    int d = position.length;
    int pos = 0;
    while (d-- > 0) {
      pos *= numPartitions[d];
      pos += position[d];
    }
    return pos;
  }

  /**
   * Add a value in the entry at the given point location.
   * @param coord the coordinates of the point
   * @param size the size of the point
   */
  public void addPoint(double[] coord, long size) {
    assert coord.length == getCoordinateDimension();
    int binID = getPointBinID(coord, this, this.numPartitions);
    if (binID >= 0 && binID < values.length)
      values[binID] += size;
  }

  /**
   * Returns the ID of the bin that contains the given point.
   * @param coord the coordinate of the point
   * @param mbb the minimum bounding box of the histogram
   * @param numPartitions the number of partitions per dimension in the histogram
   * @return the ID of the bin that contains the point or -1 if it is out of range
   */
  public static int getPointBinID(double[] coord, EnvelopeNDLite mbb, int[] numPartitions) {
    assert coord.length == numPartitions.length;
    int d = coord.length;
    int binID = 0;
    while (d-- > 0) {
      binID *= numPartitions[d];
      int position;
      if (coord[d] == mbb.getMaxCoord(d))
        position = numPartitions[d] - 1;
      else
        position = (int) Math.floor((coord[d] - mbb.getMinCoord(d)) * numPartitions[d] / mbb.getSideLength(d));
      if (position < 0 || position >= numPartitions[d])
        return -1;
      binID += position;
    }
    return binID;
  }

  /**
   * Returns the ID of the bin that contains the given point.
   * @param point the point
   * @param mbb the minimum bounding box of the histogram
   * @param numPartitions the number of partitions per dimension in the histogram
   * @return the ID of the bin that contains the point or -1 if it is out of range
   */
  public static int getPointBinID(PointND point, EnvelopeNDLite mbb, int[] numPartitions) {
    assert point.getCoordinateDimension() == numPartitions.length;
    int d = numPartitions.length;
    int binID = 0;
    while (d-- > 0) {
      binID *= numPartitions[d];
      // Special case for points that lie exactly at the end of this dimension
      // This case allows points lying at the upper sides of the MBR to be accounted.
      int position;
      if (point.getCoordinate(d) == mbb.getMaxCoord(d))
        position = numPartitions[d] - 1;
      else
        position = (int) Math.floor((point.getCoordinate(d) - mbb.getMinCoord(d)) * numPartitions[d] / mbb.getSideLength(d));
      if (position < 0 || position >= numPartitions[d])
        return -1;
      binID += position;
    }
    return binID;
  }

  public void addPoint(PointND p, long size) {
    assert p.getCoordinateDimension() == getCoordinateDimension();
    int[] position = new int[p.getCoordinateDimension()];
    for (int d = 0; d < p.getCoordinateDimension(); d++) {
      position[d] = (int) Math.floor((p.getCoordinate(d) - this.getMinCoord(d)) * this.numPartitions[d] / this.getSideLength(d));
      position[d] = Math.min(position[d], numPartitions[d] - 1);
    }
    addEntry(position, size);
  }

  /**
   * Merges with another histogram that is perfectly aligned with this histogram, i.e., the same MBR and the same
   * number of rows and columns
   * @param another the other histogram to merge with
   * @return this histogram to call serially
   */
  public UniformHistogram mergeAligned(UniformHistogram another) {
    assert this.isAligned(another);
    for (int i = 0; i < values.length; i++)
      this.values[i] += another.values[i];
    return this;
  }

  /**
   * Compute the volume of the overlap between two buckets in two histogram.
   * @param h1 the first histogram
   * @param p1 the position of the bucket in the first histogram
   * @param h2 the second histogram
   * @param p2 the position of the bucket in the second histogram
   * @return the volume of the overlap region between the two buckets. This number will be zero if the two buckets
   * do not overlap.
   */
  public static double getOverlapVolume(UniformHistogram h1, int[] p1, UniformHistogram h2, int[] p2)
  {
    assert h1.getCoordinateDimension() == p1.length : "Invalid bucket ID for the first partition";
    assert h2.getCoordinateDimension() == p2.length : "Invalid bucket ID for the second partition";
    assert p1.length == p2.length : "Mismatching number of dimensions for the two histogram";

    double overlapVolume = 1.0;

    for(int d = 0; d < h1.getCoordinateDimension(); d++){
      double overlapD = Math.min(h1.getPartitionMax(p1[d], d), h2.getPartitionMax(p2[d], d)) -
          Math.max(h1.getPartitionMin(p1[d], d), h2.getPartitionMin(p2[d], d));
      // If the buckets are disjoint, return 0
      if (overlapD <= 0)
        return 0;
      overlapVolume *= overlapD;
    }
    return overlapVolume;
  }

  public int getBucketID(int[] i)
  {
    int dims = getCoordinateDimension();
    int pos = 0;
    while (dims-- > 0) {
      pos *= getNumPartitions(dims);
      pos += i[dims];
    }

    return pos;
  }

  /**
   * Returns the lower coordinate of the buckets in the given dimension (inclusive)
   * @param i the index of the partition
   * @param d the dimension
   * @return the lower coordinate of the buckets at the i<sup>th</sup> partition along the d dimension
   */
  public double getPartitionMin(int i, int d) {
    return (getMinCoord(d) * (numPartitions[d] - i) + getMaxCoord(d) * i) / numPartitions[d];
  }

  /**
   * Returns the higher coordinate of the buckets in the given dimension (exclusive)
   * @param i the index of the partition
   * @param d the dimension
   * @return the higher coordinate of the buckets at the i<sup>th</sup> partition along the d dimension
   */
  public double getPartitionMax(int i, int d) {
    return getPartitionMin(i + 1, d);
  }

  public UniformHistogram mergeNonAligned(UniformHistogram another){
    assert this.getCoordinateDimension() == another.getCoordinateDimension() :
        String.format("Mismatching number of dimensions %d != %d", this.getCoordinateDimension(), another.getCoordinateDimension());
    int numDimensions = this.getCoordinateDimension();
    double sourceBucketVolume = 1.0;
    for (int d = 0; d < numDimensions; d++)
      sourceBucketVolume *= another.getPartitionMax(0, d) - another.getPartitionMin(0, d);
    // The position of the source and target buckets initialized to {0}^d
    int[] i = new int[numDimensions]; // The position in this histogram
    int bucketIDI = 0;
    int[] j = new int[numDimensions]; // The position in the other histogram
    int bucketIDJ = 0;
    boolean finished = false;
    while (!finished) {
      // Update the values at buckets i, j
      double overlap = getOverlapVolume(this, i, another, j);
      assert another.getBucketID(j) == bucketIDJ : String.format("%d != %d", another.getBucketID(j), bucketIDJ);
      assert this.getBucketID(i) == bucketIDI : String.format("%d != %d", this.getBucketID(i), bucketIDI);
      this.values[bucketIDI] += (long) (another.values[bucketIDJ] * overlap / sourceBucketVolume);

      // Advance i and j
      // Dimension being advanced
      int d = 0;
      // The advance in bucket ID for each increment in dimension d
      int multiplierI = 1;
      int multiplierJ = 1;
      // A flag this is set when a dimension is reset. When this happens, the next dimension is incremented
      boolean dimensionReset;
      do {
        dimensionReset = false;
        if (this.getPartitionMax(i[d], d) < another.getPartitionMax(j[d], d)) {
          i[d]++;
          bucketIDI += multiplierI;
        } else {
          j[d]++;
          bucketIDJ += multiplierJ;
        }
        if (i[d] >= this.getNumPartitions(d) || j[d] >= another.getNumPartitions(d)) {
          // Reset this dimension and advance to the next dimension
          bucketIDI -= i[d] * multiplierI;
          bucketIDJ -= j[d] * multiplierJ;
          i[d] = j[d] = 0;
          multiplierI *= numPartitions[d];
          multiplierJ *= another.numPartitions[d];
          d++;
          dimensionReset = true;
        }
      } while (d < numDimensions && dimensionReset);
      finished = d == numDimensions;
    }
    return this;
  }

  /**
   * Tests if the histogram is perfectly aligned with another histogram, i.e., the same MBR and the same number of
   * rows and columns.
   * @param another the other histogram to test for alignment
   * @return {@code true} if the two histograms are aligned
   */
  public boolean isAligned(UniformHistogram another) {
    return super.equalsExact(another) && Arrays.equals(this.numPartitions, another.numPartitions);
  }

  /**
   * Computes the sum of all values in the given range of grid cells.
   * @param minPos the position of the lower corner in grid coordinates
   * @param sizes the size (number of cells) along each dimension
   * @return the value of the given range of bins
   */
  @Override
  public long getValue(int[] minPos, int[] sizes) {
    assert minPos.length == getCoordinateDimension();
    assert sizes.length == getCoordinateDimension();
    int[] pos = new int[minPos.length];
    int totalNumberOfCells = 1;
    boolean copyMade = false;
    for (int d = 0; d < minPos.length; d++) {
      if (minPos[d] < 0 || minPos[d] + sizes[d] > numPartitions[d]) {
        // Adjust minPos and sizes to account for the negative value
        if (!copyMade) {
          minPos = Arrays.copyOf(minPos, minPos.length);
          sizes = Arrays.copyOf(sizes, sizes.length);
          copyMade = true;
        }
        if (minPos[d] < 0) {
          sizes[d] += minPos[d]; // Reduce the size along this dimension
          minPos[d] = 0; // Reset the minimum position to zero along this dimension
        }
        if (minPos[d] + sizes[d] > numPartitions[d])
          sizes[d] = numPartitions[d] - minPos[d];
        if (sizes[d] <= 0)
          return 0;
      }
      totalNumberOfCells *= sizes[d];
    }
    long sum = 0;
    for (int i = 0; i < totalNumberOfCells; i++) {
      int d = minPos.length;
      int offset = 0;
      while (d-- > 0) {
        offset *= numPartitions[d];
        offset += minPos[d] + pos[d];
      }
      sum += values[offset];
      // Move to the next position
      d = 0;
      while (d < pos.length && ++pos[d] >= sizes[d])
        pos[d++] = 0;
    }
    return sum;
  }

  /**
   * Computes the sum of all entries in a given rectangle. It is assumed that x1 &le; x2 and y1 &le; y2
   * @param x1 the lower x-coordinate
   * @param y1 the lower y-coordinate
   * @param x2 the upper x-coordinate
   * @param y2 the upper y-coordinate
   * @return the sum of values in the given rectangle
   */
  public long sumRectangle(double x1, double y1, double x2, double y2) {
    int col1 = (int) Math.floor((x1 - this.getMinCoord(0)) * numPartitions[0] / this.getSideLength(0));
    int col2 = (int) Math.ceil((x2 - this.getMinCoord(0)) * numPartitions[0] / this.getSideLength(0));
    int row1 = (int) Math.floor((y1 - this.getMinCoord(1)) * numPartitions[1] / this.getSideLength(1));
    int row2 = (int) Math.ceil((y2 - this.getMinCoord(1)) * numPartitions[1] / this.getSideLength(1));
    // Compute the fraction in each direction that is outside the expanded region. These are the parts that need
    // to be subtracted. The variable naming assumes the coordinates increase from left to right and from top to bottom
    double fractionLeft = (x1 - this.getMinCoord(0)) / (this.getSideLength(0) / this.numPartitions[0]);
    fractionLeft -= Math.floor(fractionLeft);
    double fractionTop = (y1 - this.getMinCoord(1)) / (this.getSideLength(1) / this.numPartitions[1]);
    fractionTop -= Math.floor(fractionTop);
    double fractionRight = (this.getMaxCoord(0) - x2) / (this.getSideLength(0) / this.numPartitions[0]);
    fractionRight -= Math.floor(fractionRight);
    double fractionBottom = (this.getMaxCoord(1) - y2) / (this.getSideLength(1) / this.numPartitions[1]);
    fractionBottom -= Math.floor(fractionBottom);

    double expandedSum = getValue(new int[] {col1, row1}, new int[] {col2 - col1, row2 - row1});
    // Subtract the left column
    expandedSum -= fractionLeft * getValue(new int[] {col1, row1}, new int[] {1, row2 - row1});
    // Subtract the right column
    expandedSum -= fractionRight * getValue(new int[] {col2-1, row1}, new int[] {1, row2 - row1});
    // Subtract the top row
    expandedSum -= fractionTop * getValue(new int[] {col1, row1}, new int[] {col2 - col1, 1});
    // Subtract the bottom row
    expandedSum -= fractionBottom * getValue(new int[] {col1, row2-1}, new int[] {col2 - col1, 1});

    // Add back the top-left fraction that was subtracted twice
    expandedSum += fractionTop * fractionLeft * getValue(new int[] {col1, row1}, new int[] {1, 1});
    // Add back the bottom-left fraction that was subtracted twice
    expandedSum += fractionBottom * fractionLeft * getValue(new int[] {col1, row2-1}, new int[] {1, 1});
    // Add back the top-right fraction that was subtracted twice
    expandedSum += fractionTop * fractionRight * getValue(new int[] {col2-1, row1}, new int[] {1, 1});
    // Add back the bottom-right fraction that was subtracted twice
    expandedSum += fractionBottom * fractionRight * getValue(new int[] {col2-1, row2-1}, new int[] {1, 1});
    return Math.round(expandedSum);
  }

  public int getNumPartitions(int d) {
    return numPartitions[d];
  }

  /**
   * Increment a bin given its position in the array of bins
   * @param binID the ID of the bin to increment. Must be in the range [0, {@link #getNumBins()}[.
   * @param count the increment amount
   * @see #getBinID(double[])
   */
  public void incrementBin(int binID, long count) {
    values[binID] += count;
  }

  @Override
  public int getNumBins() {
    return values.length;
  }

  @Override
  public long getBinValue(int binID) {
    return values[binID];
  }

  /**
   * Reset the value of the given bin to a new value
   * @param binID the ID of the bin, must be in the range [0, {@link #getNumBins()}[
   * @param newValue the new value to set for the given bin
   */
  public void setBinValue(int binID, long newValue) {
    values[binID] = newValue;
  }
}