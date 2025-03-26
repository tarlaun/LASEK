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

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.util.LongArray;
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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A uniform grid histogram storing Long values.
 */
public class EulerHistogram2D extends AbstractHistogram implements Externalizable {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(EulerHistogram2D.class);

  /**Dimensions of the grid of the histogram*/
	protected int numColumns, numRows;

	/**Total values for ranges that have a lower corner in each cell*/
	protected long[] c1;
	/**Total values for ranges that have a lower horizontal edge overlapping each cell*/
	protected long[] c2;
  /**Total values for ranges that have a lower vertical edge overlapping each cell*/
  protected long[] c3;
  /**Total values for ranges that overlap each cell*/
  protected long[] c4;

	/**Default constructor is needed for deserialization*/
	public EulerHistogram2D() { }

	public EulerHistogram2D(EulerHistogram2D h) {
	  this.numColumns = h.numColumns;
	  this.numRows = h.numRows;
    this.numPartitions = new int[] {this.numColumns, this.numRows};
	  this.c1 = Arrays.copyOf(c1, c1.length);
	  this.c2 = Arrays.copyOf(c2, c2.length);
	  this.c3 = Arrays.copyOf(c3, c3.length);
	  this.c4 = Arrays.copyOf(c4, c4.length);
  }

	public EulerHistogram2D(EnvelopeNDLite mbr, int numCols, int numRows) {
		this.set(mbr);
    assert mbr.getCoordinateDimension() == 2;
    this.numColumns = numCols;
    this.numRows = numRows;
    this.numPartitions = new int[] {this.numColumns, this.numRows};
		c1 = new long[numCols * numRows];
		c2 = new long[numCols * numRows];
		c3 = new long[numCols * numRows];
		c4 = new long[numCols * numRows];
	}

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
	  GeometryHelper.writeIEnvelope(this, out);
    out.writeInt(numColumns);
    out.writeInt(numRows);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(baos));
    LongArray.writeLongArray(c1, dos);
    LongArray.writeLongArray(c2, dos);
    LongArray.writeLongArray(c3, dos);
    LongArray.writeLongArray(c4, dos);
    dos.close();

    byte[] serializedData = baos.toByteArray();
    out.writeInt(serializedData.length);
    out.write(serializedData);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(this, in);
    this.numColumns = in.readInt();
    this.numRows = in.readInt();
    this.numPartitions = new int[] {this.numColumns, this.numRows};
    int compressedDataLength = in.readInt();
    byte[] compressedData = new byte[compressedDataLength];
    in.readFully(compressedData);
    DataInputStream din = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(compressedData)));
    c1 = LongArray.readLongArray(c1, din);
    c2 = LongArray.readLongArray(c2, din);
    c3 = LongArray.readLongArray(c3, din);
    c4 = LongArray.readLongArray(c4, din);
    din.close();
  }

  /**
   * Adds a value that covers a range of cells.
   * @param col the first column that the range covers
   * @param row the first row that the range covers
   * @param width the number of column that the range covers
   * @param height the number of rows that the range covers
   * @param value that value to be added
   */
	public void addEntry(int col, int row, int width, int height, long value) {
	  int offset = row * numColumns + col;
	  // Increment (c1) for the cell at the corner
	  c1[offset] += value;
	  // Increment (c2) for the cells that overlap the top edge
	  for (int $c = 0; $c < width; $c++)
	    c2[offset + $c] += value;
	  // Increment (c3) for the cells that overlap the left edge
	  for (int $r = 0; $r < height; $r++)
	    c3[offset + $r * numColumns] += value;
	  // Increment (c4) for all cells that overlap the given range
    for (int $r = 0; $r < height; $r++) {
      for (int $c = 0; $c < width; $c++) {
        c4[offset + $r * numColumns + $c] += value;
      }
    }
	}

  /**
   * Adds the given value to the associated range in coordinates
   * @param x1 the lower x coordinate of the rectangle
   * @param y1 the lower y coordinate of the rectangle
   * @param x2 the upper x coordinate of the rectangle
   * @param y2 the upper y coordinate of the rectangle
   * @param value the value to assign to the given rectangle
   */
  public void addEnvelope(double x1, double y1, double x2, double y2, long value) {
    int col1 = (int) Math.floor((x1 - super.getMinCoord(0)) * numColumns / super.getSideLength(0));
    int col2 = (int) Math.ceil((x2 - super.getMinCoord(0)) * numColumns / super.getSideLength(0));
    int row1 = (int) Math.floor((y1 - super.getMinCoord(1)) * numRows / super.getSideLength(1));
    int row2 = (int) Math.ceil((y2 - super.getMinCoord(1)) * numRows / super.getSideLength(1));
    addEntry(col1, row1, col2 - col1, row2 - row1, value);
  }

	/**
	 * Merges with another histogram that is perfectly aligned with this histogram, i.e., the same MBR and the same
   * number of rows and columns
	 * @param another the other histogram to merge with
   * @return this histogram to call serially
	 */
	public EulerHistogram2D mergeAligned(EulerHistogram2D another) {
	  int length = c1.length;
		for (int i = 0; i < length; i++) {
      this.c1[i] += another.c1[i];
      this.c2[i] += another.c2[i];
      this.c3[i] += another.c3[i];
      this.c4[i] += another.c4[i];
    }
		return this;
	}

  @Override
  public long getValue(int[] minPos, int[] sizes) {
    return getValue(minPos[0], minPos[1], sizes[0], sizes[1]);
  }

  @Override
  public int getNumPartitions(int d) {
    return d == 0? numColumns : numRows;
  }

  @Override
  public long getBinValue(int binID) {
    return c4[binID];
  }

  /**
	 * Computes the sum of all values in the given range of grid cells.
   * @param col the first column that the range covers
   * @param row the first row that the range covers
   * @param width the number of column that the range covers
   * @param height the number of rows that the range covers
	 * @return the sum of values at the given rectangle
	 */
	public long getValue(int col, int row, int width, int height) {
	  long sum = 0;
	  int offset = row * numColumns + col;
	  // Count the ranges that overlap the top-left corner of the given range (col, row)
	  sum += c4[offset];
	  // Add the ranges that have a left edge in a cell along the top line
    for (int $c = 1; $c < width; $c++)
     sum += c3[offset + $c];
	  // Add the ranges that have a top edge in a cell along the left line
    for (int $r = 1; $r < height; $r++)
     sum += c2[offset + $r * numColumns];
   // Add all the ranges that have a top-left corner in all other overlapping cells
    for (int $r = 1; $r < height; $r++) {
      for (int $c = 1; $c < width; $c++) {
        sum += c1[offset + $r * numColumns + $c];
      }
    }
		return sum;
	}

  public long sumEnvelope(double x1, double y1, double x2, double y2) {
	  x1 = Math.max(x1, this.getMinCoord(0));
	  y1 = Math.max(y1, this.getMinCoord(1));
	  x2 = Math.min(x2, this.getMaxCoord(0));
	  y2 = Math.min(y2, this.getMaxCoord(1));
	  if (x1 >= x2 || y1 >= y2)
	    return 0;
    int col1 = (int) Math.floor((x1 - super.getMinCoord(0)) * numColumns / super.getSideLength(0));
    int col2 = (int) Math.ceil((x2 - super.getMinCoord(0)) * numColumns / super.getSideLength(0));
    int row1 = (int) Math.floor((y1 - super.getMinCoord(1)) * numRows / super.getSideLength(1));
    int row2 = (int) Math.ceil((y2 - super.getMinCoord(1)) * numRows / super.getSideLength(1));
    return getValue(col1, row1, col2 - col1, row2 - row1);
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumRows() {
    return numRows;
  }
}