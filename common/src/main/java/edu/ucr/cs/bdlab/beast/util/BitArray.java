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
package edu.ucr.cs.bdlab.beast.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * An array of bits which is stored efficiently in memory and can be serialized
 * or deserialized using Hadoop serialization framework.
 * @author Ahmed Eldawy
 *
 */
public class BitArray implements Writable, Serializable {

  /**Number of bits per entry*/
  private static final int BitsPerEntry = 64;
  
  /**Condensed representation of all data*/
  public long[] entries;
  
  /**Total number of bits stores in this array*/
  protected long size;

  /**Default constructor is needed for deserialization*/
  public BitArray() {
  }
  
  /**
   * Initializes a bit array with the given capacity in bits.
   * @param size Total number of bits in the array. All initialized to false.
   */
  public BitArray(long size) {
    this.size = size;
    entries = new long[(int) ((size + BitsPerEntry - 1) / BitsPerEntry)];
  }

  /**
   * Copy constructor
   * @param copy another bit array
   */
  public BitArray(BitArray copy) {
    this.entries = Arrays.copyOf(copy.entries, copy.entries.length);
    this.size = copy.size;
  }

  public void clear() {
    for (int i = 0; i < this.entries.length; i++)
      this.entries[i] = 0;
  }

  @Override
  public BitArray clone() {
    BitArray replica = new BitArray();
    replica.size = this.size;
    replica.entries = this.entries.clone();
    return replica;
  }

  /**
   * Sets the bit at position <code>i</code>
   * @param i the index of the bit (0-based)
   * @param b the value of the bit to set (true/false)
   */
  public void set(long i, boolean b) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    if (b) {
      entries[entry] |= (1L << offset);
    } else {
      entries[entry] &= ~(1L << offset);
    }
  }

  /**
   * Resize the array to have at least the given new size without losing the current data
   * @param newSize the newSize of the array in terms of number of bits.
   */
  public void resize(long newSize) {
    if (newSize > size) {
      // Resize needed
      int newArraySize = (int) ((newSize + BitsPerEntry - 1) / BitsPerEntry);
      if (newArraySize > entries.length) {
        long[] newEntries = new long[newArraySize];
        System.arraycopy(entries, 0, newEntries, 0, entries.length);
        entries = newEntries;
      }
    }
    size = newSize;
  }
  
  /**
   * Returns the boolean at position <code>i</code>
   * @param i the position of the bit to retrieve (0-based)
   * @return the current value of the bit (true/false)
   */
  public boolean get(long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    return (entries[entry] & (1L << offset)) != 0;
  }

  public static boolean isBitSet(byte[] entries, long i) {
    int entry = (int) (i >> 3);
    int offset = (int) (i & 7);
    return (entries[entry] & (1 << offset)) != 0;
  }

  public static void setBit(byte[] entries, long i) {
    int entry = (int) (i >> 3);
    int offset = (int) (i & 7);
    entries[entry] |= 1 << offset;
  }

  public static boolean isBitSet(long[] entries, long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    return (entries[entry] & (1L << offset)) != 0;
  }

  public static void setBit(long[] entries, long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    entries[entry] |= (1L << offset);
  }

  public static void unsetBit(long[] entries, long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    entries[entry] &= ~(1L << offset);
  }

  /**
   * Count number of set bits in the bit array.
   * Code adapted from
   * https://codingforspeed.com/a-faster-approach-to-count-set-bits-in-a-32-bit-integer/
   * @return the number of bits that are set to 1 in the entire bit array
   */
  public long countOnes() {
    long totalCount = 0;
    for (long i : entries) {
      i = i - ((i >>> 1) & 0x5555555555555555L);
      i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
      i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
      i = i + (i >>> 8);
      i = i + (i >>> 16);
      i = i + (i >>> 32);
      totalCount += i & 0x7f;
    }
    return totalCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(size);
    int numEntriesToWrite = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    for (int $i = 0; $i < numEntriesToWrite; $i++)
      out.writeLong(entries[$i]);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    size = in.readLong();
    int numEntriesToRead = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    if (entries == null || entries.length < numEntriesToRead)
      entries = new long[numEntriesToRead];
    for (int $i = 0; $i < numEntriesToRead; $i++)
      entries[$i] = in.readLong();
  }

  /**
   * Write only bit values (without the size) in the minimal number of bytes
   * @param out the output to write to
   * @throws IOException if an error happens while writing
   */
  public void writeBitsMinimal(DataOutput out) throws IOException {
    int numBytes = (int) ((size + 7) / 8);
    // First, write full 64-bit longs
    int iEntry = 0;
    while (numBytes >= 8) {
      out.writeLong(entries[iEntry++]);
      numBytes -= 8;
    }
    if (numBytes > 0) {
      // Write last entry byte by byte
      long lastEntry = entries[iEntry];
      while (numBytes > 0) {
        out.write((int) (lastEntry & 0xff));
        lastEntry >>>= 8;
        numBytes--;
      }
    }
  }

  public void readBitsMinimal(DataInput in) throws IOException {
    int numBytes = (int) ((size + 7) / 8);
    // First, read full 64-bit longs
    int iEntry = 0;
    while (numBytes >= 8) {
      entries[iEntry++] = in.readLong();
      numBytes -= 8;
    }
    if (numBytes > 0) {
      // Read last entry byte by byte
      long lastEntry = 0;
      int shift = 0;
      while (numBytes > 0) {
        long b = in.readByte() & 0xff;
        lastEntry |= (b << (shift));
        shift += 8;
        numBytes--;
      }
      entries[iEntry] = lastEntry;
    }
  }

  /**
   * The size of the bit array in terms of number of bits
   * @return the size of the array
   */
  public long size() {
    return size;
  }

  /**
   * Fill the entire bit array with the given value (0 or 1)
   * @param b the boolean value to fill int he entire bit array
   */
  public void fill(boolean b) {
    long fillValue = b ? 0xffffffffffffffffL : 0;
    for (int i = 0; i < entries.length; i++)
      entries[i] = fillValue;
  }

  /**
   * Inverts each bit from 0 to 1 and vice-verse
   * @return a new bit array that is the inverse of this bit array
   */
  public BitArray invert() {
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size();
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = ~this.entries[i];
    return result;
  }

  /**
   * Computes the bitwise OR of two bitmasks of the same size
   * @param other the other array to compute the logical OR with
   * @return the new array that results from ORing this bit array with the given one.
   */
  public BitArray or(BitArray other) {
    if (this.size != other.size)
      throw new RuntimeException("Cannot OR two BitArrays of different sizes");
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size;
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = this.entries[i] | other.entries[i];
    return result;
  }

  /**
   * ORs this bit array with the given one
   * @param other the other bit array to compute thr OR with
   */
  public void inplaceOr(BitArray other) {
    if (this.size != other.size)
      throw new RuntimeException("Cannot OR two BitArrays of different sizes");
    for (int i = 0; i < entries.length; i++)
      this.entries[i] |= other.entries[i];
  }

  /**
   * Sets the given range to the same value
   * @param start the first offset of set (inclusive)
   * @param end the end of the range (exclusive)
   * @param value the value to set in the given range
   */
  public void setRange(int start, int end, boolean value) {
    // TODO make it more efficient for long ranges by setting all values in the same entry together
    while (start < end)
      set(start++, value);
  }

  /**
   * Computes a logical OR between a specific range in this bit array and another bit array.
   * @param destinationOffset the first bit to modify in this bit array
   * @param other the other bit array to OR with
   * @param sourceOffset the first bit to read from the other bit array
   * @param width the number of bits to OR
   */
  public void inplaceOr(long destinationOffset, BitArray other, long sourceOffset, int width) {
    // TODO make it more efficient for long ranges by ORing long entries rather than bit-by-bit
    while (width-- > 0) {
      this.set(destinationOffset + width, other.get(sourceOffset + width));
    }
  }
}
