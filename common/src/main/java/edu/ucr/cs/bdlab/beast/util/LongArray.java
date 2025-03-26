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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Stores an expandable array of long integers
 * @author Ahmed Eldawy
 *
 */
public class LongArray implements Externalizable, Iterable<Long> {
  /**Stores all elements*/
  protected long[] array;
  /**Number of entries occupied in array*/
  protected int size;

  public LongArray() {
    this.array = new long[16];
  }

  public LongArray(int size) {
    this.array = new long[size];
    this.size = size;
  }

  public void add(long x) {
    append(x);
  }

  /**
   * Inserts a value at a given position in the array.
   * @param position the index in which the new value will be inserted
   * @param value the value to insert into the array
   */
  public void insert(int position, long value) {
    expand(1);
    System.arraycopy(array, position, array, position+1, size() - position);
    array[position] = value;
    size++;
  }

  public void append(long x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(long[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(long[] xs, int offset, int count, int delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(LongArray another) {
    append(another.array, 0, another.size);
  }

  public void append(LongArray another, int offset, int count) {
    append(another.array, offset, count);
  }

  public void append(LongArray another, int delta) {
    append(another.array, 0, another.size, delta);
  }
  
  public boolean contains(long value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        return true;
      }
    }
    return false;
  
  }
  
  /**
   * Ensures that the array can accept the additional entries
   * @param additionalSize number of additional elements that wish to be added to the array
   */
  protected void expand(int additionalSize) {
    if (size + additionalSize > array.length) {
      int newCapacity = MathUtil.nextPowerOfTwo(size + additionalSize);
      long[] newArray = new long[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  public static void writeLongArray(long[] array, DataOutput out) throws IOException {
    out.writeInt(array.length);
    for (int i = 0; i < array.length; i++)
      out.writeLong(array[i]);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(size);
    for (int i = 0; i < array.length; i++)
      out.writeLong(array[i]);
  }

  public static long[] readLongArray(long[] array, DataInput in) throws IOException {
    int newSize = in.readInt();
    // Check if we need to allocate a new array
    if (array == null || newSize != array.length)
      array = new long[newSize];
    for (int $i = 0; $i < newSize; $i++)
      array[$i] = in.readLong();
    return array;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int newSize = in.readInt();
    expand(newSize);
    for (int $i = 0; $i < newSize; $i++)
      array[$i] = in.readLong();
    size = newSize;
  }

  public int size() {
    return size;
  }
  
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the underlying array. The returned array might have a length that
   * is larger than {@link #size()}. The values of those additional slots are
   * undefined and should not be used.
   * @return the internal array used by this LongArray (not a copy).
   */
  public long[] underlyingArray() {
    return array;
  }
  
  /**
   * Converts this LongArray into a native Java array that with a length equal to {@link #size()}.
   * @return a new array with elements
   */
  public long[] toArray() {
    long[] compactArray = new long[size];
    System.arraycopy(array, 0, compactArray, 0, size);
    return compactArray;
  }

  public long get(int index) {
    return array[index];
  }

  /**
   * Removes and returns the last element in the array
   * @return the last value in the array
   */
  public long pop() {
    return array[--size];
  }

  /**
   * Returns the last element in the array without removing it.
   * @return return the last in the array
   */
  public long peek() {
    return array[size-1];
  }

  public boolean remove(long value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        System.arraycopy(array, i + 1, array, i, size - (i + 1));
        size--;
        return true;
      }
    }
    return false;
  }

  public LongArray clone() {
    LongArray newLongArray = new LongArray();
    newLongArray.size = this.size;
    newLongArray.array = this.array.clone();
    return newLongArray;
  }

  /**
   * Remove all elements in the array.
   */
  public void clear() {
    size = 0;
  }

  @Override
  public Iterator<Long> iterator() {
    return new LongIterator();
  }

  /**
   * Shrinks the array to contain the given number of elements only
   * @param newSize the new size of the array
   */
  public void resize(int newSize) {
    if (newSize > this.size)
      throw new IllegalArgumentException("The new size cannot be greater than the current size");
    this.size = newSize;
  }

  public void set(int index, long value) {
    if (index >= size)
      throw new ArrayIndexOutOfBoundsException(index);
    array[index] = value;
  }

  class LongIterator implements Iterator<Long> {
    int i = -1;

    @Override
    public boolean hasNext() {
      return i < size() - 1;
    }

    @Override
    public Long next() {
      return array[++i];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not yet supported");
    }
    
  }

  public void swap(int i, int j) {
    long t = array[i];
    array[i] = array[j];
    array[j] = t;
  }

  public static String join(char separator, long ... values) {
    if (values == null)
      return "null";
    StringBuffer b = new StringBuffer();
    for (int $i = 0; $i < values.length; $i++) {
      if ($i > 0)
        b.append(separator);
      b.append(values[$i]);
    }
    return b.toString();
  }

  public static long[] split(String str, char separator) {
    if (str == null || str.length() == 0)
      return new long[0];
    String[] parts = str.split(Character.toString(separator));
    long[] x = new long[parts.length];
    for (int $i = 0; $i < parts.length; $i++)
      x[$i] = Long.parseLong(parts[$i]);
    return x;
  }
}
