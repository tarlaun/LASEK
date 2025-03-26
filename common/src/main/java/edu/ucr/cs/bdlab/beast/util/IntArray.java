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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Stores an expandable array of integers
 * @author Ahmed Eldawy
 *
 */
public class IntArray implements Externalizable, Iterable<Integer> {
  /**Stores all elements*/
  protected int[] array;
  /**Number of entries occupied in array*/
  protected int size;
  
  public IntArray() {
    this.array = new int[16];
  }

  public IntArray(int size) {
    this.array = new int[size];
    this.size = size;
  }

  public void add(int x) {
    append(x);
  }

  /**
   * Inserts a value at a given position in the array.
   * @param position the index in which the new value will be inserted
   * @param value the value to insert into the array
   */
  public void insert(int position, int value) {
    expand(1);
    System.arraycopy(array, position, array, position+1, size() - position);
    array[position] = value;
    size++;
  }

  public void append(int x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(int[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(int[] xs, int offset, int count, int delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(IntArray another) {
    append(another.array, 0, another.size);
  }

  public void append(IntArray another, int offset, int count) {
    append(another.array, offset, count);
  }

  public void append(IntArray another, int delta) {
    append(another.array, 0, another.size, delta);
  }
  
  public boolean contains(int value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        return true;
      }
    }
    return false;
  
  }
  
  /**
   * Ensures that the array can accept the additional entries
   * @param additionalSize the number of entries that wish to be added to the list
   */
  protected void expand(int additionalSize) {
    if (size + additionalSize > array.length) {
      int newCapacity = MathUtil.nextPowerOfTwo(size + additionalSize);
      int[] newArray = new int[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  public static void writeIntArray(int[] array, DataOutput out) throws IOException {
    writeIntArray(array, 0, array.length, out);
  }

  public static void writeIntArray(int[] array, int offset, int length, DataOutput out) throws IOException {
    out.writeInt(length);
    for (int i = 0; i < array.length; i++)
      out.writeInt(array[offset + i]);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Note: We cannot use writeIntArray because we need to write only the occupied entries of the array
    out.writeInt(size);
    for (int i = 0; i < size; i++)
      out.writeInt(array[i]);
  }

  public static int[] readIntArray(int[] array, DataInput in) throws IOException {
    int newSize = in.readInt();
    // Check if we need to allocate a new array
    if (array == null || newSize != array.length)
      array = new int[newSize];
    for (int $i = 0; $i < newSize; $i++)
      array[$i] = in.readInt();
    return array;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    int newSize = in.readInt();
    expand(newSize);
    for (int $i = 0; $i < newSize; $i++)
      array[$i] = in.readInt();
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
   * @return a reference to the underlying array
   */
  public int[] underlyingArray() {
    return array;
  }
  
  /**
   * Converts this IntArray into a native Java array that with a length equal
   * to {@link #size()}.
   * @return a new array that is identical to this list
   */
  public int[] toArray() {
    int[] compactArray = new int[size];
    System.arraycopy(array, 0, compactArray, 0, size);
    return compactArray;
  }
  
  public void sort() {
    Arrays.sort(array, 0, size);
  }

  public void insertionSort(Comparator<Integer> c) {
    for (int sortSize = 2; sortSize <= size; sortSize++) {
      int pivot = array[sortSize-1];
      int j = sortSize - 2;
      while (j >= 0 && c.compare(array[j], pivot) > 0) {
        array[j + 1] = array[j];
        j--;
      }
      array[j+1] = pivot;
    }
  }

  public int get(int index) {
    return array[index];
  }

  /**
   * Removes and returns the last element in the array
   * @return the last element in the array after removing it.
   */
  public int pop() {
    return array[--size];
  }

  /**
   * Returns the last element in the array without removing it.
   * @return the last element in the array
   */
  public int peek() {
    return array[size-1];
  }

  public boolean remove(int value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        System.arraycopy(array, i + 1, array, i, size - (i + 1));
        size--;
        return true;
      }
    }
    return false;
  }

  public IntArray clone() {
    IntArray newIntArray = new IntArray();
    newIntArray.size = this.size;
    newIntArray.array = this.array.clone();
    return newIntArray;
  }

  /**
   * Remove all elements in the array.
   */
  public void clear() {
    size = 0;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new IntIterator();
  }

  /**
   * Shrinks the array to contain the given number of elements only
   * @param newSize the new size of this array
   */
  public void resize(int newSize) {
    if (newSize > this.size)
      throw new IllegalArgumentException("The new size cannot be greater than the current size");
    this.size = newSize;
  }

  public void set(int index, int value) {
    if (index >= size)
      throw new ArrayIndexOutOfBoundsException(index);
    array[index] = value;
  }

  class IntIterator implements Iterator<Integer> {
    int i = -1;

    @Override
    public boolean hasNext() {
      return i < size() - 1;
    }

    @Override
    public Integer next() {
      return array[++i];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not yet supported");
    }
    
  }

  public void swap(int i, int j) {
    int t = array[i];
    array[i] = array[j];
    array[j] = t;
  }

  public static String join(char separator, int ... values) {
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

  public static int[] split(String str, char separator) {
    if (str == null || str.length() == 0)
      return new int[0];
    String[] parts = str.split(Character.toString(separator));
    int[] x = new int[parts.length];
    for (int $i = 0; $i < parts.length; $i++)
      x[$i] = Integer.parseInt(parts[$i]);
    return x;
  }
}
