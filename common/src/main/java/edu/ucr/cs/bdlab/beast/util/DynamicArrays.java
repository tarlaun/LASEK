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

import java.util.Arrays;

/**
 * A set of helper methods to maintain dynamic arrays.
 */
public class DynamicArrays {

  /**
   * Expand the array to ensure it will hold at least {@code newSize}. If the array has at least {@code newSize}
   * entries, it will be returned as-is. If not, it will be expanded to {@code newSize*2} and all its existing entries
   * will be copied to the new array. If {@code array} is null, it is initialized to {@code newSize*2} and returned.
   * @param array the existing array to expand
   * @param newSize the new size of the array
   * @return either the same array object if not resized, or the new array if resized.
   */
  public static int[] expand(int[] array, int newSize) {
    if (array == null)
      array = new int[newSize];
    else if (array.length < newSize) {
      int[] newArray = new int[newSize];
      System.arraycopy(array, 0, newArray, 0, array.length);
      array = newArray;
    }
    return array;
  }

  /**
   * Expand the array to ensure it will hold at least {@code newSize}. If the array has at least {@code newSize}
   * entries, it will be returned as-is. If not, it will be expanded to {@code newSize*2} and all its existing entries
   * will be copied to the new array. If {@code array} is null, it is initialized to {@code newSize*2} and returned.
   * @param array the existing array to expand
   * @param newSize the new size of the array
   * @return either the same array object if not resized, or the new array if resized.
   */
  public static double[] expand(double[] array, int newSize) {
    if (array == null)
      array = new double[newSize];
    else if (array.length < newSize) {
      double[] newArray = new double[newSize];
      System.arraycopy(array, 0, newArray, 0, array.length);
      array = newArray;
    }
    return array;
  }

  /**
   * Expand the array to ensure it will hold at least {@code newSize}. If the array has at least {@code newSize}
   * entries, it will be returned as-is. If not, it will be expanded to {@code newSize*2} and all its existing entries
   * will be copied to the new array. If {@code array} is null, it is initialized to {@code newSize*2} and returned.
   * @param array the existing array to expand
   * @param newSize the new size of the array
   * @return either the same array object if not resized, or the new array if resized.
   */
  public static byte[] expand(byte[] array, int newSize) {
    if (array == null)
      array = new byte[newSize];
    else if (array.length < newSize) {
      byte[] newArray = new byte[newSize];
      System.arraycopy(array, 0, newArray, 0, array.length);
      array = newArray;
    }
    return array;
  }

  /**
   * Finds the index of the first occurrence of {@code value} in the {@code array} starting the search at the range
   * [{@code start}, {@code end}). If the given value is not value, {@code end} is returned.
   * @param array the array to search in
   * @param value the value to search for
   * @param start the beginning index (inclusive)
   * @param end the end index (exclusive)
   * @return the index of the first occurrence of {@code value} in the range [{@code start}, {@code end}), or the index
   * {@code end} if not value.
   */
  public static int indexOf(byte[] array, byte value, int start, int end) {
    int offset = start;
    while (offset < end && array[offset] != value)
      offset++;
    return offset;
  }

  public static String[] expand(String[] array, int newSize) {
    if (array == null)
      array = new String[newSize];
    else if (array.length < newSize) {
      String[] newArray = new String[newSize];
      System.arraycopy(array, 0, newArray, 0, array.length);
      array = newArray;
    }
    return array;
  }

  /**
   * Creates an inverted index for an integer array. For an input array {@code a}, the resulting array {@code x}
   * is defined as {@code x[i]=j <=> a[j]=i} and {@code x[i]=-1 <=> i &notin; a}
   * @param a the array
   * @return the inverted index of this array
   */
  public static int[] invertedIndex(int[] a) {
    int max = 0;
    for (int i : a)
      max = Math.max(max, i);
    int[] x = new int[max + 1];
    Arrays.fill(x, -1);
    for (int j = 0; j < a.length; j++) {
      int i = a[j];
      x[i] = j;
    }
    return x;
  }
}
