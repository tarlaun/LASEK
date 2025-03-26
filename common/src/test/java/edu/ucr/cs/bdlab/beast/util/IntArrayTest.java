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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Unit test for the utility class {@link IntArray}.
 */
public class IntArrayTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName
   *          name of the test case
   */
  public IntArrayTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(IntArrayTest.class);
  }

  public void testRandomInsert() {
    IntArray array = new IntArray();
    array.add(5);
    array.insert(0, 3);
    assertEquals(2, array.size());
    assertEquals(3, array.get(0));
    assertEquals(5, array.get(1));
  }

  public void testSetFunction() {
    IntArray array = new IntArray();
    array.add(5);
    assertEquals(5, array.get(0));
    array.set(0, 15);
    assertEquals(15, array.get(0));
    assertEquals(1, array.size);
  }

  public void testInsertionSort() {
    IntArray array = new IntArray();
    array.add(5);
    array.add(3);
    array.add(1);
    array.add(10);
    array.insertionSort(new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1 - o2;
      }
    });

    assertTrue("Array not sorted", Arrays.equals(new int[] {1,3,5,10}, array.toArray()));
  }

  public void testInsertionSortTwoElements() {
    IntArray array = new IntArray();
    array.add(5);
    array.add(3);
    array.insertionSort(new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1 - o2;
      }
    });

    assertTrue("Array not sorted", Arrays.equals(new int[] {3,5}, array.toArray()));
  }
}
