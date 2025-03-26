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

import edu.ucr.cs.bdlab.test.JavaSparkTest;
import junit.framework.TestCase;

import java.util.Random;

public class MathUtilTest extends JavaSparkTest {

  public void testNumSignificantBits() {
    assertEquals(5, MathUtil.numberOfSignificantBits(31));
    assertEquals(24, MathUtil.numberOfSignificantBits(16000000));
    assertEquals(1, MathUtil.numberOfSignificantBits(0));
  }

  public void testGetBits() {
    byte[] data = {0x63, (byte) 0x94, 0x5f, 0x0C};
    assertEquals(0x39, MathUtil.getBits(data, 4, 8));
    assertEquals(0x394, MathUtil.getBits(data, 6, 10));
    assertEquals(0xA2F, MathUtil.getBits(data, 11, 12));
    assertEquals(0x63945F0C, MathUtil.getBits(data, 0, 32));
  }

  public void testGetBitsLong() {
    long[] data = {0x63945f0c00000000L};
    assertEquals(0x39, MathUtil.getBits(data, 4, 8));
    assertEquals(0x394, MathUtil.getBits(data, 6, 10));
    assertEquals(0xA2F, MathUtil.getBits(data, 11, 12));
    assertEquals(0x63945F0C, MathUtil.getBits(data, 0, 32));

    data = new long[] {0x0, 0x63945f0c00000000L};
    assertEquals(0x0063, MathUtil.getBits(data, 60, 12));
    data = new long[] {0xf0000, 0x63945f0c00000000L};
    assertEquals(0x0063, MathUtil.getBits(data, 60, 12));
  }

  public void testSetBitsLong() {
    long[] data = new long[1];
    MathUtil.setBits(data, 4, 8, 0x39);
    MathUtil.setBits(data, 6, 10, 0x394);
    MathUtil.setBits(data, 0, 32, 0x63945F0C);
    assertEquals(0x63945f0c00000000L, data[0]);

    data = new long[2];
    MathUtil.setBits(data, 40, 32, 0x12345678);
    assertEquals(0x123456L, data[0]);
    assertEquals(0x7800000000000000L, data[1]);

    // Set the last bits in the entire array
    data = new long[3];
    MathUtil.setBits(data, 3 * 64 - 32, 32, 0x12345678);
    assertEquals(0x12345678, data[2]);
  }

  public void testBytesToLongs() {
    byte[] data = {0x63, (byte) 0x94, 0x5f, 0x0C};
    long[] expectedLongs = {0x63945f0c00000000L};
    assertArrayEquals(expectedLongs, MathUtil.byteArrayToLongArray(data));
    data = new byte[] {0, 0, 0, 0, 0, 0, 0x63, (byte) 0x94, 0x5f, 0x0C};
    expectedLongs = new long[] {0x6394, 0x5f0c000000000000L};
    assertArrayEquals(expectedLongs, MathUtil.byteArrayToLongArray(data));
  }

  public void testSetBits() {
    byte[] data = {0, 0, 0, 0};
    MathUtil.setBits(data, 8, 4, 0x9);
    assertEquals((byte) 0x90, data[1]);
    MathUtil.setBits(data, 4, 8, 0x39);
    assertEquals((byte) 0x03, data[0]);
    assertEquals((byte) 0x90, data[1]);
    MathUtil.setBits(data, 17, 7, 0x5f);
    assertEquals((byte) 0x5f, data[2]);
    MathUtil.setBits(data, 28, 2, 0x3);
    assertEquals((byte) 0x0c, data[3]);
    MathUtil.setBits(data, 0, 32, 0x63945F0C);
    assertEquals(0x63945F0C, MathUtil.getBits(data, 0, 32));
    MathUtil.setBits(data, 4, 8, 0);
    assertEquals(0x60045F0C, MathUtil.getBits(data, 0, 32));
  }

  public void testLog2() {
    assertEquals(-1, MathUtil.log2(0));
    assertEquals(0, MathUtil.log2(1));
    assertEquals(1, MathUtil.log2(2));
    assertEquals(1, MathUtil.log2(3));
    assertEquals(2, MathUtil.log2(4));
    assertEquals(6, MathUtil.log2(64));
    assertEquals(5, MathUtil.log2(63));
  }

  public void testNumDecimalDigits() {
    assertEquals(1, MathUtil.getNumDecimalDigits(0));
    assertEquals(1, MathUtil.getNumDecimalDigits(9));
    assertEquals(2, MathUtil.getNumDecimalDigits(10));
    assertEquals(6, MathUtil.getNumDecimalDigits(569875));
    assertEquals(15, MathUtil.getNumDecimalDigits(100000000000000L));
    assertEquals(15, MathUtil.getNumDecimalDigits(-100000000000000L));
    assertEquals(18, MathUtil.getNumDecimalDigits(279624296851435688L));
  }

  public void testNumFractionDigits() {
    assertEquals(0, MathUtil.getNumFractionDigits(0.0));
    assertEquals(1, MathUtil.getNumFractionDigits(15.2));
    assertEquals(5, MathUtil.getNumFractionDigits(-0.13658));
    assertEquals(12, MathUtil.getNumFractionDigits(0.000000000003));
  }
}