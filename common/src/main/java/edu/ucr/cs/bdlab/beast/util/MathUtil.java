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

public class MathUtil {

  /**
   * Computes the number of significant bits in the given integer. This also represents the minimum number of bits
   * needed to represent this number.
   * Special case, if {@code x} is 0, the return value is 1.
   * @param x any integer
   * @return number of significant bits
   */
  public static int numberOfSignificantBits(int x) {
    if (x == 0)
      return 1;
    int numBits = 0;
    if ((x & 0xffff0000) != 0) {
      numBits += 16;
      x >>>= 16;
    }
    if ((x & 0xff00) != 0) {
      numBits += 8;
      x >>>= 8;
    }
    if ((x & 0xf0) != 0) {
      numBits += 4;
      x >>>= 4;
    }
    if ((x & 0xc) != 0) {
      numBits += 2;
      x >>>= 2;
    }
    if ((x & 0x2) != 0) {
      numBits += 1;
      x >>>= 1;
    }
    if (x != 0)
      numBits++;
    return numBits;
  }


  /**
   * Returns the number of decimal digits in the given number. If the input is zero, it returns 1.
   * Otherwise, it returns the smallest number of digits that can represent the given number.
   * The negative sign is not counted.
   * @return the minimum number of decimal digits to represent the given number.
   */
  public static short getNumDecimalDigits(long x) {
    if (x < 0)
      x = -x;
    short numDecimalDigits = 1;
    // Largest 64-bit number can be represented in at most 19 digits
    // Initially, there could be at most 19 digits
    if (x >= 10000000000L) {
      numDecimalDigits += 10;
      x /= 10000000000L;
    }
    // At this point, there could be at most 10 digits
    if (x >= 100000) {
      numDecimalDigits += 5;
      x /= 100000;
    }
    // At this point, there could be at most 5 digits
    if (x >= 1000) {
      numDecimalDigits += 3;
      x /= 1000;
    }
    // At this point, there could be at most 3 digits
    if (x >= 100) {
      numDecimalDigits+=2;
      x /= 100;
    }
    // At this point, there could be at most 2 digits
    if (x >= 10)
      numDecimalDigits++;
    return numDecimalDigits;
  }

  /**
   * Returns the minimum number of fraction digits in the given number. It only counts the number of digits
   * right to the decimal point. If the input is zero, it returns zero. The negative symbol is not counted.
   * @param x the number to count its fraction decimal digits.
   * @return the number of significant decimal digits after the decimal point.
   */
  public static short getNumFractionDigits(double x) {
    String str = String.format("%.19f", x);
    // Remove trailing zeros
    int lastNonZero = str.length() - 1;
    while (lastNonZero > 0 && str.charAt(lastNonZero) == '0')
      lastNonZero--;
    int decimalPoint = str.indexOf('.');
    if (decimalPoint == -1)
      return 0;
    return (short) (lastNonZero - decimalPoint);
  }

  /**
   * Retrieves the specified number of bits from the given array of bytes.
   * @param data the sequence of all bytes
   * @param position the bit position to start retrieving
   * @param length the number of bits to retrieve
   * @return the retrieved bits stored in an integer
   */
  public static long getBits(byte[] data, int position, int length) {
    long value = 0;
    int start = position >> 3;
    int end = (position + length - 1) >> 3;
    // Concatenate all the bytes fully from start to end into a single 64-bit long value
    while (start <= end)
      value = (value << 8) | (data[start++] & 0xff);
    // Shift right to adjust the least significant bit to the first position
    value = value >>> (7 - (position + length - 1) & 0x7);
    // Keep only the least significant bits of the value as desired
    value &= KeepLSB[length];
    return value;
  }

  static final long[] KeepLSB = new long[65];
  static {
    for (int length = 0; length <= 64; length++) {
      KeepLSB[length] = 0xffffffffffffffffL >>> (64 - length);
    }
  }

  /**
   * Return a range of bits from the given list of long values.
   * The range is specified as if all the longs have been concatenated in one long sequence such that from left
   * to right. In other words, the bit at position 0 is the most significant bit of the first long value.
   * @param data the array of long values.
   * @param position the bit position to start retrieving the data at
   * @param length the number of bits to retrieve
   * @return the retrieved bits stored in a long integer
   */
  public static long getBits(long[] data, long position, int length) {
    long value;
    int start = (int) (position / 64);
    int end = (int) ((position + length - 1) / 64);
    if (start == end) {
      // The entire value is in one long
      value = data[start];
      value = value >>> (63 - (position + length - 1) % 64);
    } else {
      assert start == end - 1 : String.format("Retrieved data too long [%d, %d]", position, position + length - 1);
      // The value spans two long values
      value = data[start] << ((position + length) % 64);
      value |= data[end] >>> (64 - (position + length) % 64);
    }
    value &= (0xffffffffffffffffL >>> (64 - length));
    return value;
  }

  public static long[] byteArrayToLongArray(byte[] data) {
    // Combine every eight bytes into a long
    long[] longs = new long[(data.length + 7) / 8];
    for (int i = 0; i < data.length; i++) {
      longs[i / 8] |= (data[i] & 0xffL) << (64 - (i % 8 + 1) * 8);
    }
    return longs;
  }

  /**
   * Sets the given range of positions to the lowest bits of the given value.
   * @param data the array of bytes to modify
   * @param position the first bit to change starts with position 0 in the array and treating the most significant
   *                 bit as the first position in each byte
   * @param length total number of bits to change and to take from the given value
   * @param value the lowest significant bits of the given value will be used to set bits in the data array
   */
  public static void setBits(byte[] data, int position, int length, int value) {
    // Mask left marks the bits that will change on the lowest byte position
    byte maskLeft = (byte) (0xff >> (position % 8));
    // Mask right marks the bits that will change on the highest byte position
    int lastBitExclusive = position + length;
    byte maskRight = (byte) (0xff00 >> (lastBitExclusive % 8));
    int firstByteToChange = position / 8;
    int lastByteToChange = lastBitExclusive / 8;
    if (firstByteToChange == lastByteToChange) {
      // Special case of partially changing one byte
      int mask = maskLeft & maskRight;
      data[firstByteToChange] &= ~mask;
      int lshift = 7 - (lastBitExclusive - 1) % 8;
      data[firstByteToChange] |= (value << lshift) & mask;
      return;
    }
    // General case where we change more than one byte
    // Change the left-most byte
    int rshift = length - (8 - position % 8);
    if (maskLeft != 0xff) {
      data[firstByteToChange] &= ~maskLeft;
      data[firstByteToChange] |= (value >> rshift) & maskLeft;
      firstByteToChange++;
      rshift -= 8;
    }
    // Change all bytes in between (completely changed)
    while (firstByteToChange < lastByteToChange) {
      data[firstByteToChange] = (byte) (value >> rshift);
      firstByteToChange++;
      rshift -= 8;
    }
    // Change the right most byte
    if (maskRight != 0) {
      data[lastByteToChange] &= ~maskRight;
      int lshift = 7 - (lastBitExclusive - 1) % 8;
      data[lastByteToChange] |= (value << lshift) & maskRight;
    }
  }

  /**
   * Sets the given range of positions to the lowest bits of the given value.
   * @param data the array of longs to modify
   * @param position the first bit to change starts with position 0 in the array and treating the most significant
   *                 bit as the first position in each byte
   * @param length total number of bits to change and to take from the given value
   * @param value the lowest significant bits of the given value will be used to set bits in the data array
   */
  public static void setBits(long[] data, long position, int length, long value) {
    // Remove any extra bits. Useful for negative numbers
    value &= 0xffffffffffffffffL >>> (64 - length);
    // This function can change one or two values in the data array
    int firstEntry = (int) (position / 64);
    int lastEntry = (int) ((position + length - 1) / 64);
    if (firstEntry == lastEntry) {
      // Change one entry
      int leftGap = (int) (position % 64);
      int rightGap = 64 - length - leftGap;
      long mask = (0xffffffffffffffffL << (64 - length)) >>> leftGap;
      data[firstEntry] = (data[firstEntry] & ~mask) | (value << rightGap);
    } else {
      // Change two entries
      int nBits1 = (int) ((64 - position % 64) % 64);
      int nBits2 = length - nBits1;
      long mask1 = 0xffffffffffffffffL << nBits1;
      data[firstEntry] = (data[firstEntry] & mask1) | (value >>> nBits2 & ~mask1);
      long mask2 = 0xffffffffffffffffL >>> nBits2;
      data[lastEntry] = (data[lastEntry] & mask2) | (value << (64 - nBits2));
    }
  }

  public static int nextPowerOfTwo(int i) {
    return Integer.highestOneBit(i) << 1;
  }

  /**
   * Integer floor of base to the log 2. For the special case when the input is zero, this function returns -1.
   * This function is not defined for negative values.
   * @param i the value to compute
   * @return &lfloor; log<sub>2</sub>(i)&rfloor;
   */
  public static int log2(int i) {
    return 32 - Integer.numberOfLeadingZeros(i) - 1;
  }

  /**
   * Long integer floor of base to the log 2. For the special case when the input is zero, this function returns -1.
   * This function is not defined for negative values.
   * @param i the value to compute
   * @return &lfloor; log<sub>2</sub>(i)&rfloor;
   */
  public static int log2(long i) {
    return 64 - Long.numberOfLeadingZeros(i) - 1;
  }
}
