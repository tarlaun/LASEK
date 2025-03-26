/*
 * Copyright 2021 University of California, Riverside
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

public final class BitInputStream {

  private final byte[] data;

  /**The position of the next byte to read from teh byte array*/
  private int byteOffsetInArray;

  /**A copy of the eight bytes currently being inspected*/
  private long currentLong;

  /**The number of remaining bits (least significant) in the current long*/
  private int remainingBitsInCurrentLong;

  public BitInputStream(byte[] data) {
    this.data = data;
  }

  public final short nextShort(int desiredNumBits) {
    while (desiredNumBits > remainingBitsInCurrentLong) {
      currentLong = (currentLong << 8) | (data[byteOffsetInArray++] & 0xffL);
      remainingBitsInCurrentLong += 8;
    }
    short value = (short) (currentLong >>> (remainingBitsInCurrentLong -= desiredNumBits));
    return (short) (value& (0xffff >> (16 - desiredNumBits)));
  }
}
