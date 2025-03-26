/*
 * Copyright 2022 University of California, Riverside
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Similar to {@link java.io.ByteArrayOutputStream} but stores the values in a long array which allows it to grow
 * to greater sizes due to array length restrictions.
 */
public class LongArrayOutputStream extends OutputStream {
  /**The size of data in bytes*/
  protected long size;

  /**
   * The array that backs up the data. The data is ordered in little-endian. This means that the first data byte
   * is stored in the least significant eight bits of the first long entry.
   */
  protected long[] data = new long[16];

  /**Maximum capacity in bytes*/
  private static final long MaxCapacity = ((long)Integer.MAX_VALUE) >> 3;

  public LongArrayOutputStream() {
    ensureCapacity(48);
  }

  public LongArrayOutputStream(long initialCapacityInBytes) {
    ensureCapacity(initialCapacityInBytes);
  }

  protected void ensureCapacity(long capacityInBytes) {
    if (capacityInBytes > MaxCapacity)
      throw new OutOfMemoryError(String.format("Capacity %d is too big. Maximum allowed is %d.", capacityInBytes, MaxCapacity));
    if (capacityInBytes > ((long)data.length) << 3) {
      // Need to grow
      long newCapacity = Math.min(Long.highestOneBit(capacityInBytes) << 1, MaxCapacity);
      data = Arrays.copyOf(data, (int) (newCapacity >> 3));
    }
  }

  @Override
  public void write(int b) throws IOException {
    ensureCapacity(size + 1);
    int position = (int) (size >> 3);
    long shift = (size & 0x7) << 3;
    long bl = ((long) b & 0xff) << shift;
    data[position] |= bl;
    size++;
  }

  // TODO @Override public void write(@NotNull byte[] b, int off, int len) throws IOException { // For efficiency }

  /**
   * Get a copy of the data as a long array
   * @return a copy of the data (not the original array)
   */
  public long[] getData() {
    return Arrays.copyOf(data, (int) ((size + 7) >> 3));
  }
}
