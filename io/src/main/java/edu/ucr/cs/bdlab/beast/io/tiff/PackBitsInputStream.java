/*
 * Copyright 2023 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.tiff;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

/**
 * Applies PackBits decoding as explained in Tiff6 documentation (Section 9).
 *
 * A pseudo code fragment to unpack might look like this:
 * <ul>
 * <li>Loop until you get the number of unpacked bytes you are expecting:</li>
 * <li>Read the next source byte into n.</li>
 * <li>If n is between 0 and 127 inclusive, copy the next n+1 bytes literally.</li>
 * <li>Else if n is between -127 and -1 inclusive, copy the next byte -n+1 times.</li>
 * <li>Else if n is -128, noop.</li>
 * <li>Endloop</li>
 * </ul>
 */
public class PackBitsInputStream extends InputStream implements AutoCloseable {

  /** The index of the next byte to read from the compressed data */
  private int i = 0;

  /** The underlying compressed data */
  private final byte[] compressedData;

  /** The count value that was read from the input */
  protected int count = 0;

  /** Flag that we are in the repeat mode */
  protected boolean repeat;

  public PackBitsInputStream(byte[] data) {
    this.compressedData = data;
  }

  @Override
  public int read() throws IOException {
    if (i >= compressedData.length)
      return -1;
    while (count == 0) {
      // Read next value
      byte next = compressedData[i++];
      if (next >= 0) {
        // If n is between 0 and 127 inclusive, copy the next n+1 bytes literally.
        count = next + 1;
        repeat = false;
      } else if (next > -128) {
        // if n is between -127 and -1 inclusive, copy the next byte -n+1
        count = -next + 1;
        repeat = true;
      } else {
        // if n is -128, noop.
      }
    }
    count--;
    if (repeat) {
      byte value = compressedData[i];
      if (count == 0)
        i++;
      return value;
    } else {
      return compressedData[i++];
    }
  }

  @Override
  public int read(@NotNull byte[] b, int off, int len) throws IOException {
    for (int i = 0; i < len; i++) {
      int next = read();
      if (next == -1) {
        return i;
      }
      b[off + i] = (byte) next;
    }
    return len;
  }

}
