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

import java.io.IOException;
import java.io.OutputStream;

/**
 * An output stream that writes compact bits
 */
public class BitOutputStream {

  /**The bits that are not yet written to the output.*/
  private long buffer;

  /**The number of bits (most significant) in the buffer that are not yet written*/
  private int bufferSize;

  /**The underlying output stream*/
  private final OutputStream out;

  public BitOutputStream(OutputStream out) {
    this.out = out;
  }

  /**
   * Writes the least significant nBits from the given value to the output stream.
   * @param b the value to read the bits from
   * @param nBits the number of bits from ti
   */
  public void write(long b, int nBits) throws IOException {
    if (nBits + bufferSize > 64) {
      int numBytesToWrite = bufferSize / 8;
      while (numBytesToWrite-- > 0) {
        out.write((int) (buffer >>> 56));
        buffer = buffer << 8;
        bufferSize -= 8;
      }
    }
    // Remove all non-relevant bits from the given value
    b = b & (0xffffffffL >>> (32 - nBits));
    // Place it in the correct position in the buffer
    buffer |= b << (64 - bufferSize - nBits);
    bufferSize += nBits;
  }

  public void writeBoolean(boolean b) throws IOException {
    this.write(b? 1 : 0, 1);
  }

  /**
   * Writes any remaining bits with the minimal number of bytes. If the buffer contains less than a full byte,
   * zeros are appended
   */
  public void flush() throws IOException {
    if (bufferSize % 8 != 0) // Append zeros to make the bufferSize a multiple of eight
      bufferSize += 8 - bufferSize % 8;
    int numBytesToWrite = bufferSize / 8;
    while (numBytesToWrite-- > 0) {
      out.write((int) (buffer >>> 56));
      buffer = buffer << 8;
      bufferSize -= 8;
    }
    out.flush();
  }

  /**
   * Writes any remaining bits and closes the underlying output stream
   */
  public void close() throws IOException {
    this.flush();
    out.close();
  }
}
