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
import java.io.InputStream;

/**
 * An input stream that is backed up by a long array. This can be coupled with {@link LongArrayOutputStream}
 * to go beyond the array size limit of {@link java.io.ByteArrayInputStream}
 */
public class LongArrayInputStream extends InputStream {

  /**
   * The underlying data stored in little-endian format. This means that the first byte is stored in the
   * least significant bits of the first entry.
   */
  private final long[] data;

  /**Which byte will be read next*/
  private long offset;

  public LongArrayInputStream(long[] data) {
    this.data = data;
  }

  @Override
  public int read() throws IOException {
    if (available() == 0)
      return -1;
    int entry = (int) (offset >> 3);
    int shift = (int) ((offset & 0x7) << 3);
    offset++;
    return (int) ((this.data[entry] >>> shift) & 0xff);
  }

  @Override
  public int available() throws IOException {
    return (int) ((data.length << 3) - offset);
  }
}
