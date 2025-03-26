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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * An output stream that forwards all written records to another stream
 * and counts the number of written bytes.
 */
public class CounterOutputStream extends OutputStream implements Serializable {
  /**All writes are redirected to this stream*/
  protected final OutputStream wrapped;
  /**Total number of bytes written to the stream*/
  protected long count;

  /**
   * Counts and discards all written data.
   */
  public CounterOutputStream() {
    this.wrapped = null;
  }

  /**
   * Counts the written bytes and writes them to the given output stream.
   * @param o the underlying output stream to wrap
   */
  public CounterOutputStream(OutputStream o) {
    this.wrapped = o;
  }

  @Override
  public void write(int b) throws IOException {
    if (wrapped != null)
      wrapped.write(b);
    count++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    if (wrapped != null)
      wrapped.write(b);
    count += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (wrapped != null)
      wrapped.write(b, off, len);
    count += len;
  }

  /**
   * Returns the total number of bytes written to this output stream so far.
   * @return the current counter
   */
  public long getCount() {
    return count;
  }

  /**
   * Resets the counter to zero
   */
  public void reset() {
    this.count = 0;
  }

  @Override
  public void close() throws IOException {
    if (wrapped != null)
      wrapped.close();
  }
}
