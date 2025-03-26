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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A feature writer that can write a single big file in parts.
 * It can be used to export a single big file in parallel where the first and last partitions might be different.
 * There are two ways to implement a concrete class according to this abstract class.
 * <ol>
 *   <li>Implement the four functions, {@link #writeFirst(IFeature)}, {@link #writeMiddle(IFeature)},
 *   {@link #writeLast(IFeature)}, and {@link #writeFirstLast(IFeature)}</li>
 *   <li>Implement only the {@link #writeMiddle(IFeature)} function if all records are written in the same way,
 *   e.g., single record per line as in CSV</li>
 * </ol>
 * An implementing class should always call {@code super.initialize()} and {@code super.close()} in the corresponding
 * functions.
 */
public abstract class MultipartFeatureWriter extends FeatureWriter {

  /*
   * To provide a default implementation that hides the complexity of writing first or last records differently,
   * we keep track of the write operations and automatically call the correct function. Since there is no way
   * to know when the last record is written, we have to buffer a record internally before writing it.
   */
  /**A flag that indicates that the buffered feature is the first one*/
  protected boolean firstRecord;

  /**A feature that is buffered and not written yet*/
  protected IFeature bufferedFeature;

  public void writeFirst(IFeature feature) throws IOException, InterruptedException {
    writeMiddle(feature);
  }

  public void writeLast(IFeature feature) throws IOException, InterruptedException {
    writeMiddle(feature);
  }

  public void writeFirstLast(IFeature feature) throws IOException, InterruptedException {
    writeMiddle(feature);
  }

  public abstract void writeMiddle(IFeature feature) throws IOException, InterruptedException;

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    this.bufferedFeature = null;
  }

  @Override
  public final void write(IFeature feature) throws IOException, InterruptedException {
    if (this.bufferedFeature == null) {
      // First record, buffer it and mark it as the first record
      this.firstRecord = true;
    } else {
      // There is a buffered feature, write it first
      if (this.firstRecord)
        writeFirst(this.bufferedFeature);
      else
        writeMiddle(this.bufferedFeature);
      this.firstRecord = false;
    }
    this.bufferedFeature = feature;
  }

  @Override
  public void close() throws IOException, InterruptedException {
    if (this.bufferedFeature != null) {
      if (firstRecord)
        writeFirstLast(this.bufferedFeature);
      else
        writeLast(this.bufferedFeature);
    }
  }
}
