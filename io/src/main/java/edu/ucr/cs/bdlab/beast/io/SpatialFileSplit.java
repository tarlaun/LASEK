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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Extends the regular file split with envelope bounding function.
 */
public class SpatialFileSplit extends FileSplit {

  /**The spatial range covered by this split or an infinite 2D envelope if it does not cover any range*/
  protected final EnvelopeNDLite envelope = new EnvelopeNDLite();

  public SpatialFileSplit() {}

  public SpatialFileSplit(Path path, long start, long length, String[] hosts) {
    super(path, start, length, hosts);
    this.envelope.setCoordinateDimension(2);
    this.envelope.setInfinite();
  }

  public SpatialFileSplit(Path path, long start, long length, String[] hosts, String[] inMemoryHosts) {
    super(path, start, length, hosts, inMemoryHosts);
    this.envelope.setCoordinateDimension(2);
    this.envelope.setInfinite();
  }

  public SpatialFileSplit(Path path, long start, long length, String[] hosts, String[] inMemoryHosts,
                          EnvelopeNDLite mbb) {
    super(path, start, length, hosts, inMemoryHosts);
    this.envelope.set(mbb);
  }

  /**
   * Returns the minimum bounding box of this split. If unknown, an infinite envelope is returned.
   * @param e an existing envelope to reuse or {@code null} if a new envelope to be created.
   * @return the given envelope or a new envelope if it is null
   */
  public EnvelopeNDLite getEnvelope(EnvelopeNDLite e) {
    if (e == null)
      e = new EnvelopeNDLite(this.envelope);
    else
      e.set(this.envelope);
    return e;
  }

  public EnvelopeNDLite getEnvelope() {
    return this.envelope;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(super.toString());
    str.append(" input #");
    if (Double.isFinite(this.envelope.getSideLength(0))) {
      str.append("MBR: ");
      str.append(this.envelope.toString());
    } else {
      str.append("MBR not set");
    }
    return str.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(envelope, out);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    GeometryHelper.readIEnvelope(envelope, in);
    super.readFields(in);
  }
}
