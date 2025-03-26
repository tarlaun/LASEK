/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.util.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Holds the fixed-size 100-byte header of Shapefiles.
 */
public class ShapefileHeader implements Externalizable {
  /**
   * The signature of the Shapefiles
   */
  static final int Signature = 9994;

  /**
   * Length of the file in 16-bit words (i.e., size in bytes / 2)
   */
  int fileLength;

  /**
   * As of now, the version should be always 1000
   */
  int version;

  /**
   * Type of shapes stored in the file
   */
  int shapeType;

  /**
   * The minimum bounding rectangle (MBR) of the file
   */
  double xmin, ymin, xmax, ymax;

  /**
   * Bounds on the third z dimension. Value is zero when not used.
   */
  double zmin, zmax;

  /**
   * Bounds on the measure value. Zero if not used.
   */
  double mmin, mmax;

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    write(out);
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(Signature); // File signature
    out.writeInt(0); // Five unused integers
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(fileLength);
    IOUtil.writeIntLittleEndian(out, version);
    IOUtil.writeIntLittleEndian(out, shapeType);
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(xmin));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(ymin));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(xmax));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(ymax));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(zmin));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(zmax));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(mmin));
    IOUtil.writeLongLittleEndian(out, Double.doubleToLongBits(mmax));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    readFields(in);
  }

  public void readFields(DataInput in) throws IOException {
    int code = in.readInt();
    if (code != Signature)
      throw new RuntimeException(String.format("Invalid Shapefile code %d. Expected %d.", code, Signature));
    in.skipBytes(5 * 4); // Skip the five unused integers
    this.fileLength = in.readInt();
    this.version = IOUtil.readIntLittleEndian(in);
    this.shapeType = IOUtil.readIntLittleEndian(in);
    this.xmin = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.ymin = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.xmax = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.ymax = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.zmin = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.zmax = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.mmin = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
    this.mmax = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in));
  }

  @Override
  public String toString() {
    return "Header{" +
        "fileLength=" + fileLength +
        ", version=" + version +
        ", shapeType=" + shapeType +
        ", xmin=" + xmin +
        ", ymin=" + ymin +
        ", xmax=" + xmax +
        ", ymax=" + ymax +
        ", zmin=" + zmin +
        ", zmax=" + zmax +
        ", mmin=" + mmin +
        ", mmax=" + mmax +
        '}';
  }
}
