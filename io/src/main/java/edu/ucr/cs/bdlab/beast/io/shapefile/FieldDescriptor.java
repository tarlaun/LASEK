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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FieldDescriptor implements Externalizable, Writable {
  /**
   * Field name in ASCII (zero-filled).
   */
  byte[] fieldName;

  /**
   * Field type in ASCII (B, C, D, N, L, M, @, I, +, F, 0 or G).
   */
  short fieldType;

  /**
   * Field length in binary.
   */
  short fieldLength;

  /**
   * Field decimal count in binary. I think it means number of digits after the decimal point.
   */
  byte decimalCount;

  /**
   * Production .MDX field flag; 0x01 if field has an index tag in the production .MDX file;
   * 0x00 if the field is not indexed.
   */
  byte mdxFlag;

  /**
   * Next Autoincrement value, if the Field type is Autoincrement, 0x00 otherwise.
   */
  int nextAutoIncrementValue;

  public String getFieldName() {
    // Search for the null terminator
    int fieldNameLength = 0;
    while (fieldNameLength < this.fieldName.length && this.fieldName[fieldNameLength] != 0)
      fieldNameLength++;
    return new String(this.fieldName, 0, fieldNameLength);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    write(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(fieldName.length);
    out.write(fieldName);
    out.writeByte(fieldType);
    out.writeByte(fieldLength);
    out.writeByte(decimalCount);
    out.writeByte(mdxFlag);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int fieldNameLength = in.readUnsignedByte();
    if (fieldName == null || fieldNameLength != fieldName.length)
      fieldName = new byte[fieldNameLength];
    in.readFully(fieldName);
    fieldType = in.readByte();
    fieldLength = (short) in.readUnsignedByte();
    decimalCount = (byte) in.readUnsignedByte();
    mdxFlag = in.readByte();
  }
}
