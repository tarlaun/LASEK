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
package edu.ucr.cs.bdlab.beast.io.tiff;

import java.nio.ByteBuffer;

/**
 * An abstract IFDEntry which is the base of IFDEntries in both regular and big TIFF files.
 */
public abstract class AbstractIFDEntry {

  /**The Tag that identifies the field*/
  public short tag;

  /**
   * The field Type.
   * 1 = BYTE - 8-bit unsigned integer
   * 2 = ASCII - 8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero).
   * 3 = SHORT - 16-bit (2-byte) unsigned integer
   * 4 = LONG - 32-bit (4-byte) unsigned integer
   * 5 = RATIONAL - Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
   */
  public short type;

  /**
   * Read the contents of the IFDEntry into a buffer.
   * @param buffer the buffer to read the value into
   * @param bigEndian whether the file is stored in big-endian or not.
   * @return this entry
   */
  public abstract AbstractIFDEntry read(ByteBuffer buffer, boolean bigEndian);

  /**
   * Returns the number of values as a 32-bit integer (used with regular (not big) TIFF files)
   * @return the length of this entry in bytes
   */
  public abstract int getCountAsInt();

  /**
   * Returns the number of values as a 64-bit long integer (used with BigTIFF files)
   * @return the length of this entry in bytes
   */
  public abstract long getCountAsLong();

  /**
   * If possible, return the offset field as a 32-bit integer (used with regular (not big) TIFF files)
   * @return the offset of this entry in the file
   */
  public abstract int getOffsetAsInt();

  /**
   * If possible, return the offset field as a 64-bit long integer (used with BigTIFF files)
   * @return the offset of this entry in the file
   */
  public abstract long getOffsetAsLong();


  @Override
  public String toString() {
    return String.format("Tag #%d, type %d, count %d, offset %d", tag & 0xffff, type, getCountAsLong(), getOffsetAsLong());
  }

  public int getLength() {
    return getCountAsInt() * TiffConstants.TypeSizes[type];
  }
}
