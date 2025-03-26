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
 * A directory entry in the Big TIFF file.
 */
public class BigIFDEntry extends AbstractIFDEntry {

  /**The number of values, Count of the indicated Type*/
  public long count;

  /**
   * The Value Offset, the file offset (in bytes) of the Value for the field.
   * The Value is expected to begin on a word boundary; the corresponding
   * Value Offset will thus be an even number. This file offset may
   * point anywhere in the file, even after the image data.
   */
  public long offset;

  public BigIFDEntry read(ByteBuffer buffer, boolean bigEndian) {
    this.tag = buffer.getShort();
    this.type = buffer.getShort();
    this.count = buffer.getLong();
    this.offset = buffer.getLong();
    // Handle big endian for records that are less than eight bytes
    if (bigEndian && getLength() <= 8) {
      long newValue;
      // Correct the value for big endian depending on the type if the actual value is stored in the offset field
      switch (TiffConstants.TypeSizes[type]) {
        case 1:
          newValue = this.offset >>> 56;
          newValue |= (this.offset >>> 40) &             0xff00L;
          newValue |= (this.offset >>> 24) &           0xff0000L;
          newValue |= (this.offset >>>  8) &         0xff000000L;
          newValue |= (this.offset <<   8) &       0xff00000000L;
          newValue |= (this.offset <<  24) &     0xff0000000000L;
          newValue |= (this.offset <<  40) &   0xff000000000000L;
          newValue |= (this.offset <<  56) & 0xff00000000000000L;
          break;
        case 2:
          newValue = this.offset >>> 48;
          newValue |= (this.offset >>> 16) &         0xffff0000L;
          newValue |= (this.offset <<  16) &     0xffff00000000L;
          newValue |= (this.offset <<  48) & 0xffff000000000000L;
          break;
        case 4:
          newValue = this.offset >>> 32;
          newValue |= (this.offset << 32) & 0xffffffff00000000L;
          break;
        case 8:
          newValue = this.offset;
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type %d of length %d", type, TiffConstants.TypeSizes[type]));
      }
      this.offset = newValue;
    }
    return this;
  }

  @Override
  public int getCountAsInt() {
    if (count > Integer.MAX_VALUE)
      throw new RuntimeException(String.format("Value too big %d", count));
    return (int) count;
  }

  @Override
  public long getCountAsLong() {
    return count;
  }

  @Override
  public int getOffsetAsInt() {
    if (offset > Integer.MAX_VALUE)
      throw new RuntimeException(String.format("Value too big %d for tag %d", offset, tag));
    return (int) offset;
  }

  @Override
  public long getOffsetAsLong() {
    return offset;
  }

  /**
   * Form the offset field in an IFDEntry to represent the given values. If the total length of the values are
   * more than eight bytes, zero is returned.
   * @param type the type of values which determines the length
   * @param values the array of one or more values
   * @param bigEndian whether to encode the values using big endian or little endian
   * @return a 32-bite value that encodes all the given values, or zero if the length is more than four bytes
   */
  public static long makeValueBig(short type, long[] values, boolean bigEndian) {
    byte elementSize = TiffConstants.TypeSizes[type];
    int length = elementSize * values.length;
    if (length > 8)
      return 0;
    long value = 0;
    if (bigEndian) {
      // Big endian
      switch (elementSize) {
        case 1:
          // 8-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (8 * (7 - i));
          break;
        case 2:
          // 16-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (16 * (3 - i));
          break;
        case 4:
          // 32-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (32 * (1 - i));
          break;
        case 8:
          // 64-bit values
          value = values[0];
          break;
        default:
          throw new RuntimeException("Unsupported value length "+elementSize);
      }
    } else {
      // Little endian
      throw new RuntimeException("Little endian with BigGeoTiff is not currently supported");
    }
    return value;
  }
}
