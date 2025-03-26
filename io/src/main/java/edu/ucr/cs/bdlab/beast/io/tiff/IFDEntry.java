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
 * A directory entry in the TIFF file. See specs page 14.
 */
public class IFDEntry extends AbstractIFDEntry {

  /**The number of values, Count of the indicated Type*/
  public int count;

  /**
   * The Value Offset, the file offset (in bytes) of the Value for the field.
   * The Value is expected to begin on a word boundary; the corresponding
   * Value Offset will thus be an even number. This file offset may
   * point anywhere in the file, even after the image data.
   */
  public int offset;

  public IFDEntry() {}

  public IFDEntry read(ByteBuffer buffer, boolean bigEndian) {
    this.tag = buffer.getShort();
    this.type = buffer.getShort();
    this.count = buffer.getInt();
    this.offset = buffer.getInt();
    if (bigEndian && getLength() <= 4) {
      int newValue;
      // Correct the value for big endian depending on the type if the actual value is stored in the offset field
      switch (TiffConstants.TypeSizes[type]) {
        case 1:
          newValue = this.offset >>> 24;
          newValue |= (this.offset >>> 8) & 0xff00;
          newValue |= (this.offset << 8) & 0xff0000;
          newValue |= (this.offset << 24) & 0xff000000;
          break;
        case 2:
          newValue = (this.offset >>> 16) & 0xffff;
          newValue |= (this.offset << 16) & 0xffff0000;
          break;
        case 4:
          newValue = this.offset;
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type %d of length %d", type, TiffConstants.TypeSizes[type]));
      }
      this.offset = newValue;
    }
    return this;
  }

  /**
   * Form the offset field in an IFDEntry to represent the given values. If the total length of the values are
   * more than four bytes, zero is returned.
   * @param type the type of values which determines the length
   * @param values the array of one or more values
   * @param bigEndian whether to encode the values using big endian or little endian
   * @return a 32-bite value that encodes all the given values, or zero if the length is more than four bytes
   */
  public static int makeValue(short type, int[] values, boolean bigEndian) {
    byte elementSize = TiffConstants.TypeSizes[type];
    int length = elementSize * values.length;
    if (length > 4)
      return 0;
    int value = 0;
    if (bigEndian) {
      // Big endian
      switch (elementSize) {
        case 1:
          // 8-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (8 * (3 - i));
          break;
        case 2:
          // 16-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (16 * (1 - i));
          break;
        case 4:
          value = values[0];
          break;
        default:
          throw new RuntimeException("Unsupported value length "+elementSize);
      }
    } else {
      // Little endian
      switch (elementSize) {
        case 1:
          // 8-bit values
          for (int i = 0; i < values.length; i++)
            value |= values[i] << (8 * (3 - i));
          break;
        case 2:
          // 16-bit values
          for (int i = 0; i < values.length; i++) {
            value |= (values[i] & 0xff) << (16 * (1 - i) + 8);
            value |= ((values[i] & 0xff00) >> 8) << (16 * (1 - i));
          }
          break;
        case 4:
          value |= (values[0] & 0xff) << 24;
          value |= (values[0] & 0xff00) << 8;
          value |= (values[0] & 0xff0000) >> 8;
          value |= (values[0] & 0xff000000);
          break;
        default:
          throw new RuntimeException("Unsupported value length "+elementSize);
      }
    }
    return value;
  }

  /**
   * Write an entry to the given byte buffer.
   * @param buffer the buffer to write the IFDEntry to
   */
  public void write(ByteBuffer buffer) {
    buffer.putShort(this.tag);
    buffer.putShort(this.type);
    buffer.putInt(this.count);
    buffer.putInt(this.offset);
  }

  @Override
  public int getCountAsInt() {
    return count;
  }

  @Override
  public long getCountAsLong() {
    return count;
  }

  @Override
  public int getOffsetAsInt() {
    return offset;
  }

  @Override
  public long getOffsetAsLong() {
    return offset;
  }
}
