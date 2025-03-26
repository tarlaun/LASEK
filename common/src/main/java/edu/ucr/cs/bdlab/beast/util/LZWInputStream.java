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
import java.io.InputStream;
import java.util.Arrays;

/**
 * An input stream that decodes a compressed LZW message
 */
public class LZWInputStream extends InputStream {
  /**A code to clear the message table*/
  public static final int ClearCode = 256;

  /**The code that signals the end of information*/
  public static final int EOI_CODE = 257;

  /**
   * The decoded message that is returned from the stream.
   * Notice that we need to keep the entire message in memory because LZW can refer to an old part of the message.
   * We can only clear the message when we find a Clear Code in the LZW stream.
   */
  static class DecodedMessage {
    public byte[] data;
    public int length;

    public DecodedMessage(int initialCapacity) {
      this.data = new byte[initialCapacity];
    }

    private void ensureCapacity(int newLength) {
      if (newLength > data.length)
        data = Arrays.copyOf(data, newLength);
    }

    public void append(byte[] data, int offset, int dataLength) {
      ensureCapacity(this.length + dataLength);
      System.arraycopy(data, offset, this.data, this.length, dataLength);
      this.length += dataLength;
    }

    public void append(byte value) {
      ensureCapacity(this.length + 1);
      data[this.length++] = value;
    }

    public void clear() {
      this.length = 0;
    }
  }

  /**The index of the array that contains the *offset* in the decode table array*/
  private static final int Offset = 0;
  /**The index of the array that contains the *length* in the decode table array*/
  private static final int Length = 1;

  /**The decode table that is built as the message is decoded*/
  private final int[][] decodeTable = new int[2][4096];

  /**The current size fo the decode table including th standard part (0-255 + ClearCode + EOICode)*/
  private int decodeTableSize = 0;

  /**The input stream that contains the encoded message*/
  private final BitInputStream2 inputBits;

  /**Part of the decoded message*/
  private final DecodedMessage decodedMessage = new DecodedMessage(100);

  /**The offset of the next byte to read from the decoded message*/
  private int offsetNextByte;

  private short oldCode = -1;

  /**The current size of the code to read from the encoded LZW stream (in bits)*/
  private int length = 9;
  private int offsetOldCode = 0;

  /**A flag that is raised when the end of the input stream or encoded message is reached.*/
  private boolean reachedEOF = false;

  private static void addStringToTable(int[][] decodeTable, int size, int offset, int length) {
    decodeTable[Offset][size] = offset;
    decodeTable[Length][size] = length;
  }

  public LZWInputStream(InputStream encodedData) {
    inputBits = new BitInputStream2(encodedData);
  }

  @Override
  public int read() throws IOException {
    if (reachedEOF)
      return -1;
    while (offsetNextByte == decodedMessage.length) {
      // Already returned all bytes in the decoded message. Need to decode to retrieve more bytes
      short code = inputBits.nextShort(length);
      if (code == EOI_CODE) {
        reachedEOF = true;
        break;
      } if (code == ClearCode) {
        // Clear the table
        // 256 primitive entries + Clear Code + EOI code
        decodedMessage.clear();
        offsetNextByte = 0;
        decodeTableSize = 256 + 2;
        length = 9;
        code = inputBits.nextShort(length);
        if (code == EOI_CODE) {
          reachedEOF = true;
          break;
        }
        offsetOldCode = decodedMessage.length;
        getAndWriteValue(decodedMessage, decodeTable, code);
        oldCode = code;
      } /* end of ClearCode case */ else {
        int currentOffset = decodedMessage.length;
        if (code < decodeTableSize) {
          // code in table
          getAndWriteValue(decodedMessage, decodeTable, code);
        } else {
          assert code == decodeTableSize : String.format("Unexpected new code %d while table size is %d", code, decodeTableSize);
          getAndWriteValue(decodedMessage, decodeTable, oldCode);
          decodedMessage.append(decodedMessage.data[currentOffset]);
        }
        addStringToTable(decodeTable, decodeTableSize, offsetOldCode, currentOffset - offsetOldCode + 1);
        decodeTableSize++;
        offsetOldCode = currentOffset;
        oldCode = code;
        if (decodeTableSize - 1 == 510)
          length = 10;
        else if (decodeTableSize - 1 == 1022)
          length = 11;
        else if (decodeTableSize - 1 == 2046)
          length = 12;
      }
    }

    return offsetNextByte == decodedMessage.length? -1 : this.decodedMessage.data[offsetNextByte++] & 0xff;
  }

  /**
   * Retrieve a value from the table and write it to the given output stream.
   * @param decodedMessage the output stream to write the decoded message to.
   * @param decodeTable the decode table
   * @param code the code to retrieve from the table.
   */
  static void getAndWriteValue(DecodedMessage decodedMessage, int[][] decodeTable, int code) {
    if (code <= 256)
      decodedMessage.append((byte) code);
    else
      decodedMessage.append(decodedMessage.data, decodeTable[Offset][code], decodeTable[Length][code]);
  }

}
