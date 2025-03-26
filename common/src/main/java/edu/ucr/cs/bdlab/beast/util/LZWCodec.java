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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Decodes LZW-compressed data from a TIFF file.
 * This code is based on the description in the TIFF file specs.
 */
public class LZWCodec {

  /**A code to clear the message table*/
  public static final int ClearCode = 256;

  /**The code that signals the end of information*/
  public static final int EOI_CODE = 257;

  /**The index of the array that contains the *offset* in the decode table array*/
  private static final int Offset = 0;
  /**The index of the array that contains the *length* in the decode table array*/
  private static final int Length = 1;

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
  }

  /**
   * Retrieve a value from the table and write it to the given output stream.
   * @param decodedMessage the output stream to write the decoded message to.
   * @param decodeTable the decode table
   * @param code the code to retrieve from the table.
   * @throws IOException if an error happens while appending to the decoded message
   */
  static void getAndWriteValue(DecodedMessage decodedMessage, int[][] decodeTable, int code) throws IOException {
    if (code <= 256)
      decodedMessage.append((byte) code);
    else
      decodedMessage.append(decodedMessage.data, decodeTable[Offset][code], decodeTable[Length][code]);
  }

  /**
   * Decodes LZW-encoded message. See TIFF specs page 61 for details.
   * @param encodedData the encoded data as an array of bytes
   * @param expectedDecodeSize (optional) A hint about the decoded message size to reduce dynamic memory allocation.
   * @param terminateAtSize terminate when the size of the decoded message reaches the expected decode size.
   *                        This flag is useful when the encoded message might not have a proper terminator.
   * @return the decoded data as an array of bytes
   */
  public static byte[] decode(byte[] encodedData, int expectedDecodeSize, boolean terminateAtSize) {
    int terminatingSize = terminateAtSize? expectedDecodeSize : Integer.MAX_VALUE;
    BitInputStream inputBits = new BitInputStream(encodedData);
    if (expectedDecodeSize == 0)
      expectedDecodeSize = encodedData.length * 5;
    DecodedMessage decodedMessage = new DecodedMessage(expectedDecodeSize);
    int[][] decodeTable = new int[2][4096];
    int decodeTableSize = 0;
    short code, oldCode = -1;
    int worldLength = 9; // The length of each work in the encoded data
    int remainingBits = encodedData.length * 8;
    int offsetOldCode = 0;
    code = inputBits.nextShort(worldLength);
    try {
      while (code != EOI_CODE && worldLength < remainingBits && decodedMessage.length < terminatingSize) {
        remainingBits -= worldLength;
        if (code == ClearCode) {
          // Clear the table
          // +2 is for Clear Code and EOI special codes
          decodeTableSize = 256 + 2;
          worldLength = 9;
          code = inputBits.nextShort(worldLength);
          remainingBits -= worldLength;
          if (code == EOI_CODE)
            break;
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
          // Check if the decode table reached its limit and the word length should be increased
          if (decodeTableSize - 1 == 510)
            worldLength = 10;
          else if (decodeTableSize - 1 == 1022)
            worldLength = 11;
          else if (decodeTableSize - 1 == 2046)
            worldLength = 12;
        }
        code = inputBits.nextShort(Math.min(worldLength, remainingBits));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error decoding LZW", e);
    }

    if (decodedMessage.length != decodedMessage.data.length)
      decodedMessage.data = Arrays.copyOf(decodedMessage.data, decodedMessage.length);
    return decodedMessage.data;
  }

  private static void addStringToTable(int[][] decodeTable, int size, int offset, int length) {
    decodeTable[Offset][size] = offset;
    decodeTable[Length][size] = length;
  }

  public static byte[] encode(byte[] message) {
    try {
      ByteArrayOutputStream encodedMessage = new ByteArrayOutputStream();
      LZWOutputStream zout = new LZWOutputStream(encodedMessage);
      zout.write(message);
      zout.close();
      return encodedMessage.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected error while encoding", e);
    }
  }
}
