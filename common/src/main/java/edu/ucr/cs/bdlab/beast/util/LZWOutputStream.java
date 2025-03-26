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
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A wrapper output stream that compresses the output using LZW.
 */
public class LZWOutputStream extends OutputStream {

  /**A code to clear the message table*/
  public static final int ClearCode = 256;

  /**The code that signals the end of information*/
  public static final int EOI_CODE = 257;

  /**
   * The table used for encoding.
   * It stores sequences of bytes that were previously observed in the message and give each one a unique key.
   */
  static final class EncodeTable {
    /**Table size including the implicit characters (0-255) and control codes*/
    int tableSize;

    /**
     * Maps each pair of (previous code + new character) to the code that represents up-to the new character.
     * If the pair has not seen before, then its entry will empty.
     * This takes up a total of 2MB memory which is a reasonable price to improve the encoding performance.
     * We use 0 to indicate non-existence.
     */
    short[] encoded = new short[4096 * 256];

    public EncodeTable() {
      this.clear();
    }

    public void clear() {
      Arrays.fill(encoded, (short) 0);
      tableSize = 258;
    }

    public int size() {
      return tableSize;
    }

    /**
     * Checks if the message that consists of an existing code (prev) concatenated with a new character (nextChar)
     * exists in the table. prev=-1 indicates no previous message which makes the result always true.
     * @param prev code of a previous message or -1 if the previous message is null
     * @param nextChar the character to append to the previous message
     * @return {@code true} if the previous message + nextChar exists in the table
     */
    public boolean exists(int prev, int nextChar) {
      if (prev == -1)
        return true;
      return encoded[(prev << 8) | nextChar] != 0;
    }

    public int codeFromString(int prev, int nextChar) {
      if (prev == -1)
        return nextChar & 0xff;
      assert(exists(prev, nextChar));
      return encoded[(prev << 8) | nextChar];
    }

    public void addTableEntry(int prev, int nextChar) {
      assert(!exists(prev, nextChar));
      encoded[(prev << 8) | nextChar] = (short) tableSize++;
    }
  }

  /**The wrapped output stream*/
  private final BitOutputStream encodeOutput;

  /**The encoding table that stores all patterns found so far*/
  EncodeTable encodeTable = new EncodeTable();

  /**The code of the previous pattern found*/
  private int prev = -1;

  /**The current size for the output in terms of number of bits*/
  private int nBits = 9;

  public LZWOutputStream(OutputStream out) throws IOException {
    this.encodeOutput = new BitOutputStream(out);
    encodeOutput.write(ClearCode, nBits);
  }

  @Override
  public void write(int nextChar) throws IOException {
    // Make it unsigned byte
    nextChar = nextChar & 0xff;
    if (encodeTable.exists(prev, nextChar)) {
      // String already exists, append and move on
      prev = encodeTable.codeFromString(prev, nextChar);
    } else {
      // New string. Write previous and add the string to table for future occurrences
      encodeOutput.write(prev, nBits);
      encodeTable.addTableEntry(prev, nextChar);
      if (encodeTable.size() == 512)
        nBits++;
      else if (encodeTable.size() == 1024)
        nBits++;
      else if (encodeTable.size() == 2048)
        nBits++;
      else if (encodeTable.size() == 4095) {
        // Clear the table and write clear code
        encodeOutput.write(ClearCode, nBits);
        encodeTable.clear();
        nBits = 9;
      }
      prev = nextChar;
    }
  }

  @Override
  public void flush() throws IOException {
    // TODO not yet implemented
    // Should write whatever remains in buffer followed by a reset table marker
  }

  @Override
  public void close() throws IOException {
    // Write last character
    encodeOutput.write(prev, nBits);
    if (prev < 256) {
      // When decoding, the decoder will add a new entry not knowing that this is the last character
      // This will result in incorrectly decoding the EOI_CODE unless we adjust its length accordingly
      int decodeTableSize = encodeTable.size() + 1;
      // Increase table size for the last EOI code
      if (decodeTableSize == 512)
        nBits++;
      else if (decodeTableSize == 1024)
        nBits++;
      else if (decodeTableSize == 2048)
        nBits++;
    }
    encodeOutput.write(EOI_CODE, nBits);
    encodeOutput.close();
  }
}
