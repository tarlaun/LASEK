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

import edu.ucr.cs.bdlab.test.JavaSparkTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

public class LZWTest extends JavaSparkTest {
  public void testSimpleEncodeDecode() throws IOException {
    byte[] message = {7, 7, 7, 8, 8, 7, 7, 6, 6};
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    LZWOutputStream out = new LZWOutputStream(baos);
    out.write(message);
    out.close();

    byte[] encodedMessage1 = baos.toByteArray();
    byte[] encodedMessage2 = LZWCodec.encode(message);
    assertArrayEquals(encodedMessage1, encodedMessage2);

    byte[] decodedMessage2 = LZWCodec.decode(encodedMessage1, 0, false);
    assertArrayEquals(message, decodedMessage2);

    byte[] decodedMessage1 = new byte[message.length];
    int length = new LZWInputStream(new ByteArrayInputStream(encodedMessage1)).read(decodedMessage1);
    assertEquals(message.length, length);
    assertArrayEquals(message, decodedMessage1);
  }

  public void testFlushAndWrite() throws IOException {
    byte[] message = {7, 7, 7, 8, 8, 7, 7, 6, 6};
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    LZWOutputStream out = new LZWOutputStream(baos);
    out.write(message);
    out.flush();
    out.close();

    byte[] encodedMessage1 = baos.toByteArray();
    byte[] encodedMessage2 = LZWCodec.encode(message);
    assertArrayEquals(encodedMessage1, encodedMessage2);

    byte[] decodedMessage2 = LZWCodec.decode(encodedMessage1, 0, false);
    assertArrayEquals(message, decodedMessage2);

    byte[] decodedMessage1 = new byte[message.length];
    int length = new LZWInputStream(new ByteArrayInputStream(encodedMessage1)).read(decodedMessage1);
    assertEquals(message.length, length);
    assertArrayEquals(message, decodedMessage1);
  }

  public void testRandomMessages() throws IOException {
    if (shouldRunStressTest()) {
      for (int m = 0; m < 100; m++) {
        Random r = new Random(m);
        int messageLength = r.nextInt(1000) + 20;
        byte[] message = new byte[messageLength];
        for (int i = 0; i < message.length; i++)
          message[i] = (byte) r.nextInt();
        // Try the input/output stream functions
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LZWOutputStream out = new LZWOutputStream(baos);
        out.write(message);
        out.close();

        byte[] encodedMessage1 = baos.toByteArray();
        byte[] decodedMessage1 = new byte[messageLength];
        int length = new LZWInputStream(new ByteArrayInputStream(encodedMessage1)).read(decodedMessage1);
        assertEquals(messageLength, length);
        assertArrayEquals(message, decodedMessage1);
        // Try with codec functions
        byte[] encodedMessage2 = LZWCodec.encode(message);
        byte[] decodedMessage2 = LZWCodec.decode(encodedMessage2, 0, false);
        assertArrayEquals(message, decodedMessage2);

        assertArrayEquals(encodedMessage1, encodedMessage2);
      }
    }
  }

  public void testLargeMessages() throws IOException {
    if (shouldRunStressTest()) {
      for (int m = 0; m < 5; m++) {
        Random r = new Random(m);
        int messageLength = r.nextInt(1000) + 200000;
        byte[] message = new byte[messageLength];
        for (int i = 0; i < message.length; i++)
          message[i] = (byte) r.nextInt();
        // Try with codec functions
        byte[] encodedMessage2 = LZWCodec.encode(message);
        byte[] decodedMessage2 = LZWCodec.decode(encodedMessage2, 0, false);
        assertArrayEquals(message, decodedMessage2);

        // Try the input/output stream functions
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LZWOutputStream out = new LZWOutputStream(baos);
        out.write(message);
        out.close();

        byte[] encodedMessage1 = baos.toByteArray();
        assertArrayEquals(encodedMessage2, encodedMessage1);
        byte[] decodedMessage1 = new byte[messageLength];
        int length = new LZWInputStream(new ByteArrayInputStream(encodedMessage1)).read(decodedMessage1);
        assertEquals(messageLength, length);
        assertArrayEquals(message, decodedMessage1);
      }
    }
  }
}