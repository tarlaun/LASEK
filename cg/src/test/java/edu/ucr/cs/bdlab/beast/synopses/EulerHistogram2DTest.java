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
package edu.ucr.cs.bdlab.beast.synopses;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class EulerHistogram2DTest extends TestCase {

  public void testCreateAndSum() {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    EulerHistogram2D h = new EulerHistogram2D(e, 4, 4);
    h.addEntry(0, 0, 2, 2, 3);
    h.addEntry(2, 0, 2, 2, 5);
    h.addEntry(0, 1, 3, 1, 7);
    assertEquals(15, h.getValue(0, 0, 4, 4));
    assertEquals(15, h.getValue(1, 1, 2, 1));
    assertEquals(8, h.getValue(1, 0, 2, 1));
  }

  public void testSerializationDeserialization() throws IOException, ClassNotFoundException {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    EulerHistogram2D h = new EulerHistogram2D(e, 4, 4);
    h.addEntry(0, 0, 2, 2, 3);
    h.addEntry(2, 0, 2, 2, 5);
    h.addEntry(0, 1, 3, 1, 7);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream dos = new ObjectOutputStream(baos);
    dos.writeObject(h);
    dos.close();

    ObjectInputStream din = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    EulerHistogram2D h2 = (EulerHistogram2D) din.readObject();
    din.close();
    assertEquals(15, h2.getValue(0, 0, 4, 4));
    assertEquals(15, h2.getValue(1, 1, 2, 1));
    assertEquals(8, h2.getValue(1, 0, 2, 1));
  }

  public void testAddEnvelope() {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    EulerHistogram2D h = new EulerHistogram2D(e, 4, 4);
    h.addEnvelope(0.5, 0.25, 1.7, 1.8, 3);
    h.addEnvelope(2.3, 0.1, 3.1, 1.8, 5);
    h.addEnvelope(0.1, 1.8, 2.5, 1.9, 7);
    assertEquals(15, h.getValue(0, 0, 4, 4));
    assertEquals(15, h.getValue(1, 1, 2, 1));
    assertEquals(8, h.getValue(1, 0, 2, 1));
  }

  public void testSumEnvelope() {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    EulerHistogram2D h = new EulerHistogram2D(e, 4, 4);
    h.addEntry(0, 0, 2, 2, 3);
    h.addEntry(2, 0, 2, 2, 5);
    h.addEntry(0, 1, 3, 1, 7);
    assertEquals(15, h.sumEnvelope(0.1, 0.2, 3.5, 3.6));
    assertEquals(15, h.sumEnvelope(1.2, 1.1, 2.5, 2.7));
    assertEquals(8, h.sumEnvelope(1.5, 0.3, 2.3, 1.0));
  }
}