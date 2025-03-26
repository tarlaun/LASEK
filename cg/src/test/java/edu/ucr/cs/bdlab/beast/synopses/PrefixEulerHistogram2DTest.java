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

public class PrefixEulerHistogram2DTest extends TestCase {

  public void testCreateAndSum() {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    EulerHistogram2D h = new EulerHistogram2D(e, 4, 4);
    h.addEntry(0, 0, 2, 2, 3);
    h.addEntry(2, 0, 2, 2, 5);
    h.addEntry(0, 1, 3, 1, 7);
    PrefixEulerHistogram2D ph = new PrefixEulerHistogram2D(h);
    assertEquals(15, ph.getValue(0, 0, 4, 4));
    assertEquals(15, ph.getValue(1, 1, 2, 1));
    assertEquals(8, ph.getValue(1, 0, 2, 1));
  }
}