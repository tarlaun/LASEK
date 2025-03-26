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

public class Prefix2DHistogramTest extends TestCase {
  public void testCreateFromUniformHistogram() {
    // First, create a regular histogram of 4 columns and 3 rows
    EnvelopeNDLite e = new EnvelopeNDLite(2, 1.0, 2.0, 4.0, 6.0);
    UniformHistogram h = new UniformHistogram(e, 4, 3);
    h.addEntry(new int[]{0, 0}, 5);
    h.addEntry(new int[]{1, 0}, 3);
    h.addEntry(new int[]{1, 1}, 4);
    // Now create the prefix sum histogram
    Prefix2DHistogram ph = new Prefix2DHistogram(h);

    assertEquals(e.getMinCoord(0), ph.getMinCoord(0));
    assertEquals(e.getMinCoord(1), ph.getMinCoord(1));
    assertEquals(e.getMaxCoord(0), ph.getMaxCoord(0));
    assertEquals(e.getMaxCoord(1), ph.getMaxCoord(1));

    assertEquals(8, ph.getValue(new int[] {0, 0}, new int[] {2, 1}));
    assertEquals(12, ph.getValue(new int[] {0, 0}, new int[] {2, 2}));
  }

  public void testWithHoles() {
    // First, create a regular histogram of 4 columns and 3 rows
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0);
    UniformHistogram h = new UniformHistogram(e, 4, 4);
    h.addEntry(new int[]{0, 0}, 100);
    h.addEntry(new int[]{3, 2}, 100);
    // Now create the prefix sum histogram
    Prefix2DHistogram ph = new Prefix2DHistogram(h);

    assertEquals(100, ph.getValue(new int[] {3, 2}, new int[] {1, 1}));
  }
}