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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

public class HCurvePartitionerTest extends JavaSpatialSparkTest {

  public void testPartition2DPoints() {
    double[][] coords = readCoordsResource("/test.points");
    HCurvePartitioner partitioner = new HCurvePartitioner();
    partitioner.setup(new BeastOptions(false), false);
    Summary summary = new Summary();
    summary.set(new double[]{0.0, 0.0}, new double[]{16.0, 16.0});
    summary.setNumFeatures(11);
    summary.setSize(11 * 2 * 8);
    partitioner.construct(summary, coords, null, 3);

    // Test the split coordinates
    assertEquals(3, partitioner.splitPoints[0].length);
    assertEquals(3.0, partitioner.splitPoints[0][0]);
    assertEquals(8.0, partitioner.splitPoints[1][0]);
    assertEquals(9.0, partitioner.splitPoints[0][1]);
    assertEquals(8.0, partitioner.splitPoints[1][1]);

    // Test overlap partition
    EnvelopeND env = new EnvelopeND(new GeometryFactory(), 2);
    env.set(new double[] {3.0, 5.0}, new double[]{3.0, 5.0});
    assertEquals(0, partitioner.overlapPartition(env));
    env.set(new double[] {3.0, 9.0}, new double[]{4.0, 9.0});
    assertEquals(1, partitioner.overlapPartition(env));
    env.set(new double[] {10.0, 10.0}, new double[]{10.0, 10.0});
    assertEquals(2, partitioner.overlapPartition(env));
    // Test with one of the partition boundary points
    env.set(new double[] {3.0, 8.0}, new double[]{3.0, 8.0});
    // It does not matter which partition will match
    assertTrue(partitioner.overlapPartition(env) == 0 || partitioner.overlapPartition(env) == 1);
  }
}