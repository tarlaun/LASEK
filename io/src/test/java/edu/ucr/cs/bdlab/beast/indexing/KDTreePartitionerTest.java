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
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;

public class KDTreePartitionerTest extends JavaSpatialSparkTest {

  public void testPartition2DPoints() throws IOException {
    double[][] coords = readCoordsResource("/test.points");
    KDTreePartitioner partitioner = new KDTreePartitioner();
    partitioner.setup(new BeastOptions(false), true);
    Summary summary = new Summary();
    summary.set(new double[] {1.0, 0.0}, new double[] {12.0, 12.0});
    summary.setNumFeatures(11);
    summary.setSize(11 * 2 * 8);
    partitioner.construct(summary, coords, null, 4);
    assertEquals(3, partitioner.splitCoords.length);
    assertEquals(5.5, partitioner.splitCoords[0]);
    assertEquals(8.0, partitioner.splitCoords[1]);
    assertEquals(7.0, partitioner.splitCoords[2]);

    // Test overlap a single partition
    EnvelopeND env = new EnvelopeND(new GeometryFactory(), 2, 0.0, 0.0, 0.0, 0.0);
    assertEquals(0, partitioner.overlapPartition(env));
    env.set(new double[] {0.0, 10.0}, new double[] {0.0, 10.0});
    assertEquals(1, partitioner.overlapPartition(env));
    env.set(new double[] {10.0, 0.0}, new double[] {10.0, 0.0});
    assertEquals(2, partitioner.overlapPartition(env));
    env.set(new double[] {10.0, 10.0}, new double[] {10.0, 10.0});
    assertEquals(3, partitioner.overlapPartition(env));

    // Test overlap multiple partitions
    env.set(new double[] {0.0, 0.0}, new double[] {1.0, 10.0});
    IntArray partitions = new IntArray();
    partitioner.overlapPartitions(env, partitions);
    assertEquals(2, partitions.size());
    assertTrue(partitions.contains(0));
    assertTrue(partitions.contains(1));

    // Test partition MBR
    EnvelopeNDLite mbb = new EnvelopeNDLite(2);
    partitioner.getPartitionMBR(0, mbb);
    assertEquals(new EnvelopeNDLite(2, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 5.5, 8.0), mbb);
    partitioner.getPartitionMBR(1, mbb);
    assertEquals(new EnvelopeNDLite(2, Double.NEGATIVE_INFINITY, 8.0, 5.5, Double.POSITIVE_INFINITY), mbb);
    partitioner.getPartitionMBR(2, mbb);
    assertEquals(new EnvelopeNDLite(2, 5.5, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 7.0), mbb);
    partitioner.getPartitionMBR(3, mbb);
    assertEquals(new EnvelopeNDLite(2, 5.5, 7.0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), mbb);
  }
}