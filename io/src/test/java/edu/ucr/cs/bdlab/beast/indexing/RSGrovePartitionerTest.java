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
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.Random;

public class RSGrovePartitionerTest extends JavaSpatialSparkTest {

  public void testConstructFromSample() {
    int numPoints = 1000;
    double[] xs = new double[numPoints];
    double[] ys = new double[numPoints];
    for (int i = 0; i < numPoints; i++) {
      xs[i] = Math.random();
      ys[i] = Math.random();
    }
    // Make the summary look like the sample is 10% of the data
    Summary s = new Summary();
    s.set(new double[] {0.0, 0.0}, new double[] {1.0, 1.0});
    s.setNumFeatures(numPoints * 10);
    s.setSize(numPoints * 10 * 16);

    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    // Create 10 partitions
    partitioner.setup(new BeastOptions(false), false);
    partitioner.construct(s, new double[][] {xs, ys}, null, 10);

    assertEquals(10, partitioner.numPartitions());
  }


  public void testNoExpansionToInf() {
    int numPoints = 1000;
    double[] xs = new double[numPoints];
    double[] ys = new double[numPoints];
    for (int i = 0; i < numPoints; i++) {
      xs[i] = Math.random();
      ys[i] = Math.random();
    }
    // Make the summary look like the sample is 10% of the data
    Summary s = new Summary();
    s.set(new double[] {0.0, 0.0}, new double[] {1.0, 1.0});
    s.setNumFeatures(numPoints * 10);
    s.setSize(numPoints * 10 * 16);

    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    // Create 10 partitions
    partitioner.setup(new BeastOptions(false)
        .setBoolean(RSGrovePartitioner.ExpandToInfinity, false), false);
    partitioner.construct(s, new double[][] {xs, ys}, null, 10);

    for (EnvelopeNDLite partition : partitioner)
      assertTrue("Partition "+partition+" should not be infinite", Double.isFinite(partition.getArea()));
  }

  public void testConstructFromKDimensions() {
    int numPoints = 1000;
    int numDimensions = 5;
    double[][] coords = new double[numDimensions][numPoints];
    Random random = new Random();
    for (int d = 0; d < numDimensions; d++) {
      for (int iPoint = 0; iPoint < numPoints; iPoint++) {
        coords[d][iPoint] = random.nextDouble();
      }
    }
    // Make the summary look like the sample is 10% of the data
    Summary s = new Summary();
    double[] minCoord = new double[numDimensions];
    double[] maxCoord = new double[numDimensions];
    for (int d = 0; d < numDimensions; d++) {
      minCoord[d] = 0.0;
      maxCoord[d] = 1.0;
    }
    s.set(minCoord, maxCoord);
    s.setNumFeatures(numPoints * 10);
    s.setSize(numPoints * 10 * 8 * numDimensions);

    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    // Create 10 partitions
    partitioner.setup(new BeastOptions(false), false);
    partitioner.construct(s, coords, null, 10);

    assertEquals(10, partitioner.numPartitions());
  }

  public void testNoNegativeID() throws IOException {
    double[][] coords = readCoordsResource("/test.rect");
    int numRecords = coords[0].length;
    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    partitioner.setup(new BeastOptions(false), false);
    double[][] centroids = new double[2][numRecords];
    Summary s = new Summary();
    s.setCoordinateDimension(2);
    for (int i = 0; i < numRecords; i++) {
      centroids[0][i] = (coords[0][i] + coords[2][i])/2.0;
      centroids[1][i] = (coords[1][i] + coords[2][i])/2.0;
      s.merge(new double[] {coords[0][i], coords[1][i]});
      s.merge(new double[] {coords[2][i], coords[3][i]});
    }
    s.setNumFeatures(numRecords);
    s.setSize(numRecords * 10); // Not really used

    // Create the partition from the centroids
    partitioner.construct(s, centroids, null, 4);

    // Test all the input rectangles and make sure that they are assigned a correct partition ID
    EnvelopeNDLite e = new EnvelopeNDLite(2);
    for (int i = 0; i < numRecords; i++) {
      e.set(new double[]{coords[0][i], coords[1][i]}, new double[]{coords[2][i], coords[3][i]});
      int pid = partitioner.overlapPartition(new EnvelopeND(new GeometryFactory(), e));
      assertTrue(String.format("Record #%d is assigned an invalid partition ID: %d", i, pid), pid >= 0);
    }
  }

  public void testEmptyPointsShouldNotBeAssignedNegativeIDs() {
    int numPoints = 1000;
    int numDimensions = 2;
    double[][] points = new double[numDimensions][numPoints];
    Random r = new Random(0);
    for (int $i = 0; $i < numPoints; $i++) {
      if ($i % 100 == 0) {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = Double.NaN;
      } else {
        for (int $d = 0; $d < numDimensions; $d++)
          points[$d][$i] = r.nextDouble();
      }
    }
    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    partitioner.setup(new BeastOptions(false), false);
    Summary s = new Summary();
    s.setCoordinateDimension(numDimensions);
    for (int $d = 0; $d < numDimensions; $d++) {
      s.setMinCoord($d, 0.0);
      s.setMaxCoord($d, 0.0);
    }
    s.setNumFeatures(numPoints);
    s.setSize(numPoints * 10); // Not really used

    // Create the partition from the centroids
    partitioner.construct(s, points, null, 4);

    // Test all the input rectangles and make sure that they are assigned a correct partition ID
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), numDimensions);
    for (int i = 0; i < numPoints; i++) {
      e.set(new double[]{points[0][i], points[1][i]}, new double[]{points[0][i], points[1][i]});
      int pid = partitioner.overlapPartition(e);
      assertTrue(String.format("Record #%d is assigned an invalid partition ID: %d", i, pid), pid >= 0);
    }
  }

  public void testConstructWithEmptySamples() {
    // Make the summary look like the sample is 10% of the data
    Summary s = new Summary();
    s.set(new double[] {0.0, 0.0}, new double[] {1.0, 1.0});
    s.setNumFeatures(100);
    s.setSize(100 * 10 * 16);

    RSGrovePartitioner partitioner = new RSGrovePartitioner();
    // Create 10 partitions
    partitioner.setup(new BeastOptions(false), false);
    partitioner.construct(s, new double[][] {}, null, 10);

    assertTrue(partitioner.numPartitions() > 1);
  }
}