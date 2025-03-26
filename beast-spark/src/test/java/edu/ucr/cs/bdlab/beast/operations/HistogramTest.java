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
package edu.ucr.cs.bdlab.beast.operations;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.EulerHistogram2D;
import edu.ucr.cs.bdlab.beast.synopses.HistogramOP;
import edu.ucr.cs.bdlab.beast.synopses.UniformHistogram;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class HistogramTest extends JavaSparkTest {

  public void testComputeTwoRoundsPoints() {
    JavaRDD<IFeature> points = javaSparkContext().parallelize(Arrays.asList( new IFeature[] {
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 3.0)),
    }));
    UniformHistogram h = (UniformHistogram) HistogramOP.computeHistogram(points, 4);
    assertEquals(2, h.getNumPartitions(0));
    assertEquals(2, h.getNumPartitions(1));
    assertEquals(1, h.getValue(new int[] {0,0},new int[] {1,1}));
    assertEquals(0, h.getValue(new int[] {1,0},new int[] {1,1}));
    assertEquals(2, h.getValue(new int[] {0,0},new int[] {2,2}));
  }

  public void testComputeTwoRoundsPointsWithRandomPoints() {
    int numPoints = 1000;
    Feature[] points = new Feature[numPoints];
    Random random = new Random(1);
    for (int i = 0; i < points.length; i++)
      points[i] = Feature.create(null, new PointND(new GeometryFactory(), 2, random.nextDouble() * 2, random.nextDouble()));
    JavaRDD<IFeature> pointsRDD = javaSparkContext().parallelize(Arrays.asList(points), 4);
    int numBuckets = 100;
    UniformHistogram h = (UniformHistogram) HistogramOP.computeHistogram(pointsRDD, numBuckets);
    assertTrue(String.format("Histogram too big. %d buckets > %d", h.getNumPartitions(0) * h.getNumPartitions(1), numBuckets), h.getNumPartitions(0) * h.getNumPartitions(1) <= numBuckets);
    assertEquals(numPoints, h.getValue(new int[] {0, 0}, new int[] {h.getNumPartitions(0), h.getNumPartitions(1)}));
  }

  public void testShouldSkipEmptyGeometries() {
    IFeature[] arfeatures = {
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 3.0)),
        Feature.create(null, EmptyGeometry.instance),
    };
    JavaRDD<IFeature> features = javaSparkContext().parallelize(Arrays.asList(arfeatures));
    UniformHistogram h = (UniformHistogram) HistogramOP.computeHistogram(features, 4);
    assertEquals(2, h.getNumPartitions(0));
    assertEquals(2, h.getNumPartitions(1));
    assertEquals(1, h.getValue(new int[] {0,0},new int[] {1,1}));
    assertEquals(0, h.getValue(new int[] {1,0},new int[] {1,1}));
    assertEquals(2, h.getValue(new int[] {0,0},new int[] {2,2}));
  }

  public void testShouldSkipEmptyGeometriesWithSizeHistogram() {
    IFeature[] arfeatures = {
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 3.0)),
        Feature.create(null, EmptyGeometry.instance),
    };
    JavaRDD<IFeature> features = javaSparkContext().parallelize(Arrays.asList(arfeatures));
    AbstractHistogram h = HistogramOP.computePointHistogramTwoPass(features, IFeature::getStorageSize, new EnvelopeNDLite(2, 1.0, 1.0, 3.0, 3.0), 2, 2);
    assertEquals(2, h.getNumPartitions(0));
    assertEquals(2, h.getNumPartitions(1));
    assertEquals(2 * arfeatures[0].getStorageSize(), h.getValue(new int[] {0,0},new int[] {2,2}));
  }

  public void testHistogramWithWriteSize() {
    String[] wkts = {
        "POINT (2 2),xxxxxxx",
        "POINT (100 1)",
        "POINT (1 100)",
    };
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    JavaRDD<String> wktRDD = javaSparkContext().parallelize(Arrays.asList(wkts));
    JavaRDD<IFeature> features = SpatialReader.parseWKT(wktRDD, 0, ',');

    UniformHistogram histogram = Histogram.computePointWriteSizeHistogram(features,
        new EnvelopeNDLite(2, 1.0, 1.0, 100.0, 100.0), opts, 2, 2);
    assertEquals(wkts[0].length() + 1, histogram.getValue(new int[]{0, 0}, new int[] {1,1}));
  }

  public void testHistogramWithMemorySize() throws IOException {
    Path inPath = new Path(scratchPath(), "input.points");
    copyResource("/test111.points", new File(inPath.toString()));

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "point")
        .set("separator", ",")
        .set("histogramvalue", "size");
    JavaRDD<IFeature> features = SpatialReader.readInput(javaSparkContext(), opts, inPath.toString(), opts.getString(SpatialFileRDD.InputFormat()));
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, -180, -90.0, 180.0, 90.0);
    UniformHistogram h = HistogramOP.computePointHistogramTwoPass(features, IFeature::getStorageSize, mbr, 1000);
    assertEquals(111 * (8 * 2 + GeometryHelper.FixedGeometryOverhead), h.sumRectangle(-180, -90.0, 180.0, 90.0));
  }

  public void testComputeEulerHistogramWithOutOfBoundEnvelopes() {
    // This is used when computing the Euler histogram with visualization and some rectangles are out of map boundaries
    int numRecords = 1000;
    Feature[] envelopes = new Feature[numRecords + 1];
    Random random = new Random(0);
    for (int i = 0; i < envelopes.length; i++) {
      int x = random.nextInt(1000);
      int y = random.nextInt(1000);
      envelopes[i] = Feature.create(null, new EnvelopeND(new GeometryFactory(), 2, x, y, x + 100, y + 100));
    }
    // Add a record that is completely out of bounds
    envelopes[numRecords] = Feature.create(null, new EnvelopeND(new GeometryFactory(), 2, 2000, 2000, 2000 + 100, 2000 + 100));
    JavaRDD<IFeature> envelopesRDD = javaSparkContext().parallelize(Arrays.asList(envelopes), 4);
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0, 0, 1000, 1000);

    EulerHistogram2D histogram = HistogramOP.computeEulerHistogram(envelopesRDD, IFeature::getStorageSize, mbr, 10, 10);
    // Only the last record should be completely dropped. All other records should be counted
    assertEquals(numRecords * (8 * 4 + GeometryHelper.FixedGeometryOverhead), histogram.getValue(0, 0, 10, 10));
  }
}