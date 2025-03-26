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
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.synopses.SummaryAccumulator;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.File;
import java.util.Arrays;

public class GeometricSummaryTest extends JavaSparkTest {

  public void testComputeWithAccumulator() {
    IFeature[] points = {
        Feature.create(null, new PointND(new GeometryFactory(), 2, 0.0, 0.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 2.0, 10.0)),
    };
    SummaryAccumulator accum = Summary.createSummaryAccumulator(sparkContext());
    JavaRDD<IFeature> features = javaSparkContext().parallelize(Arrays.asList(points));
    features = features.map(f -> {accum.add(f); return f;});
    // Force running the accumulator by calling any action
    features.count();
    Summary mbr = accum.value();
    assertEquals(3, mbr.numFeatures());
    assertEquals(0.0, mbr.getMinCoord(0));
    assertEquals(0.0, mbr.getMinCoord(1));
    assertEquals(3.0, mbr.getMaxCoord(0));
    assertEquals(10.0, mbr.getMaxCoord(1));
    assertEquals(3L, mbr.numPoints());
  }

  public void testComputeWithEmptyGeometriesUsingAccumulator() {
    IFeature[] points = {
        Feature.create(null, EmptyGeometry.instance),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 2.0, 10.0)),
    };
    SummaryAccumulator accum = Summary.createSummaryAccumulator(sparkContext());
    JavaRDD<IFeature> features = javaSparkContext().parallelize(Arrays.asList(points));
    features = features.map(f -> {accum.add(f); return f;});
    // Force running the accumulator by calling any action
    features.count();
    Summary mbr = accum.value();
    assertEquals(3, mbr.numFeatures());
    assertEquals(2.0, mbr.getMinCoord(0));
    assertEquals(1.0, mbr.getMinCoord(1));
    assertEquals(3.0, mbr.getMaxCoord(0));
    assertEquals(10.0, mbr.getMaxCoord(1));
  }

  public void testComputeWithEmptyGeometriesUsingRDD() {
    IFeature[] points = {
        Feature.create(null, EmptyGeometry.instance),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 2.0, 10.0)),
    };
    JavaRDD<IFeature> features = javaSparkContext().parallelize(Arrays.asList(points));
    Summary mbr = Summary.computeForFeatures(features);
    assertEquals(3, mbr.numFeatures());
    assertEquals(2.0, mbr.getMinCoord(0));
    assertEquals(1.0, mbr.getMinCoord(1));
    assertEquals(3.0, mbr.getMaxCoord(0));
    assertEquals(10.0, mbr.getMaxCoord(1));
    assertEquals(2L, mbr.numPoints());
  }

  public void testComputeWithOutputSize() {
    String[] wkts = {
        "POINT (1 2)",
        "LINESTRING (1 2, 3 4, 5 6)",
        "POLYGON ((10 20, 15 25, 10 30, 10 20))",
    };
    int totalSize = 0;
    for (String wkt : wkts)
      totalSize += wkt.length() + 1;
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialWriter.OutputFormat(), "wkt(0)");
    JavaRDD<String> wktRDD = javaSparkContext().parallelize(Arrays.asList(wkts));
    JavaRDD<IFeature> features = SpatialReader.parseWKT(wktRDD, 0, '\0');
    Summary summary = GeometricSummary.computeForFeaturesWithOutputSize(features, opts);
    assertEquals(totalSize, summary.size());
  }

  public void testComputeOutputSizeWithDifferentOutputFormat() {
    Path inPath = new Path(scratchPath(), "test.rect");
    copyResource("/test.rect", new File(inPath.toString()));
    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "envelope")
        .set("separator", ",")
        .set("oformat", "rtree");
    Summary summary = (Summary) GeometricSummary.run(opts, new String[] {inPath.toString()}, null, sparkContext());
    assertEquals(14, summary.numFeatures());
    assertTrue("Size should be large", summary.size() >= 448);
    // Try again with same input/output format to make sure it's different
    opts = new BeastOptions(false).set("iformat", "envelope").set("separator", ",");
    summary = (Summary) GeometricSummary.run(opts, new String[] {inPath.toString()}, null, sparkContext());
    assertTrue("Size should be small", summary.size() < 400);
  }

  public void testComputeWithOutputSizeWithAccumulator() {
    String[] wkts = {
        "POINT (1 2)",
        "LINESTRING (1 2, 3 4, 5 6)",
        "POLYGON ((10 20, 15 25, 10 30, 10 20))",
    };
    int totalSize = 0;
    for (String wkt : wkts)
      totalSize += wkt.length() + 1;
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialWriter.OutputFormat(), "wkt(0)");
    SummaryAccumulator accum = GeometricSummary.createSummaryAccumulatorWithWriteSize(sparkContext(), opts);
    JavaRDD<String> wktRDD = javaSparkContext().parallelize(Arrays.asList(wkts));
    JavaRDD<IFeature> features = SpatialReader.parseWKT(wktRDD, 0, '\0');
    features = features.map(f -> {accum.add(f); return f;});
    // Force an action
    features.count();
    assertEquals(totalSize, accum.value().size());
  }

  public void testAutodetect() {
    File input = makeFileCopy("/test2.points");
    try {
      Summary s = (Summary) GeometricSummary.run(new BeastOptions(), new String[] {input.getPath()}, null, sparkContext());
      assertEquals(3, s.numFeatures());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Main method threw an exception");
    }
  }

}