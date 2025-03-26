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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class SpatialReaderTest extends JavaSpatialSparkTest {

  public void testParsePoints() {
    String[] textFile = {
        "abc,3,4",
        "def,5,6"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> pointRDD = SpatialReader.parsePointsXY(textRDD, 1, 2, ',');
    assertEquals(2, pointRDD.count());
    List<IFeature> points = pointRDD.collect();
    assertEquals(2, points.size());
    assertEquals(2, GeometryHelper.getCoordinateDimension(points.get(0).getGeometry()));
    assertEquals(3.0, ((Point)points.get(0).getGeometry()).getX());
    assertEquals(4.0, ((Point)points.get(0).getGeometry()).getY());
    assertEquals(5.0, ((Point)points.get(1).getGeometry()).getX());
    assertEquals(6.0, ((Point)points.get(1).getGeometry()).getY());
  }

  public void testParsePointsXYZ() {
    String[] textFile = {
        "abc,3,4,1",
        "def,5,6,2"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> pointRDD = SpatialReader.parsePointsXYZ(textRDD, 1, 2, 3, ',');
    assertEquals(2, pointRDD.count());
    List<IFeature> points = pointRDD.collect();
    assertEquals(2, points.size());
    assertEquals(3.0, points.get(0).getGeometry().getCoordinate().getX());
    assertEquals(4.0, points.get(0).getGeometry().getCoordinate().getY());
    assertEquals(1.0, points.get(0).getGeometry().getCoordinate().getZ());
    assertEquals(5.0, points.get(1).getGeometry().getCoordinate().getX());
    assertEquals(6.0, points.get(1).getGeometry().getCoordinate().getY());
    assertEquals(2.0, points.get(1).getGeometry().getCoordinate().getZ());
  }

  public void testParsePointsXYM() {
    String[] textFile = {
        "abc,3,4,1",
        "def,5,6,2"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> pointRDD = SpatialReader.parsePointsXYM(textRDD, 1, 2, 3, ',');
    assertEquals(2, pointRDD.count());
    List<IFeature> points = pointRDD.collect();
    assertEquals(2, points.size());
    assertEquals(3, GeometryHelper.getCoordinateDimension(points.get(0).getGeometry()));
    assertEquals(3.0, points.get(0).getGeometry().getCoordinate().getX());
    assertEquals(4.0, points.get(0).getGeometry().getCoordinate().getY());
    assertTrue(Double.isNaN(points.get(0).getGeometry().getCoordinate().getZ()));
    assertEquals(1.0, points.get(0).getGeometry().getCoordinate().getM());
    assertEquals(5.0, points.get(1).getGeometry().getCoordinate().getX());
    assertEquals(6.0, points.get(1).getGeometry().getCoordinate().getY());
    assertEquals(2.0, points.get(1).getGeometry().getCoordinate().getM());
  }

  public void testParseFeatureAttributes() {
    String[] textFile = {
        "abc,3,4",
        "defg,5,6"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> pointRDD = SpatialReader.parsePointsXY(textRDD, 1, 2, ',');
    List<IFeature> features = pointRDD.collect();
    assertEquals("abc", features.get(0).get(1));
    assertEquals("defg", features.get(1).get(1));
  }

  public void testParseFeatureAttributes2() {
    String[] textFile = {
        "abc,3,def,4,ghi",
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> pointRDD = SpatialReader.parsePointsXY(textRDD, 1, 3, ',');
    List<IFeature> features = pointRDD.collect();
    assertEquals(4, features.get(0).length());
    assertEquals("abc", features.get(0).get(1));
    assertEquals("def", features.get(0).get(2));
    assertEquals("ghi", features.get(0).get(3));
  }

  public void testParseWKT() {
    String[] textFile = {
        "POINT(3 4)",
        "POINT(5 6)"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> geometryRDD = SpatialReader.parseWKT(textRDD, 0, '\t');
    assertEquals(2, geometryRDD.count());
    List<IFeature> features = geometryRDD.collect();
    assertEquals(2, features.size());
    assertEquals(3.0, ((Point)features.get(0).getGeometry()).getX(), 1E-5);
    assertEquals(4.0, ((Point)features.get(0).getGeometry()).getY(), 1E-5);
  }

  public void testParseQuotedWKT() {
    String[] textFile = {
        "abc,'POINT(3 4)',def",
        "abc,\"POINT(5 6)\",def"
    };
    JavaRDD<String> textRDD = javaSparkContext().parallelize(Arrays.asList(textFile));
    JavaRDD<IFeature> geometryRDD = SpatialReader.parseWKT(textRDD, 1, ',');
    assertEquals(2, geometryRDD.count());
    List<IFeature> features = geometryRDD.collect();
    assertEquals(2, features.size());
    assertEquals(3.0, ((Point)features.get(0).getGeometry()).getX(), 1E-5);
    assertEquals(4.0, ((Point)features.get(0).getGeometry()).getY(), 1E-5);
  }

  public void testReadShapefile() {
    File inputFile = new File(new Path(scratchPath(), "test.zip").toString());
    copyResource("/usa-major-cities.zip", inputFile);

    BeastOptions opts = new BeastOptions(false);
    JavaRDD<IFeature> input = SpatialReader.readInput(javaSparkContext(), opts, inputFile.toString(), "shapefile");
    assertEquals(120, input.count());
  }

  public void testReadKDimensionalPoints() {
    File inputFile = new File(new Path(scratchPath(), "test.csv").toString());
    copyResource("/test_points.csv", inputFile);

    // Set the shape to three-dimensional points with the default column indexes, i.e., 0, 1, and 2
    BeastOptions clo = new BeastOptions(false);
    clo.set(CSVFeatureReader.SkipHeader, "true");
    clo.set(CSVFeatureReader.FieldSeparator, ",");
    JavaRDD<IFeature> input = SpatialReader.readInput(javaSparkContext(), clo, inputFile.toString(), "pointk(3)");
    assertEquals(2, input.count());
    IFeature first = input.first();
    assertEquals("Point", first.getGeometry().getGeometryType());
    assertEquals(3, GeometryHelper.getCoordinateDimension(first.getGeometry()));
  }

  public void testReadEnvelopes() {
    File inputFile = new File(new Path(scratchPath(), "test.csv").toString());
    copyResource("/test.partitions", inputFile);

    // Set the shape to three-dimensional points with the default column indexes, i.e., 0, 1, and 2
    BeastOptions clo = new BeastOptions(false);
    clo.set(CSVFeatureReader.SkipHeader, "true");
    clo.set(CSVFeatureReader.FieldSeparator, "\t");
    JavaRDD<IFeature> input = SpatialReader.readInput(javaSparkContext(), clo, inputFile.toString(), "envelopek(2,5)");
    assertEquals(44, input.count());
    EnvelopeNDLite firstEnvelope = new EnvelopeNDLite().merge(input.first().getGeometry());
    assertEquals(125.6258494, firstEnvelope.getMinCoord(0));
  }

  public void testReadTwoFilesWithDifferentFormats() {
    File inputFile1 = new File(new Path(scratchPath(), "test1.csv").toString());
    copyResource("/test.rect", inputFile1);
    File inputFile2 = new File(new Path(scratchPath(), "test2.csv").toString());
    copyResource("/test111.points", inputFile2);

    BeastOptions opts = new BeastOptions(false).set("iformat[0]", "envelope")
        .set("iformat[1]", "point");
    opts.set(CSVFeatureReader.FieldSeparator, ",");

    BeastOptions opts0 = opts.retainIndex(0);
    JavaRDD<IFeature> rectangles = SpatialReader.readInput(javaSparkContext(), opts0, inputFile1.getPath(),
        opts0.getString(SpatialFileRDD.InputFormat()));
    BeastOptions opts1 = opts.retainIndex(1);
    JavaRDD<IFeature> points = SpatialReader.readInput(javaSparkContext(), opts1, inputFile2.getPath(),
        opts1.getString(SpatialFileRDD.InputFormat()));
    assertEquals(14, rectangles.count());
    assertEquals(GeometryType.POLYGON.typename, rectangles.first().getGeometry().getGeometryType());
    assertEquals(new EnvelopeND(new GeometryFactory(), 2, 913.0, 16.0, 924.0, 51.0), rectangles.first().getGeometry());
    assertEquals(111, points.count());
    assertEquals(new PointND(new GeometryFactory(), 101.7075756, 3.2407152), points.first().getGeometry());
  }

  public void testReadWithGivenFormat() {
    File inputFile1 = new File(new Path(scratchPath(), "test1.csv").toString());
    copyResource("/test.rect", inputFile1);
    File inputFile2 = new File(new Path(scratchPath(), "test2.csv").toString());
    copyResource("/test111.points", inputFile2);

    BeastOptions opts = new BeastOptions(false);
    opts.set(CSVFeatureReader.FieldSeparator, ",");

    JavaRDD<IFeature> rectangles = SpatialReader.readInput(javaSparkContext(), opts, inputFile1.getPath(), "envelope");
    JavaRDD<IFeature> points = SpatialReader.readInput(javaSparkContext(), opts, inputFile2.getPath(), "point");
    assertEquals(14, rectangles.count());
    assertEquals(GeometryType.POLYGON.typename, rectangles.first().getGeometry().getGeometryType());
    assertEquals(new EnvelopeND(new GeometryFactory(), 2, 913.0, 16.0, 924.0, 51.0), rectangles.first().getGeometry());
    assertEquals(111, points.count());
    assertEquals(new PointND(new GeometryFactory(), 101.7075756, 3.2407152), points.first().getGeometry());
  }

  public void testReadFileWithEmptyRecordsFromIndex() {
    Path indexPath = new Path(scratchPath(), "index");
    makeResourceCopy("/test_index2", new File(indexPath.toString()));
    Path filePath = new Path(indexPath, "part-00.csv");
    BeastOptions opts = new BeastOptions(false);
    opts.set(CSVFeatureReader.FieldSeparator, ";");
    JavaRDD<IFeature> features = SpatialReader.readInput(javaSparkContext(), opts, filePath.toString(), "wkt");
    assertEquals(3, features.count());
  }
}
