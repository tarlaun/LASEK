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

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

public class SummaryTest extends JavaSparkTest {
  public static GeometryFactory geometryFactory = new GeometryFactory();

  public void testComputeAverageSideLength() {
    // Test with points
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(new PointND(geometryFactory, 0.0, 3.5));
    geoms.add(new PointND(geometryFactory, 0.5, 1.3));

    Summary s = new Summary();
    s.setCoordinateDimension(2);
    for (Geometry geom : geoms)
      s.expandToGeometry(geom);

    assertEquals(0.0, s.averageSideLength(0));
    assertEquals(0.0, s.averageSideLength(1));

    // Test with envelopes
    geoms.clear();
    geoms.add(new EnvelopeND(geometryFactory, 2, 0.0, 0.0, 5.0, 5.0));
    geoms.add(new EnvelopeND(geometryFactory, 2, 5.0, 2.0, 8.0, 3.0));
    s.setEmpty();
    for (Geometry geom : geoms)
      s.expandToGeometry(geom);
    assertEquals(4.0, s.averageSideLength(0));
    assertEquals(3.0, s.averageSideLength(1));
  }

  public void testComputeNumPoints() {
    // Test with points
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(new PointND(geometryFactory, 0.0, 3.5));
    geoms.add(new PointND(geometryFactory, 0.5, 1.3));

    Summary s = new Summary();
    s.setCoordinateDimension(2);
    for (Geometry geom : geoms)
      s.expandToGeometry(geom);

    assertEquals(2L, s.numPoints());

    // Test with envelopes
    geoms.clear();
    geoms.add(new EnvelopeND(geometryFactory, 2, 0.0, 0.0, 5.0, 5.0));
    geoms.add(new EnvelopeND(geometryFactory, 2, 5.0, 2.0, 8.0, 3.0));
    s.setEmpty();
    for (Geometry geom : geoms)
      s.expandToGeometry(geom);
    assertEquals(8L, s.numPoints());
  }

  public void testExpandToGeometryWithSizeWithNullGeometry() {
    // Test with points
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(new PointND(geometryFactory, 0.0, 3.5));
    geoms.add(new PointND(geometryFactory, 0.5, 1.3));

    Summary s = new Summary();
    s.setCoordinateDimension(2);
    for (Geometry geom : geoms)
      s.expandToGeometryWithSize(geom, 0);
    s.expandToGeometryWithSize(null, 0);

    assertEquals(2L, s.numPoints());
  }

  public void testMerge() {
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(new EnvelopeND(geometryFactory, 2, 0.0, 0.0, 5.0, 5.0));
    geoms.add(new EnvelopeND(geometryFactory, 2, 5.0, 2.0, 8.0, 3.0));

    Summary s0 = new Summary();
    s0.setCoordinateDimension(2);
    Summary s1 = new Summary();
    s1.setCoordinateDimension(2);

    s0.expandToGeometry(geoms.get(0));
    s1.expandToGeometry(geoms.get(1));

    Summary s = new Summary();
    s.setCoordinateDimension(2);
    s.expandToSummary(s1);
    s.expandToSummary(s0);
    assertEquals(2, s.numFeatures());
    assertEquals(4.0, s.averageSideLength(0));
    assertEquals(3.0, s.averageSideLength(1));
    assertEquals(8L, s.numPoints());

    // Test merge with empty
    s0 = new Summary();
    s0.setCoordinateDimension(2);
    for (Geometry geom : geoms)
      s0.expandToGeometry(geom);

    s1 = new Summary();
    s0.expandToSummary(s1);
  }

  public void testCopyConstructor() {
    Summary s1 = new Summary();
    s1.setCoordinateDimension(2);
    s1.set(new double[] {1.0, 2.0}, new double[] {3.0, 4.0});
    s1.setNumFeatures(10);
    s1.setNumPoints(20);
    s1.setSize(1000);

    Summary s2 = new Summary(s1);
    assertEquals(1.0, s2.getMinCoord(0));
    assertEquals(2.0, s2.getMinCoord(1));
    assertEquals(3.0, s2.getMaxCoord(0));
    assertEquals(4.0, s2.getMaxCoord(1));
    assertEquals(10, s2.numFeatures());
    assertEquals(20L, s2.numPoints());
    assertEquals(1000L, s2.size());
    assertEquals(s1, s2);
  }

  public void testSkipEmptyPolygons() {
    Geometry[] geometries = {
        new GeometryFactory().createPolygon(),
        new EnvelopeND(geometryFactory, 2, 1.0, 2.0, 3.0, 5.0),
        new EnvelopeND(geometryFactory, 2, 2.0, 10.0, 5.0, 15.0),
    };
    Summary summary = new Summary();
    summary.setCoordinateDimension(2);
    for (Geometry geometry : geometries)
      summary.expandToGeometry(geometry);
    assertEquals(2.5, summary.averageSideLength(0), 1E-5);
    assertEquals(4.0, summary.averageSideLength(1), 1E-5);
  }

  public void testSummaryWithSize() throws ParseException {
    String[] wkts = {
        "POINT(1 2)",
        "LINESTRING(1 2, 3 4, 5 6)"
    };
    WKTReader reader = new WKTReader();
    Summary summary = new Summary();
    summary.setCoordinateDimension(2);
    for (String wkt : wkts)
      summary.expandToGeometryWithSize((reader.read(wkt)), wkt.length());
    assertEquals(1.0, summary.getMinCoord(0), 1E-5);
    assertEquals(6.0, summary.getMaxCoord(1), 1E-5);
    assertEquals(35, summary.size());
  }

  public void testSummaryWithDataType() throws ParseException {
    String[] wkts = {
        "POINT(1 2)",
        "POINT(3 4)"
    };
    WKTReader reader = new WKTReader();
    Summary summary = new Summary();
    summary.setCoordinateDimension(2);
    for (String wkt : wkts)
      summary.expandToGeometry((reader.read(wkt)));
    assertEquals(GeometryType.POINT, summary.geometryType());

    // Test coercing linestring to multinlinestring
    wkts = new String[] {
        "LINESTRING(1 2, 3 4)",
        "MULTILINESTRING((1 2, 3 4))"
    };
    summary.setEmpty();
    summary.setCoordinateDimension(2);
    for (String wkt : wkts)
      summary.expandToGeometry((reader.read(wkt)));
    assertEquals(GeometryType.MULTILINESTRING, summary.geometryType());

    // Coerce envelope to polygon
    summary.setEmpty();
    summary.setCoordinateDimension(2);
    summary.expandToGeometry(new EnvelopeND(geometryFactory, 2, 0, 0, 1, 1));
    summary.expandToGeometry(new GeometryFactory().createPolygon());
    assertEquals(GeometryType.POLYGON, summary.geometryType());

    // Coerce mixed to GeometryCollection
    summary.setEmpty();
    summary.setCoordinateDimension(2);
    summary.expandToGeometry(new EnvelopeND(geometryFactory, 2, 0, 0, 1, 1));
    summary.expandToGeometry(new GeometryFactory().createLineString());
    assertEquals(GeometryType.GEOMETRYCOLLECTION, summary.geometryType());
  }

  public void testGeometryType() {
    // Test with points
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(geometryFactory.createLineString());
    geoms.add(geometryFactory.createLinearRing());

    Summary s = new Summary();
    s.setCoordinateDimension(2);
    for (Geometry geom : geoms)
      s.expandToGeometry(geom);

    assertEquals(GeometryType.LINESTRING, s.geometryType());
  }

  public void testComputeWithEmptyPartitions() {
    // Test with points
    List<Geometry> geoms = new ArrayList<>();
    geoms.add(new EnvelopeND(geometryFactory, 2, 1.0, 2.0, 3.0, 5.0));
    geoms.add(new EnvelopeND(geometryFactory, 2, 2.0, 10.0, 5.0, 15.0));

    JavaRDD<IFeature> features = javaSparkContext().parallelize(geoms, 4)
        .map(g -> Feature.create(null, g));
    Summary summary = Summary.computeForFeatures(features);
    assertEquals(2, summary.numFeatures());
  }

  public void testComputeWithEmptyRDD() {
    Summary summary = Summary.computeForFeatures(javaSparkContext().emptyRDD());
    assertTrue(summary.isEmpty());
  }
}