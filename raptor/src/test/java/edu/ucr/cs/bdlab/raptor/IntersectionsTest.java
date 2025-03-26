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
package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;

import java.awt.geom.AffineTransform;
import java.util.Arrays;

public class IntersectionsTest extends JavaSparkTest {

  public static GeometryFactory factory = GeometryReader.DefaultGeometryFactory;

  public static Polygon createPolygon(CoordinateSequence ... rings) {
    LinearRing shell = factory.createLinearRing(rings[0]);
    LinearRing[] holes = new LinearRing[rings.length - 1];
    for (int $i = 1; $i < rings.length; $i++)
      holes[$i-1] = factory.createLinearRing(rings[$i]);
    return factory.createPolygon(shell, holes);
  }

  public static CoordinateSequence createCoordinateSequence(double ... coordinates) {
    int size = coordinates.length / 2;
    CoordinateSequence cs = factory.getCoordinateSequenceFactory().create(coordinates.length / 2, 2);
    for (int i = 0; i < size; i++) {
      cs.setOrdinate(i, 0, coordinates[2 * i]);
      cs.setOrdinate(i, 1, coordinates[2 * i + 1]);
    }
    return cs;
  }

  static final Polygon p1, p2, p3, p4, p5, p6, p7, p8, p9;

  static final LineString l1, l2, l3;

  static {
    p1 = createPolygon(createCoordinateSequence(
        1.0, 1.0,
        9.0, 3.0,
        3.0, 5.0,
        1.0, 1.0));

    p2 = createPolygon(createCoordinateSequence(
        12.0, 3.0,
        18.0, 5.0,
        15.0, 7.0,
        12.0, 3.0));

    p3 = createPolygon(createCoordinateSequence(
        -3.0, 6.0,
        12.0, 9.0,
        3.0, 11.0,
        -3.0, 6.0));

    p4 = createPolygon(createCoordinateSequence(
        5.0, 1.0,
        8.0, 2.0,
        6.0, 5.0,
        5.0, 1.0));

    p5 = createPolygon(createCoordinateSequence(
        6.0, 11.0,
        14.0, 13.0,
        8.0, 15.0,
        6.0, 11.0));

    p6 = createPolygon(createCoordinateSequence(
        9.0, 21.0,
        12.0, 22.0,
        10.0, 25.0,
        9.0, 21.0));

    p7 = createPolygon(createCoordinateSequence(
        15.3, 8.3,
        15.8, 8.4,
        15.5, 8.8,
        15.3, 8.3));

    p8 = createPolygon(createCoordinateSequence(
        15.0, 11.0,
        15.0, 15.0,
        16.0, 13.0,
        17.0, 15.0,
        17.0, 11.0,
        15.0, 11.0));

    p9 = createPolygon(createCoordinateSequence(
        1.0, 0.0,
        1.0, 1.0,
        0.0, 1.0,
        -1E18, 0.0,
        1.0, 0.0
        ));

    l1 = factory.createLineString(createCoordinateSequence(
        7.5, 16.5,
        14.5, 17.5,
        9.5, 18.5));

    l2 = factory.createLineString(createCoordinateSequence(
        0.5, 15.5,
        0.5, 19.5,
        4.5, 15.5));

    l3 = factory.createLineString(createCoordinateSequence(
            18.5, -1.5,
            18.5, 2.5,
            22.5, -1.5));
  }

  private static RasterMetadata createSimpleGrid(int numTilesX, int numTilesY, int tileWidth, int tileHeight) {
    return new RasterMetadata(0, 0, numTilesX * tileWidth, numTilesY * tileHeight,
        tileWidth, tileHeight, 0, new AffineTransform());
  }

  public void testEmptyPolygon() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p1, createPolygon(createCoordinateSequence())}, metadata);
    assertEquals(4, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4}, ys);
  }

  public void testEmptyListOfGeometries() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {}, metadata);
    assertEquals(0, intersections.getNumIntersections());
  }

  public void testComputeOnePolygon() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p1}, metadata);
    assertEquals(4, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4}, ys);
  }

  public void testOverflowCalculation() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p9}, metadata);
    assertEquals(1, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {0}, x1s);
    assertArrayEquals(new int[] {0}, x2s);
    assertArrayEquals(new int[] {0}, ys);
  }

  public void testComputeOnePolygonWithMergedRanges() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p8}, metadata);
    assertEquals(3, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {15, 15, 15}, x1s);
    assertArrayEquals(new int[] {16, 16, 16}, x2s);
    assertArrayEquals(new int[] {11, 12, 13}, ys);
  }

  public void testTreatNonclosedLineStringAsLinearRing() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    Coordinate[] coordinates = p2.getCoordinates();
    coordinates = Arrays.copyOf(coordinates, coordinates.length - 1);
    LineString ls = factory.createLineString(coordinates);
    intersections.compute(new Geometry[] {p1, ls}, metadata);
    assertEquals(8, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3, 12, 13, 14, 15}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4, 13, 16, 16, 15}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4, 3, 4, 5, 6}, ys);
  }

  public void testSmallPolygon() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p7}, metadata);
    assertEquals(1, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {15}, x1s);
    assertArrayEquals(new int[] {15}, x2s);
    assertArrayEquals(new int[] {8}, ys);
  }

  public void testLinestring() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {l1}, metadata);
    assertEquals(5, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] { 7,  9, 10, 11, 10}, x1s);
    assertArrayEquals(new int[] { 9,  9, 10, 14, 11}, x2s);
    assertArrayEquals(new int[] {16, 18, 16, 17, 18}, ys);
  }

  public void testLinestring2() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {l2}, metadata);
    assertEquals(8, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] { 0,  4,  0,  3,  0,  2,  0,  0}, x1s);
    assertArrayEquals(new int[] { 0,  4,  0,  3,  0,  2,  1,  0}, x2s);
    assertArrayEquals(new int[] {15, 15, 16, 16, 17, 17, 18, 19}, ys);
  }

  public void testPartiallyOutsideLinestring() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {l3}, metadata);
    assertEquals(3, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {18, 18, 18}, x1s);
    assertArrayEquals(new int[] {18, 19, 18}, x2s);
    assertArrayEquals(new int[] { 0,  1,  2}, ys);
  }

  public void testComputeOnePolygonWithOutOfBounds() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p3}, metadata);
    assertEquals(3, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {0, 0, 1}, x1s);
    assertArrayEquals(new int[] {4, 9, 9}, x2s);
    assertArrayEquals(new int[] {7, 8, 9}, ys);
  }

  public void testComputeTwoDisjointPolygonsInTwoTiles() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p1, p2}, metadata);
    assertEquals(8, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3, 12, 13, 14, 15}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4, 13, 16, 16, 15}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4, 3, 4, 5, 6}, ys);
  }

  public void testComputeTwoOverlappingPolygonsInOneTile() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p1, p4}, metadata);
    assertEquals(7, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    int[] pids = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
      pids[$i] = intersections.getPolygonIndex($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3, 5, 5, 6}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4, 6, 7, 6}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4, 1, 2, 3}, ys);
    assertArrayEquals(new int[] {0, 0, 0, 0, 1, 1, 1}, pids);
 }

  public void testComputeOnePolygonCrossesTiles() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p5}, metadata);
    assertEquals(6, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] { 6,  7,  7,  8, 10, 10}, x1s);
    assertArrayEquals(new int[] { 7,  9,  9,  9, 11, 12}, x2s);
    assertArrayEquals(new int[] {11, 12, 13, 14, 12, 13}, ys);
  }

  public void testComputeMultiPolygon() {
    RasterMetadata metadata = createSimpleGrid(2, 2, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {factory.createMultiPolygon(new Polygon[] {p1, p2})}, metadata);
    assertEquals(8, intersections.getNumIntersections());
    int[] x1s = new int[intersections.getNumIntersections()];
    int[] x2s = new int[intersections.getNumIntersections()];
    int[] ys = new int[intersections.getNumIntersections()];
    for (int $i = 0; $i < intersections.getNumIntersections(); $i++) {
      x1s[$i] = intersections.getX1($i);
      x2s[$i] = intersections.getX2($i);
      ys[$i] = intersections.getY($i);
    }
    assertArrayEquals(new int[] {1, 2, 2, 3, 12, 13, 14, 15}, x1s);
    assertArrayEquals(new int[] {2, 6, 7, 4, 13, 16, 16, 15}, x2s);
    assertArrayEquals(new int[] {1, 2, 3, 4, 3, 4, 5, 6}, ys);
  }

  public void testVectorOutOfBounds() {
    RasterMetadata metadata = createSimpleGrid(1, 1, 10, 10);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p6}, metadata);
    assertEquals(0, intersections.getNumIntersections());
  }

  public void testRangeSpanManyTiles() {
    RasterMetadata metadata = createSimpleGrid(10, 10, 1, 1);
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {p1}, metadata);
    assertEquals(15, intersections.getNumIntersections());
  }

  public void testOverflowCalculation3() throws FactoryException, ParseException {
    RasterMetadata metadata = createSimpleGrid(10, 10, 2, 2);
    WKTReader reader = new WKTReader();
    Geometry geom1 = reader.read("POLYGON ((0 0, 0 3E+9, 1 3E+9, 1 0, 0 0))");
    Geometry geom2 = reader.read("POLYGON ((0 0, 0 -3E+9, 1 -3E+9, 1 0, 0 0))");
    Intersections intersections = new Intersections();
    intersections.compute(new Geometry[] {geom1, geom2}, metadata);
    assertTrue("Must have an even number of intersections", intersections.getNumIntersections() % 2 == 0);
  }
}