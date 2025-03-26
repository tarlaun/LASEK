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
package edu.ucr.cs.bdlab.beast.geolite;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

public class GeometryHelperTest extends TestCase {

  public static GeometryFactory geometryFactory = new GeometryFactory();

  public void testRelate() {
    PointND p1 = new PointND(geometryFactory, 2, 0.0, 0.0);
    PointND p2 = new PointND(geometryFactory, 2, 2.0, 1.0);
    PointND p3 = new PointND(geometryFactory, 2, 4.0, 2.0);
    PointND p4 = new PointND(geometryFactory, 2, 2.0, 0.0);
    PointND p5 = new PointND(geometryFactory, 2, 0.0, 3.0);

    assertTrue(GeometryHelper.relate(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1)) == 0);
    assertTrue(GeometryHelper.relate(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p4.getCoordinate(0), p4.getCoordinate(1)) < 0);
    assertTrue(GeometryHelper.relate(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p5.getCoordinate(0), p5.getCoordinate(1)) > 0);
  }

  public void testPointOnLineSegment() {
    PointND p1 = new PointND(geometryFactory, 2, 0.0, 0.0);
    PointND p2 = new PointND(geometryFactory, 2, 2.0, 1.0);
    PointND p3 = new PointND(geometryFactory, 2, 4.0,2.0);
    PointND p4 = new PointND(geometryFactory, 2, 2.0,0.0);
    PointND p5 = new PointND(geometryFactory, 2, 1.0,0.5);
    assertFalse(GeometryHelper.pointOnLineSegment(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1)));
    assertFalse(GeometryHelper.pointOnLineSegment(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p4.getCoordinate(0), p4.getCoordinate(1)));
    assertTrue( GeometryHelper.pointOnLineSegment(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p5.getCoordinate(0), p5.getCoordinate(1)));
    assertTrue( GeometryHelper.pointOnLineSegment(p2.getCoordinate(0), p2.getCoordinate(1), p1.getCoordinate(0), p1.getCoordinate(1), p5.getCoordinate(0), p5.getCoordinate(1)));
  }

  public void testLineSegmentOverlap() {
    PointND p1 = new PointND(geometryFactory, 2, 0.0, 0.0);
    PointND p2 = new PointND(geometryFactory, 2, 2.0, 1.0);
    PointND p3 = new PointND(geometryFactory, 2, 2.0, 0.0);
    PointND p4 = new PointND(geometryFactory, 2, 0.0, 5.0);
    assertTrue(GeometryHelper.lineSegmentOverlap(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1), p4.getCoordinate(0), p4.getCoordinate(1)));
    assertFalse(GeometryHelper.lineSegmentOverlap(p1.getCoordinate(0), p1.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p4.getCoordinate(0), p4.getCoordinate(1)));
  }

  public void testTriangleArea() {
    PointND p1 = new PointND(geometryFactory, 2, 0.0, 0.0);
    PointND p2 = new PointND(geometryFactory, 2, 2.0, 0.0);
    PointND p3 = new PointND(geometryFactory, 2, 0.0, 2.0);
    assertEquals(2.0, GeometryHelper.absTriangleArea(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1)));
    assertEquals(2.0, GeometryHelper.signedTriangleArea(p1.getCoordinate(0), p1.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1)));
    assertEquals(-2.0, GeometryHelper.signedTriangleArea(p1.getCoordinate(0), p1.getCoordinate(1), p3.getCoordinate(0), p3.getCoordinate(1), p2.getCoordinate(0), p2.getCoordinate(1)));
  }
}