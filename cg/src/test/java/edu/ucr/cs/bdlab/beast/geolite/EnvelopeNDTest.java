package edu.ucr.cs.bdlab.beast.geolite;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class EnvelopeNDTest extends TestCase {

  public void testComputeEnvelope() {
    GeometryFactory geometryFactory = new GeometryFactory();
    EnvelopeND e = new EnvelopeND(geometryFactory, 2, 0, 0, 1, 1);
    assertEquals(1.0, e.getEnvelopeInternal().getArea());
    e.setMinCoord(0, -1);
    assertEquals(2.0, e.getEnvelopeInternal().getArea());
  }

  public void testSetCoordinate() {
    GeometryFactory geometryFactory = new GeometryFactory();
    EnvelopeND e = new EnvelopeND(geometryFactory);
    EnvelopeND e2 = new EnvelopeND(geometryFactory);
    e.set(e2);
    assertTrue(e.isEmpty());
  }

  public void testGeometryRelationship() {
    GeometryFactory geometryFactory = new GeometryFactory();

    EnvelopeND e1 = new EnvelopeND(geometryFactory, 2, 0, 0, 1, 1);
    EnvelopeND e2 = new EnvelopeND(geometryFactory, 2, 0.5, 0.5, 1, 1);
    EnvelopeND e3 = new EnvelopeND(geometryFactory, 2, 1, 1, 2, 2);
    EnvelopeND e4 = new EnvelopeND(geometryFactory, 2, 1, 0, 2, 1);
    assertFalse(e1.disjoint(e2));
    assertTrue(e1.contains(e2));
    assertFalse(e2.contains(e1));
    assertTrue(e1.touches(e3)); // Touch in a point
    assertTrue(e1.touches(e4)); // Touch in a line
  }

  public void testIntersectsWithPoints() {
    GeometryFactory geometryFactory = new GeometryFactory();
    EnvelopeND e1 = new EnvelopeND(geometryFactory, 2, 0, 0, 1, 1);
    EnvelopeND e2 = new EnvelopeND(geometryFactory, 2, 0, 0, 0, 0);
    assertTrue("Should be intersecting", e1.intersectsEnvelope(e2));

    EnvelopeND e3 = new EnvelopeND(geometryFactory, 2, 0, 5, 0, 5);
    assertFalse("Should not be intersecting", e1.intersectsEnvelope(e3));
  }

  public void testIntersectsWithLineStrings() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry e1 = geometryFactory.createLineString(new Coordinate[] {
        new CoordinateXY(0, 0), new CoordinateXY(1, 0),
        new CoordinateXY(1, 1), new CoordinateXY(0, 1),
        new CoordinateXY(0, 0)
    });
    EnvelopeND e2 = new EnvelopeND(geometryFactory, 2, 0, 0, 0, 0);
//    assertTrue("Should be intersecting", e1.intersects(e2));
    assertTrue("Should be intersecting", e2.intersects(e1));

    EnvelopeND e3 = new EnvelopeND(geometryFactory, 2, 0, 5, 0, 5);
    assertFalse("Should not be intersecting", e1.intersects(e3));
    assertFalse("Should not be intersecting", e3.intersects(e1));
  }

  public void testIntersectsWithPolygons() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry e1 = geometryFactory.toGeometry(new Envelope(0, 1, 0, 1));
    EnvelopeND e2 = new EnvelopeND(geometryFactory, 2, 0, 0, 0, 0);
    assertTrue("Should be intersecting", e1.intersects(e2));
    assertTrue("Should be intersecting", e2.intersects(e1));

    EnvelopeND e3 = new EnvelopeND(geometryFactory, 2, 0, 5, 0, 5);
    assertFalse("Should not be intersecting", e1.intersects(e3));
    assertFalse("Should not be intersecting", e3.intersects(e1));
  }

}