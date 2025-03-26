package edu.ucr.cs.bdlab.beast.geolite;

import junit.framework.TestCase;
import org.locationtech.jts.geom.GeometryFactory;

public class PointNDTest extends TestCase {

  public void testSetEmptyWithZeroCoordinates() {
    GeometryFactory geometryFactory = new GeometryFactory();
    PointND p = new PointND(geometryFactory);
    p.setCoordinateDimension(0);
    p.setEmpty();

    p = new PointND(geometryFactory, 0);
    p.setEmpty();
  }
}