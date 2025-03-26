package edu.ucr.cs.bdlab.beast.geolite;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class GeometryWriterReaderTest extends TestCase {

  public void testReadWriteMixed() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    GeometryWriter writer = new GeometryWriter();
    GeometryFactory factory = new GeometryFactory();

    Geometry[] geoms = {
        factory.createPoint(new CoordinateXY(0, 1)),
        factory.createPoint(new CoordinateXY(2, 5)),
        new EnvelopeND(factory, 3, 1, 2, 3, 4, 5, 6),
        factory.createPolygon(),
        new PointND(factory, 5, -1, -2, -3, -4, -5),
        factory.createLineString(new Coordinate[] {new CoordinateXY(1, 2), new CoordinateXY(7, 8)})
    };
    for (Geometry g : geoms)
      writer.write(g, dos, false);
    dos.close();

    // Read them back in order
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    GeometryReader reader = new GeometryReader(factory);
    for (Geometry expected : geoms) {
      Geometry actual = reader.parse(din);
      assertEquals(expected, actual);
    }
    din.close();
  }

  public void testReadWriteEmptyGeometries() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    GeometryWriter writer = new GeometryWriter();
    GeometryFactory factory = new GeometryFactory();

    Geometry[] geoms = {
      factory.createPoint(),
      factory.createLineString(),
      factory.createPolygon(),
      new EnvelopeND(factory)
    };
    for (Geometry g : geoms)
      writer.write(g, dos, false);
    dos.close();

    // Read them back in order
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    GeometryReader reader = new GeometryReader(factory);
    for (Geometry expected : geoms) {
      Geometry actual = reader.parse(din);
      assertTrue("Geometry "+ actual +" should be empty", actual.isEmpty());
      assertEquals(expected, actual);
    }
    din.close();
  }

}