package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

public class CSVWKTEncoderTest extends JavaSpatialSparkTest {

  public void testEncodePointWithAttributes() {
    PointND p = new PointND(new GeometryFactory(), 2, 0.5, 0.1);
    Feature f = Feature.create(p, null, null, new Object[] {"att1", "att2", "att3", "att4"});
    char fieldSeparator = ',';
    CSVWKTEncoder writer = new CSVWKTEncoder(fieldSeparator, 1);
    String s = writer.apply(f, null).toString();
    assertEquals("att1,POINT(0.5 0.1),att2,att3,att4", s);
  }

  public void testQuoteWKTWhenNeeded() {
    EnvelopeND p = new EnvelopeND(new GeometryFactory(), 2, 0.5, 0.1, 0.7, 0.3);
    Feature f = Feature.create(p, null, null, new Object[] {"att1", "att2", "att3", "att4"});
    char fieldSeparator = ',';
    CSVWKTEncoder writer = new CSVWKTEncoder(fieldSeparator, 1);
    String s = writer.apply(f, null).toString();
    assertEquals("att1,\"POLYGON((0.5 0.1,0.7 0.1,0.7 0.3,0.5 0.3,0.5 0.1))\",att2,att3,att4", s);
  }
}