package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import junit.framework.TestCase;
import org.locationtech.jts.geom.GeometryFactory;


public class CSVEnvelopeDecoderTest extends TestCase {

  public void testParse3D() {
    EnvelopeND expected = new EnvelopeND(new GeometryFactory(), 3, 0.0, 1.2, 3.4, 15.3, 13.4, 17.7);
    String str = "0.0,1.2,3.4,15.3,13.4,17.7";
    EnvelopeND actual = CSVEnvelopeDecoder.instance.apply(str, null);
    assertEquals(expected, actual);
  }

  public void testReuseEnvelopes() {
    EnvelopeND expected = new EnvelopeND(new GeometryFactory(), 3, 0.0, 1.2, 3.4, 15.3, 13.4, 17.7);
    String str = "0.0,1.2,3.4,15.3,13.4,17.7";
    EnvelopeND actual = CSVEnvelopeDecoder.instance.apply(str, new EnvelopeND(new GeometryFactory(), 2));
    assertEquals(expected, actual);
  }
}