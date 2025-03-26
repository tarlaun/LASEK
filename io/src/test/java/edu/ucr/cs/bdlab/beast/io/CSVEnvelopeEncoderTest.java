package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;

public class CSVEnvelopeEncoderTest extends JavaSpatialSparkTest {

  public void testEncode2DEnvelope() {
    IFeature feature = new Feature(new Object[] {
        new EnvelopeND(GeometryReader.DefaultGeometryFactory,2, 1.0, 2.0, 5.0, 3.0), "abc", "def"},
        null);
    CSVEnvelopeEncoder encoder = new CSVEnvelopeEncoder(',', new int[] {1,2,4,5});
    assertArrayEquals(new int[] {-1,0,1,-1,2,3}, encoder.orderedColumns);
    String encoded = encoder.apply(feature, new StringBuilder()).toString();
    assertEquals("abc,1.0,2.0,def,5.0,3.0", encoded);
  }
}