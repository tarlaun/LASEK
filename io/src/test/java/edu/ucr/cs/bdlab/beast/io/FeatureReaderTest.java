package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;

public class FeatureReaderTest extends JavaSpatialSparkTest {

  public void testGetFileExtension() {
    String expectedExtension = ".geojson";
    String actualExtension = FeatureReader.getFileExtension("geojson");
    assertEquals(expectedExtension, actualExtension);
  }

}