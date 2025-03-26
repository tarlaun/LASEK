package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.test.JavaSparkTest;

public class PlotterTest extends JavaSparkTest {

  public void testGetExtension() {
    String expectedExtension = ".png";
    String actualExtension = Plotter.getImageExtension("gplot");
    assertEquals(expectedExtension, actualExtension);
  }

  public void testGetExtensionFromClass() {
    String expectedExtension = ".png";
    String actualExtension = Plotter.getImageExtension(GeometricPlotter.class);
    assertEquals(expectedExtension, actualExtension);
  }

}