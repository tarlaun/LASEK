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
package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeometricPlotterTest extends TestCase {

  public static GeometryFactory factory = new GeometryFactory();

  public static CoordinateSequence createCoordinateSequence(double ... coordinates) {
    int size = coordinates.length / 2;
    CoordinateSequence cs = factory.getCoordinateSequenceFactory().create(coordinates.length / 2, 2);
    for (int i = 0; i < size; i++) {
      cs.setOrdinate(i, 0, coordinates[2 * i]);
      cs.setOrdinate(i, 1, coordinates[2 * i + 1]);
    }
    return cs;
  }

  public static Polygon createPolygonJTS(CoordinateSequence ... rings) {
    LinearRing shell = factory.createLinearRing(rings[0]);
    LinearRing[] holes = new LinearRing[rings.length - 1];
    for (int $i = 1; $i < rings.length; $i++)
      holes[$i-1] = factory.createLinearRing(rings[$i]);
    return factory.createPolygon(shell, holes);
  }

  public void testPlotMultiPolygon() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1.0, 0.0, 1.0);
    BeastOptions opts = new BeastOptions();
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    MultiPolygon poly = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(createCoordinateSequence(
        0.0, 0.0,
        0.0, 0.5,
        0.1, 0.5,
        0.0, 0.0), createCoordinateSequence(
        0.5, 0.5,
        0.5, 0.0,
        0.4, 0.5,
        0.5, 0.5))});
    boolean changed = plotter.plot(canvas, Feature.create(null, poly));
    assertTrue(changed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // There should not be any set pixel in the column half way between 0 and 50
    int x = 25;
    for (int $y = 0; $y < 100; $y++) {
      int rgb = img.getRGB(x, $y);
      assertEquals(String.format("A pixel is incorrectly set at (%d, %d)", x, $y), 0, rgb);
    }
    // There should be exactly two pixels set in the first row
    int y = 0;
    int count = 0;
    for (int $x = 0; $x < 100; $x++) {
      int rgb = img.getRGB($x, y);
      if (rgb != 0)
        count++;
    }
    assertEquals(2, count);
  }

  public void testPlotPolygonWithoutHoles() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 3.0, 0.0, 3.0);
    BeastOptions opts = new BeastOptions().set("fill", "black");
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    Polygon poly = createPolygonJTS(createCoordinateSequence(
            0.0, 0.0,
            3.0, 0.0,
            2.9, 2.9,
            0.0, 3.0,
            0.0, 0.0));
    boolean changed = plotter.plot(canvas, Feature.create(null, poly));
    assertTrue(changed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));

    // The pixel at the middle should be transparent
    int alphaCenter = new Color(img.getRGB(50, 50), true).getAlpha();
    assertTrue("The center pixel should not be transparent", alphaCenter != 0);
    // A pixel at 25 should not be transparent
    int alpha25 = new Color(img.getRGB(25, 50), true).getAlpha();
    assertTrue("The 25% pixel should not be transparent", alpha25 != 0);
  }

  public void testPlotPolygonWithoutHolesWithOutOfBounds() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 3.0, 0.0, 3.0);
    BeastOptions opts = new BeastOptions().set("fill", "black");
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    Polygon poly = createPolygonJTS(createCoordinateSequence(
            0.0, 0.0,
            5.0, 0.0,
            4.9, 4.9,
            0.0, 5.0,
            0.0, 0.0));
    boolean changed = plotter.plot(canvas, Feature.create(null, poly));
    assertTrue(changed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));

    // The pixel at the middle should be transparent
    int alphaCenter = new Color(img.getRGB(50, 50), true).getAlpha();
    assertTrue("The center pixel should not be transparent", alphaCenter != 0);
    // A pixel at 25 should not be transparent
    int alpha25 = new Color(img.getRGB(25, 50), true).getAlpha();
    assertTrue("The 25% pixel should not be transparent", alpha25 != 0);
  }


  public void testPlotPolygonWithHoles() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 3.0, 0.0, 3.0);
    BeastOptions opts = new BeastOptions().set("fill", "black");
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    Polygon poly = createPolygonJTS(createCoordinateSequence(
            0.0, 0.0,
            3.0, 0.0,
            3.0, 3.0,
            0.0, 3.0,
            0.0, 0.0), createCoordinateSequence(
            1.0, 1.0,
            2.0, 1.0,
            2.0, 2.0,
            1.0, 2.0,
            1.0, 1.0));
    boolean changed = plotter.plot(canvas, Feature.create(null, poly));
    assertTrue(changed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));

    // The pixel at the middle should be transparent
    int alphaCenter = new Color(img.getRGB(50, 50), true).getAlpha();
    assertTrue("The center pixel should be transparent", alphaCenter == 0);
    // A pixel at 25 should not be transparent
    int alpha25 = new Color(img.getRGB(25, 50), true).getAlpha();
    assertTrue("The 25% pixel should not be transparent", alpha25 != 0);
  }

  public void testLinestring() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1.0, 0.0, 1.0);
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    LineString linestring = factory.createLineString(createCoordinateSequence(
        0.1, 0.1,
        0.2, 0.98,
        0.99, 0.99));
    plotter.plot(canvas, Feature.create(null, linestring));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // Count non-transparent pixels along the middle lines
    int count = 0;
    for (int $i = 0; $i < 100; $i++) {
      if (new Color(img.getRGB(50, $i), true).getAlpha() != 0)
        count++;
      if (new Color(img.getRGB($i, 50), true).getAlpha() != 0)
        count++;
    }
    assertEquals(2, count);
  }

  public void testPlotMultiPolygon2() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1.0, 0.0, 1.0);
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    MultiPolygon poly = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(createCoordinateSequence(
        0.01, 0.01,
        0.98, 0.02,
        0.99, 0.99,
        0.02, 0.98,
        0.01, 0.01))});
    plotter.plot(canvas, Feature.create(null, poly));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));

    // Count non-transparent pixels along the middle lines
    int count = 0;
    for (int $i = 0; $i < 100; $i++) {
      if (new Color(img.getRGB(50, $i), true).getAlpha() != 0)
        count++;
      if (new Color(img.getRGB($i, 50), true).getAlpha() != 0)
        count++;
    }
    assertEquals(4, count);
  }

  public void testLinestring2() throws ParseException, IOException {
    // Try both the fast plotter and the memory saving one.
    GeometricPlotter plotter = new GeometricPlotter();
    LineString linestring = (LineString) new WKTReader().read("LINESTRING(61 897,65 891)");
    Envelope mbr = linestring.getEnvelopeInternal();
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    int imageSize = 100;
    plotter.setup(opts);
    Canvas canvas = plotter.createCanvas(imageSize, imageSize, mbr, 0);
    plotter.plot(canvas, Feature.create(null, linestring));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // Count non-transparent pixels along the middle lines
    int count = 0;
    for (int $i = 0; $i < imageSize; $i++) {
      if (new Color(img.getRGB($i, imageSize/2), true).getAlpha() != 0)
        count++;
    }
    assertEquals(1, count);
  }

  public void testLinestringThatGoesOffCanvas() throws IOException, ParseException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1.0, 0.0, 1.0);
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    plotter.setup(opts);
    int imageSize = 100;
    Canvas canvas = plotter.createCanvas(imageSize, imageSize, mbr, 0);
    LineString linestring = factory.createLineString(createCoordinateSequence(
        0.1, 0.1,
        1.2, 0.1,
        1.2, 1.2,
        0.1, 1.2,
        0.1, 0.1));
    plotter.plot(canvas, Feature.create(null, linestring));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));

    // Count number of pixels
    int count = 0;
    for (int $x = 0; $x < imageSize; $x++) {
      for (int $y = 0; $y < imageSize; $y++) {
        int alpha = new Color(img.getRGB($x, $y), true).getAlpha();
        if (alpha != 0)
          count++;
      }
    }
    assertEquals(179, count);
  }
  public void testMerge() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1.0, 0.0, 1.0);
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    plotter.setup(opts);
    Canvas canvas1 = plotter.createCanvas(100, 100, mbr, 0);
    MultiPolygon poly = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(createCoordinateSequence(
        0.0, 0.0,
        0.0, 0.5,
        0.0001, 0.5,
        0.0, 0.0))});
    plotter.plot(canvas1, Feature.create(null, poly));

    Canvas canvas2 = plotter.createCanvas(100, 100, mbr, 0);
    poly = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(createCoordinateSequence(
        0.5, 0.5,
        0.5, 0.0,
        0.50001, 0.0,
        0.5, 0.5))});
    plotter.plot(canvas2, Feature.create(null, poly));

    plotter.merge(canvas1, canvas2);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas1, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // There should be exactly two pixels set in the first row
    int y = 0;
    int count = 0;
    for (int $x = 0; $x < 100; $x++) {
      int rgb = img.getRGB($x, y);
      if (rgb != 0)
        count++;
    }
    assertEquals(2, count);
  }

  public void testPlotMultiPolygonWithManyPoints() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    Envelope mbr = new Envelope( 0.0, 1000.0, 0.0, 1000.0);
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false);
    plotter.setup(opts);
    int imageSize = 1000;
    Canvas canvas = plotter.createCanvas(imageSize, imageSize, mbr, 0);
    List<Coordinate> coords = new ArrayList<>();
    for (int $i = 0; $i < 750; $i++)
      coords.add(new CoordinateXY($i, 0.0));
    coords.add(new CoordinateXY(749.0, 10));
    coords.add(new CoordinateXY(0.0, 10));
    coords.add(new CoordinateXY(0.0, 0.0));
    CoordinateSequence ring1 = factory.getCoordinateSequenceFactory().create(coords.toArray(new Coordinate[0]));
    coords.clear();
    // Add second polygon
    for (int $i = 0; $i < 7500; $i++)
      coords.add(new CoordinateXY($i/10.0, 50.0));
    coords.add(new CoordinateXY(749.0, 60));
    coords.add(new CoordinateXY(0.0, 60));
    coords.add(new CoordinateXY(0.0, 50.0));
    CoordinateSequence ring2 = factory.getCoordinateSequenceFactory().create(coords.toArray(new Coordinate[0]));
    MultiPolygon poly = factory.createMultiPolygon(new Polygon[] {createPolygonJTS(ring1, ring2)});

    plotter.plot(canvas, Feature.create(null, poly));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plotter.writeImage(canvas, dos, false);
    dos.close();
    BufferedImage img = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    // Count number of black pixels
    int count = 0;
    for (int $x = 0; $x < imageSize; $x++) {
      for (int $y = 0; $y < imageSize; $y++) {
        int alpha = new Color(img.getRGB($x, $y), true).getAlpha();
        if (alpha != 0)
          count++;
      }
    }
    assertEquals((750 + 9) * 2 * 2, count);
  }

  public void testPlotPointsAsPixels() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    // Set up a plotter with five-pixel point size
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false).set("pointsize", 5);
    plotter.setup(opts);

    Envelope mbr = new Envelope( 0.0, 256.0, 0.0, 256.0);
    Canvas canvas = plotter.createCanvas(256, 256, mbr, 0);
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 5.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 6.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 7.0, 5.0));

    int countBlackPixels = 0;
    int blackPixel = new Color(0, 0, 0, 255).getRGB();
    BufferedImage image = getImage(plotter, canvas);
    for (int x = 0; x < image.getWidth(); x++) {
      for (int y = 0; y < image.getHeight(); y++) {
        if (image.getRGB(x, y) == blackPixel)
          countBlackPixels++;
      }
    }
    assertEquals(3, countBlackPixels);
  }

  public void testPlotPointsAsPixelsWithPointSizeZero() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    // Set up a plotter with five-pixel point size
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.Antialiasing, false).set(GeometricPlotter.PointSize, 0);
    plotter.setup(opts);

    Envelope mbr = new Envelope( 0.0, 256.0, 0.0, 256.0);
    Canvas canvas = plotter.createCanvas(256, 256, mbr, 0);
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 5.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 17.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 29.0, 5.0));

    int countBlackPixels = 0;
    int blackPixel = new Color(0, 0, 0, 255).getRGB();
    BufferedImage image = getImage(plotter, canvas);
    for (int x = 0; x < image.getWidth(); x++) {
      for (int y = 0; y < image.getHeight(); y++) {
        if (image.getRGB(x, y) == blackPixel)
          countBlackPixels++;
      }
    }
    assertEquals(3, countBlackPixels);
  }

  public void testPlotPointsAsCircles() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    // Set up a plotter with five-pixel point size
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.PointSize, 5);
    plotter.setup(opts);

    Envelope mbr = new Envelope( 0.0, 256.0, 0.0, 256.0);
    Canvas canvas = plotter.createCanvas(256, 256, mbr, 0);
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 5.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 17.0, 5.0));
    plotter.plotGeometry(canvas, new PointND(new GeometryFactory(), 2, 29.0, 5.0));

    int countBlackPixels = 0;
    int blackPixel = new Color(0, 0, 0, 255).getRGB();
    BufferedImage image = getImage(plotter, canvas);
    for (int x = 0; x < image.getWidth(); x++) {
      for (int y = 0; y < image.getHeight(); y++) {
        if (image.getRGB(x, y) == blackPixel)
          countBlackPixels++;
      }
    }
    assertTrue(String.format("Too few black pixels %d", countBlackPixels), countBlackPixels >= 13 * 3);
  }

  public void testPlotPointsAsCirclesWithMerge() throws IOException {
    GeometricPlotter plotter = new GeometricPlotter();
    // Set up a plotter with five-pixel point size
    BeastOptions opts = new BeastOptions().set(GeometricPlotter.PointSize, 5);
    plotter.setup(opts);

    Envelope mbr = new Envelope( 0.0, 256.0, 0.0, 256.0);
    Canvas canvas1 = plotter.createCanvas(256, 256, mbr, 0);
    plotter.plotGeometry(canvas1, new PointND(new GeometryFactory(), 2, 5.0, 5.0));
    plotter.plotGeometry(canvas1, new PointND(new GeometryFactory(), 2, 17.0, 5.0));
    plotter.plotGeometry(canvas1, new PointND(new GeometryFactory(), 2, 29.0, 5.0));

    Canvas canvas2 = plotter.createCanvas(256, 256, mbr, 0);
    plotter.plotGeometry(canvas2, new PointND(new GeometryFactory(), 2, 5.0, 15.0));
    plotter.plotGeometry(canvas2, new PointND(new GeometryFactory(), 2, 17.0, 15.0));
    plotter.plotGeometry(canvas2, new PointND(new GeometryFactory(), 2, 29.0, 15.0));

    canvas1 = plotter.merge(canvas1, canvas2);

    int blackPixel = new Color(0, 0, 0, 255).getRGB();
    int countBlackPixels = 0;
    BufferedImage image = getImage(plotter, canvas1);
    for (int x = 0; x < image.getWidth(); x++) {
      for (int y = 0; y < image.getHeight(); y++) {
        if (image.getRGB(x, y) == blackPixel)
          countBlackPixels++;
      }
    }
    assertTrue(String.format("Too few black pixels %d", countBlackPixels), countBlackPixels >= 13 * 6);
  }

  public void testPlotPointsWithBuffer() throws IOException {
    Envelope mbr = new Envelope( 0.0, 100.0, 100.0, 200.0);
    int imageSize = 100;
    int pointsize = 5;
    GeometricPlotter plotter = new GeometricPlotter();
    BeastOptions opts = new BeastOptions();
    opts.set("width", imageSize);
    opts.set("height", imageSize);
    opts.set(GeometricPlotter.PointSize, pointsize);
    plotter.setup(opts);

    Canvas canvas = plotter.createCanvas(100, 100, mbr, 0);
    plotter.plot(canvas, Feature.create(null, new PointND(new GeometryFactory(), 2, 0.0, 100.0)));
    plotter.plot(canvas, Feature.create(null, new PointND(new GeometryFactory(), 2, 2.0, 102.0)));
    plotter.plot(canvas, Feature.create(null, new PointND(new GeometryFactory(), 2, 50.0, 150.0)));
    plotter.plot(canvas, Feature.create(null, new PointND(new GeometryFactory(), 2, 20.0, 120.0)));
    plotter.plot(canvas, Feature.create(null, new PointND(new GeometryFactory(), 2, 20.0, 120.0)));

    BufferedImage img = getImage(plotter, canvas);
    assertEquals(100, img.getWidth());
    assertEquals(100, img.getHeight());
    assertTrue(isPixelOpaque(img, 0, 0));
    assertTrue(isPixelOpaque(img, 2, 2));
    assertFalse(isPixelOpaque(img, 1, 1));
    assertTrue(isPixelOpaque(img, 50, 50));
    assertTrue(isPixelOpaque(img, 49, 49));
    assertTrue(isPixelOpaque(img, 51, 51));

    assertTrue(isPixelOpaque(img, 20, 20));
    assertTrue(isPixelOpaque(img, 21, 21));
    assertTrue(isPixelOpaque(img, 19, 19));
  }

  public void testPlotPointsWithBufferAndMerge() throws IOException {
    Envelope mbr = new Envelope( 0.0, 100.0, 100.0, 200.0);
    int imageSize = 100;
    int pointsize = 5;
    GeometricPlotter plotter = new GeometricPlotter();
    BeastOptions opts = new BeastOptions();
    opts.set("width", imageSize);
    opts.set("height", imageSize);
    opts.set(GeometricPlotter.PointSize, pointsize);
    plotter.setup(opts);

    Canvas canvas1 = plotter.createCanvas(100, 100, mbr, 0);
    Canvas canvas2 = plotter.createCanvas(100, 100, mbr, 0);
    plotter.plot(canvas1, Feature.create(null, new PointND(new GeometryFactory(), 2, 0.0, 100.0)));
    plotter.plot(canvas2, Feature.create(null, new PointND(new GeometryFactory(), 2, 2.0, 102.0)));
    plotter.plot(canvas2, Feature.create(null, new PointND(new GeometryFactory(), 2, 50.0, 150.0)));
    plotter.plot(canvas1, Feature.create(null, new PointND(new GeometryFactory(), 2, 20.0, 120.0)));
    plotter.plot(canvas2, Feature.create(null, new PointND(new GeometryFactory(), 2, 20.0, 120.0)));

    plotter.merge(canvas1, canvas2);

    BufferedImage img = getImage(plotter, canvas1);
    assertEquals(100, img.getWidth());
    assertEquals(100, img.getHeight());
    assertTrue(isPixelOpaque(img, 0, 0));
    assertTrue(isPixelOpaque(img, 2, 2));
    assertFalse(isPixelOpaque(img, 1, 1));
    assertTrue(isPixelOpaque(img, 50, 50));
    assertTrue(isPixelOpaque(img, 49, 49));
    assertTrue(isPixelOpaque(img, 51, 51));

    assertTrue(isPixelOpaque(img, 20, 20));
    assertTrue(isPixelOpaque(img, 21, 21));
    assertTrue(isPixelOpaque(img, 19, 19));
  }

  protected BufferedImage getImage(Plotter plotter, Canvas canvas) throws IOException {
    ByteArrayOutputStream tempOut = new ByteArrayOutputStream();
    plotter.writeImage(canvas, tempOut, false);
    tempOut.close();
    return ImageIO.read(new ByteArrayInputStream(tempOut.toByteArray()));
  }

  public static boolean isPixelOpaque(BufferedImage img, int x,int y) {
    return new Color(img.getRGB(x, y), true).getAlpha() > 0;
  }
}