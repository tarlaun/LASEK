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
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class MultilevelPlotTest extends JavaSparkTest {

  public void testCreateImageTiles() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("vflip", false)
        .set(SpatialWriter.OutputFormat(), "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    ZipFile zipFile = new ZipFile(outputFile);
    for (String expectedFile : expectedFiles) {
      assertNotNull(String.format("Expected tile '%s' not found", expectedFile), zipFile.getEntry(expectedFile));
    }

    // Make sure that there are no extra tiles were created
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int count = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        count++;
    }
    assertEquals(expectedFiles.length, count);
    // Read the configuration and verify that the number of levels is correct
    ZipEntry entry = zipFile.getEntry("_visualization.properties");
    InputStream in = zipFile.getInputStream(entry);
    BeastOptions opts2 = new BeastOptions().loadFromTextFile(in);
    assertEquals(3, opts2.getInt("levels", 0));
  }

  public void testPlotGeometries() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/viztest.point", inputFile);
    BeastOptions opts = new BeastOptions(false);
    opts.set(CommonVisualizationHelper.VerticalFlip, false);
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    JavaRDD<IFeature> features = SpatialReader.readInput(javaSparkContext(), opts,
        inputFile.toString(), "point");
    JavaRDD<Geometry> geoms = features.map(IFeature::getGeometry);
    opts.set(MultilevelPlot.ImageTileThreshold(), 0);
    MultilevelPlot.plotGeometries(geoms, 0, 2, outputFile.toString(), opts);

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    ZipFile zipFile = new ZipFile(outputFile);
    for (String expectedFile : expectedFiles) {
      assertNotNull(String.format("Expected tile '%s' not found", expectedFile), zipFile.getEntry(expectedFile));
    }

    // Make sure that there are no extra tiles were created
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int count = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        count++;
    }
    assertEquals(expectedFiles.length, count);
  }

  public void testCreateImageTilesWithPartialHistogram() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/viztest.point", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("vflip", false)
        .set(MultilevelPlot.MaximumHistogramSize(), 4 * 8)
        .set(SpatialWriter.OutputFormat(), "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-3-1.png",
        "tile-2-2-3.png",
    };
    ZipFile zipFile = new ZipFile(outputFile);
    for (String expectedFile : expectedFiles) {
      assertNotNull(String.format("Expected tile '%s' not found", expectedFile), zipFile.getEntry(expectedFile));
    }

    // Make sure that there are no extra tiles were created
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int count = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        count++;
    }
    assertEquals(expectedFiles.length, count);
  }

  public void testMercatorProjection() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/test-mercator.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("vflip", false)
        .set("keep-ratio", true)
        .set("mercator", true)
        .set(SpatialWriter.OutputFormat(), "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    // Make sure that there are no extra tiles were created
    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int count = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        count++;
    }
    assertEquals(21, count);
  }

  public void testMercatorProjectionShouldEnableVFlipByDefault() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/test-mercator2.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("mercator", true)
        .set("data-tiles", false)
        .set(SpatialWriter.OutputFormat(), "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-0.png",
        "tile-2-0-0.png",
        "tile-2-1-0.png",
        "tile-2-2-0.png",
        "tile-2-3-0.png",
        "tile-2-0-1.png",
        "tile-2-1-1.png",
        "tile-2-2-1.png",
        "tile-2-3-1.png",
    };

    // Make sure that there are no extra tiles were created
    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int count = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        count++;
    }
    assertEquals(expectedFiles.length, count);
    // Read the configuration and verify that the number of levels is correct
    ZipEntry entry = zipFile.getEntry("_visualization.properties");
    InputStream in = zipFile.getInputStream(entry);
    BeastOptions opts2 = new BeastOptions().loadFromTextFile(in);
    assertTrue("Mercator should be set", opts2.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false));
  }

  public void testMercatorProjectionShouldHandleOutOfBoundsObjects() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/test-mercator3.points", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("mercator", true)
        .set("pointsize",  0)
        .set(SpatialWriter.OutputFormat(), "point");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    // Make sure that there are no extra tiles were created
    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int numTiles = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        numTiles++;
    }
    assertEquals(3, numTiles);
  }

  public void testKeepRatio() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.rect");
    File outputFile = new File(scratchPath().toString(), "out.zip");
    copyResource("/viztest.rect", inputFile);

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "envelopek(2)")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("vflip", false)
        .set("keep-ratio", true)
        .set("pointsize", 0)
        .set(SpatialWriter.OutputFormat(), "envelopek(2)");
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-1-1.png",
        "tile-2-0-1.png",
        "tile-2-3-2.png",
    };
    // Make sure that there are no extra tiles were created
    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int numTiles = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        numTiles++;
    }
    assertEquals(expectedFiles.length, numTiles);
  }

  public void testCreateImageTilesWithBuffer() throws IOException {
    File inputFile = new File(scratchPath().toString(), "in.point");
    File outputFile = new File(scratchPath().toString(), "out.zip");

    PrintWriter infile = new PrintWriter(new FileOutputStream(inputFile));

    try {
      infile.println("0,0");
      infile.println("2,2");
      infile.println("4,4");
    } finally {
      infile.close();
    }

    BeastOptions opts = new BeastOptions(false)
        .set("plotter", "gplot")
        .set("iformat", "point")
        .set("separator", ",")
        .set("levels", 3)
        .set("threshold", 0)
        .set("vflip", false)
        .set("pointsize", 5);
    MultilevelPlot.run(opts, new String[] {inputFile.getPath()}, new String[] {outputFile.getPath()}, sparkContext());

    String[] expectedFiles = {
        "tile-0-0-0.png",
        "tile-1-0-0.png",
        "tile-1-0-1.png",
        "tile-1-1-0.png",
        "tile-1-1-1.png",
        "tile-2-0-0.png",
        "tile-2-1-1.png",
        "tile-2-1-2.png",
        "tile-2-2-1.png",
        "tile-2-2-2.png",
        "tile-2-3-3.png",
    };
    // Make sure that there are no extra tiles were created
    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    int numTiles = 0;
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().startsWith("tile-"))
        numTiles++;
    }
    assertEquals(expectedFiles.length, numTiles);
  }
}
