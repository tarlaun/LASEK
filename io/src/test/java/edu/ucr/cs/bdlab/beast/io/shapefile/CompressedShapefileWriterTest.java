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
package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CompressedShapefileWriterTest extends JavaSpatialSparkTest {

  public void testCreationWithOnePart() throws IOException, InterruptedException {
    GeometryFactory factory = GeometryReader.DefaultGeometryFactory;
    int numEntries = 100;
    Feature[] features = new Feature[numEntries];
    Random random = new Random(0);
    for (int i = 0; i < numEntries; i++)
      features[i] = Feature.create(null, factory.createPoint(new CoordinateXY(random.nextFloat(), random.nextFloat())));

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test.zip");

    CompressedShapefileWriter writer = new CompressedShapefileWriter();
    try {
      writer.initialize(outPath, conf);
      for (Feature f : features)
        writer.write(f);
    } finally {
      writer.close();
    }

    FileSystem fileSystem = outPath.getFileSystem(conf);
    assertTrue("Shapefile not found", fileSystem.exists(outPath));
    // Open it as a ZIP file and ensure that it has one .shp file
    try (ZipFile zipFile = new ZipFile(outPath.toString())) {
      ZipEntry shpEntry = zipFile.getEntry("test.shp");
      assertNotNull(".shp entry not found", shpEntry);
    }
  }

  public void testCreationWithMultipleParts() throws IOException, InterruptedException {
    GeometryFactory factory = GeometryReader.DefaultGeometryFactory;
    int numEntries = 1000;
    Feature[] features = new Feature[numEntries];
    Random random = new Random(0);
    for (int i = 0; i < numEntries; i++)
      features[i] = Feature.create(null, factory.createPoint(new CoordinateXY(random.nextFloat(), random.nextFloat())));

    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setLong(CompressedShapefileWriter.PartSize, 1000);
    Path outPath = new Path(scratchPath(), "test.zip");

    CompressedShapefileWriter writer = new CompressedShapefileWriter();
    try {
      writer.initialize(outPath, conf);
      for (Feature f : features)
        writer.write(f);
    } finally {
      writer.close();
    }

    FileSystem fileSystem = outPath.getFileSystem(conf);
    assertTrue("Shapefile not found", fileSystem.exists(outPath));
    // Open it as a ZIP file and ensure that it has multiple .shp files
    int shpCount = 0;
    try (ZipFile zipFile = new ZipFile(outPath.toString())) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (FileUtil.extensionMatches(entry.getName(), ".shp"))
          shpCount++;
      }
    }
    assertTrue("The input file should contain more than one shapefile", shpCount > 1);
  }
}