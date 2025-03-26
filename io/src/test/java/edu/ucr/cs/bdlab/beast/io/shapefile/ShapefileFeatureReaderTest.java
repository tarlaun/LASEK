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

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapefileFeatureReaderTest extends JavaSpatialSparkTest {

  public void testReadFile() throws IOException {
    // Copy the required files
    String dbfFile = "/usa-major-cities/usa-major-cities.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    String shpFile = "/usa-major-cities/usa-major-cities.shp";
    Path shpPath = new Path(scratchPath(), "temp.shp");
    copyResource(shpFile, new File(shpPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(shpPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadNonCompressedShapeFileWithEmptyTrail() throws IOException {
    // Copy the required files
    File inputPath = makeResourceCopy("/linetest");
    Path shpPath = new Path(inputPath.getPath(), "linetest.shp");

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(shpPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadCompressedSparseFile() throws IOException {
    // Copy the required file
    String zipFile = "/sparselines.zip";
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource(zipFile, new File(zipPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadCompressedFileWithUpperCaseExtension() throws IOException {
    // Copy the required file
    File input = makeResourceCopy("/linetest2.zip");

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(new Path(input.getPath()), new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testIterableInterface() throws IOException {
    // Copy the required files
    String dbfFile = "/usa-major-cities/usa-major-cities.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    String shpFile = "/usa-major-cities/usa-major-cities.shp";
    Path shpPath = new Path(scratchPath(), "temp.shp");
    copyResource(shpFile, new File(shpPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(shpPath, new BeastOptions());
      int recordCount = 0;
      for (IFeature f : reader) {
        assertNotNull(f);
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadCompressedZipFile() throws IOException {
    // Copy the required file
    String zipFile = "/usa-major-cities.zip";
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource(zipFile, new File(zipPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadCompressedZipFileWithMultipleShapefiles() throws IOException {
    // Copy the required file
    String zipFile = "/points.zip";
    Path zipPath = new Path(scratchPath(), "points.zip");
    copyResource(zipFile, new File(zipPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(6, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testFilter() throws IOException {
    // Copy the required files
    String dbfFile = "/usa-major-cities/usa-major-cities.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    String shpFile = "/usa-major-cities/usa-major-cities.shp";
    Path shpPath = new Path(scratchPath(), "temp.shp");
    copyResource(shpFile, new File(shpPath.toString()));

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.FilterMBR(), "-160,18,-140,64");
      reader.initialize(shpPath, conf);
      int recordCount = 0;
      ArrayList<String> names = new ArrayList<>();
      names.add("Anchorage");
      names.add("Honolulu");
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        String name = f.getAs("NAME");
        assertTrue("Unexpected result " + name, names.remove(name));
        recordCount++;
      }
      assertTrue(names.isEmpty());
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadImmutableFile() throws IOException {
    // Copy the required files
    String dbfFile = "/usa-major-cities/usa-major-cities.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    String shpFile = "/usa-major-cities/usa-major-cities.shp";
    Path shpPath = new Path(scratchPath(), "temp.shp");
    copyResource(shpFile, new File(shpPath.toString()));
    List<IFeature> features = new ArrayList<>();

    ShapefileFeatureReader reader = new ShapefileFeatureReader();
    try {
      reader.initialize(shpPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        features.add(f);
        assertNotNull(f.get(1));
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }
}