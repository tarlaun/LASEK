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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import scala.collection.Map;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GeoJSONFeatureReaderTest extends JavaSpatialSparkTest {
  public void testReadPoints() throws IOException {
    Path inFile = new Path(scratchPath(), "point.geojson");
    copyResource("/point.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
        IFeature f = reader.getCurrentValue();
        assertEquals(GeometryType.POINT.typename, f.getGeometry().getGeometryType());
        Coordinate c = f.getGeometry().getCoordinate();
        assertEquals(100.0, c.getX(), 1E-5);
        assertEquals(0.0, c.getY(), 1E-5);
        assertEquals("value0", f.getAs("prop0"));
        assertEquals("value1", f.getAs("prop1"));
      }
      assertEquals(2, count);
    } finally {
      reader.close();
    }
  }

  public void testReadPointsWithIntegerCoordinates() throws IOException {
    Path inFile = new Path(scratchPath(), "ipoint.geojson");
    copyResource("/ipoint.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
        IFeature f = reader.getCurrentValue();
        assertEquals(GeometryType.POINT.typename, f.getGeometry().getGeometryType());
        Coordinate c = f.getGeometry().getCoordinate();
        assertEquals(100.0, c.getX(), 1E-5);
        assertEquals(0.0, c.getY(), 1E-5);
      }
      assertEquals(2, count);
    } finally {
      reader.close();
    }
  }

  public void testReadFeatures() throws IOException {
    Path inFile = new Path(scratchPath(), "features.geojson");
    copyResource("/features.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      // Read first feature
      assertTrue(reader.nextKeyValue());
      IFeature f = reader.getCurrentValue();
      assertEquals(GeometryType.LINESTRING.typename, f.getGeometry().getGeometryType());
      LineString lineString = (LineString) f.getGeometry();
      assertEquals(4, lineString.getNumPoints());
      assertEquals("id0", f.getAs("id"));
      // Read second feature
      assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      assertEquals(GeometryType.POLYGON.typename, f.getGeometry().getGeometryType());
      Polygon polygon = (Polygon) f.getGeometry();
      assertEquals(5, polygon.getNumPoints());
      assertEquals("id1", f.getAs("id"));
      // Assert no more features
      assertFalse(reader.nextKeyValue());
    } finally {
      reader.close();
    }
  }

  public void testResetLinestrings() throws IOException {
    Path inFile = new Path(scratchPath(), "linestrings.geojson");
    copyResource("/linestrings.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      // Read first feature
      assertTrue(reader.nextKeyValue());
      IFeature f = reader.getCurrentValue();
      assertEquals(GeometryType.LINESTRING.typename, f.getGeometry().getGeometryType());
      LineString lineString = (LineString) f.getGeometry();
      assertEquals(4, lineString.getNumPoints());
      assertEquals("id0", f.getAs("id"));
      // Read second feature
      assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      assertEquals(GeometryType.LINESTRING.typename, f.getGeometry().getGeometryType());
      lineString = (LineString) f.getGeometry();
      assertEquals(3, lineString.getNumPoints());
      assertEquals("id1", f.getAs("id"));
      // Assert no more features
      assertFalse(reader.nextKeyValue());
    } finally {
      reader.close();
    }
  }

  public void testReadAllFeatureTypes() throws IOException {
    Path inFile = new Path(scratchPath(), "allfeatures.geojson");
    copyResource("/allfeatures.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }
      assertEquals(7, count);
    } finally {
      reader.close();
    }
  }

  public void testReadCompressedFile() throws IOException {
    Path inFile = new Path(scratchPath(), "allfeatures.geojson.bz2");
    copyResource("/allfeatures.geojson.bz2", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(inFile, new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }
      assertEquals(7, count);
    } finally {
      reader.close();
    }
  }

  public void testReadBigCompressedFile() throws IOException, InterruptedException {
    Path inFile = new Path(scratchPath(), "allfeatures.geojson.bz2");
    BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(new FileOutputStream(inFile.toString()));
    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    writer.initialize(out, new Configuration());
    int numPoints = 10000;
    Random random = new Random(0);
    for (int $i = 0; $i < numPoints; $i++) {
      PointND p = new PointND(new GeometryFactory(), 2);
      p.setCoordinate(0, random.nextDouble());
      p.setCoordinate(1, random.nextDouble());
      Feature f = Feature.create(null, p);
      writer.write(f);
    }
    writer.close();

    // Now read the file in two splits
    long fileLength = new File(inFile.toString()).length();
    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    random = new Random(0);
    try {
      int count = 0;
      reader.initialize(inFile, 0, fileLength / 2, new BeastOptions());
      while (reader.nextKeyValue()) {
        Coordinate c = reader.getCurrentValue().getGeometry().getCoordinate();
        assertEquals(random.nextDouble(), c.getX(), 1E-5);
        assertEquals(random.nextDouble(), c.getY(), 1E-5);
        count++;
      }
      reader.close();
      reader.initialize(inFile, fileLength / 2, fileLength - (fileLength / 2), new BeastOptions());
      while (reader.nextKeyValue()) {
        Coordinate c = reader.getCurrentValue().getGeometry().getCoordinate();
        assertEquals(random.nextDouble(), c.getX(), 1E-5);
        assertEquals(random.nextDouble(), c.getY(), 1E-5);
        count++;
      }
      reader.close();
      assertEquals(numPoints, count);
    } finally {
      reader.close();
    }
  }

  public void testReadSmallGZCompressedFile() throws IOException {
    File inFile = makeResourceCopy("/MSBuilding.geojson.gz");

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(new Path(inFile.getPath()), new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }
      assertEquals(1, count);
    } finally {
      reader.close();
    }
  }

  public void testReadASplitThatEndsWithAStartObjectToken() throws IOException {
    // In this test, the split ends exactly at the START_OBJECT token "{" of a feature and it should read it.
    Path inFile = new Path(scratchPath(), "features.geojson");
    copyResource("/features.geojson", new File(inFile.toString()));

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    BeastOptions conf = new BeastOptions();
    // Try all split points around the start of the first object
    int length = (int) inFile.getFileSystem(conf.loadIntoHadoopConf(null)).getFileStatus(inFile).getLen();
    for (int end = 50; end < 70; end++) {
      int count = 0;
      // Read first split
      reader.initialize(inFile, 0, end, conf);
      try {
        while (reader.nextKeyValue())
          count++;
      } finally {
        reader.close();
      }

      // Read second split
      reader.initialize(inFile, end, length - end, conf);
      try {
        while (reader.nextKeyValue())
          count++;
      } finally {
        reader.close();
      }
      assertEquals(2, count);
    }
  }

  public void testReadThreeSplits() throws IOException {
    Path input = new Path(scratchPath(), "input.geojson");
    BeastOptions conf = new BeastOptions();
    FileSystem fs = input.getFileSystem(conf.loadIntoHadoopConf(null));
    FSDataOutputStream out = fs.create(input);
    PrintStream ps = new PrintStream(out);
    ps.println("{\"type\":\"FeatureCollection\",\"features\":[");
    int expectedNumRecords = 4;
    for (int i = 1; i <  expectedNumRecords; i++) {
      ps.println("{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-70,453],[-75,51],[150,30],[-70,453]]]},\"properties\":{}},");
    }
    ps.println("{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-70,453],[-75,51],[150,30],[-70,453]]]},\"properties\":{}}");
    ps.println("]}");
    ps.close();

    // Read the file back and ensure it reads the correct number of records
    long fileLength = fs.getFileStatus(input).getLen();
    List<FileSplit> splits = new ArrayList<>();
    long start = 0;
    while (start < fileLength) {
      long splitLength = Math.min(fileLength / 3, fileLength - start);
      splits.add(new FileSplit(input, start, splitLength, null));
      start += splitLength;
    }
    // Read all the splits back and count number of records
    int actualNumRecords = 0;
    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    for (FileSplit split : splits) {
      reader.initialize(split, conf);
      while (reader.nextKeyValue()) {
        actualNumRecords++;
      }
      reader.close();
    }
    assertEquals(expectedNumRecords, actualNumRecords);
  }

  public void testReadComplexAttributes() throws IOException {
    File file = makeFileCopy("/properties.geojson");
    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(new Path(file.getPath()), new BeastOptions());
    try {
      assertTrue(reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      Object tags = feature.getAs("tags");
      assertNotNull(tags);
      assertTrue(scala.collection.Map.class.isAssignableFrom(tags.getClass()));
      scala.collection.Map tagsMaps = (Map) tags;
      assertEquals(2, tagsMaps.size());
      Object labels = feature.getAs("labels");
      assertTrue(scala.collection.Seq.class.isAssignableFrom(labels.getClass()));
      List labelsList = feature.getList(2);
      assertEquals(2, labelsList.size());
      // Assert that the type of the labels list is Array[String]
      DataType labelsType = feature.schema().apply("labels").dataType();
      assertTrue(labelsType instanceof ArrayType);
      assertEquals(DataTypes.StringType, ((ArrayType)labelsType).elementType());

      DataType valuesType = feature.schema().apply("values").dataType();
      assertEquals(MapType.class, valuesType.getClass());
      MapType valuesMapType = (MapType)valuesType;
      assertEquals(DataTypes.StringType, valuesMapType.keyType());
      assertEquals(DataTypes.DoubleType, valuesMapType.valueType());
      Object values = feature.getAs("values");
      assertTrue(scala.collection.Map.class.isAssignableFrom(values.getClass()));
      java.util.Map<String, Double> valuesMap = feature.getJavaMap(3);
      assertEquals(2, valuesMap.size());
      assertEquals(100.0, valuesMap.get("area"));

      // Check an attribute of type array of numeric values
      DataType locationType = feature.schema().apply("location").dataType();
      assertTrue(locationType instanceof ArrayType);
      assertEquals(DataTypes.DoubleType, ((ArrayType)locationType).elementType());

      DataType otherType = feature.schema().apply("other").dataType();
      assertTrue(otherType instanceof NullType);
    } finally {
      reader.close();
    }
  }

  public void testReadCorruptedFile() throws IOException {
    File infile = makeResourceCopy("/exampleinput.geojson");

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(new Path(infile.getPath()), new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }
      assertEquals(1, count);
    } finally {
      reader.close();
    }
  }

  public void testReadGeoJSONLineFormatWithFeatureAtTheEnd() throws IOException {
    File infile = makeResourceCopy("/features.geojsonl");

    GeoJSONFeatureReader reader = new GeoJSONFeatureReader();
    reader.initialize(new Path(infile.getPath()), new BeastOptions());
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        assertNotNull(reader.getCurrentValue());
        count++;
      }
      assertEquals(3, count);
    } finally {
      reader.close();
    }
  }

}