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
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriterTest;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

public class ShapefileGeometryWriterTest extends JavaSpatialSparkTest {

  public void testCreation() throws IOException {
    Geometry[] geometries = {
      new PointND(new GeometryFactory(), 2, 0.0,1.0),
      new PointND(new GeometryFactory(), 2, 3.0, 10.0)
    };

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
  }

  public void testWriteEnvelopes() throws IOException {
    Geometry[] geometries = {
        new EnvelopeND(new GeometryFactory(), 2, 0.0,1.0, 2.0, 3.0),
        new EnvelopeND(new GeometryFactory(), 2, 3.0, 10.0, 13.0, 13.0)
    };

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        EnvelopeND e = new EnvelopeND(new GeometryFactory()).merge(reader.getCurrentValue());
        assertEquals(geometries[count], e);
        count++;
      }
      assertEquals(geometries.length, count);
    } finally {
      reader.close();
    }
  }

  public void testContents() throws IOException {
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    Configuration conf = new Configuration();

    String[] shpFileNames = {"usa-major-cities", /*"usa-major-highways"*/};
    for (String shpFileName : shpFileNames) {
      Path outPath = new Path(scratchPath(), "test");
      Path shpPath = new Path(outPath, shpFileName + ".shp");
      Path shxPath = new Path(outPath, shpFileName + ".shx");
      FileSystem fileSystem = outPath.getFileSystem(conf);
      fileSystem.mkdirs(outPath);

      try {
        DataInputStream shpIn = new DataInputStream(getClass().getResourceAsStream("/" + shpFileName + "/" + shpFileName + ".shp"));
        reader.initialize(shpIn, new BeastOptions(conf));
        writer.initialize(shpPath, conf);

        while (reader.nextKeyValue()) {
          writer.write(reader.getCurrentValue());
        }
      } finally {
        reader.close();
        writer.close();
      }
      // Make sure that the output files are exactly the same as the input file
      InputStream expectedShp = getClass().getResourceAsStream("/" + shpFileName + "/" + shpFileName + ".shp");
      InputStream actualShp = fileSystem.open(shpPath);
      try {
        assertEquals(expectedShp, actualShp);
      } finally {
        expectedShp.close();
        actualShp.close();
      }
      InputStream expectedShx = getClass().getResourceAsStream("/" + shpFileName + "/" + shpFileName + ".shx");
      FSDataInputStream actualShx = fileSystem.open(shxPath);
      try {
        assertEquals(expectedShx, actualShx);
      } finally {
        expectedShx.close();
        actualShx.close();
      }
    }
  }

  public void testContentsForPolygons() throws IOException {
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    Configuration conf = new Configuration();

    String[] shpFileNames = {"simpleshape.zip"};
    for (String shpFileName : shpFileNames) {
      Path inZShp = new Path(scratchPath(), "temp.zip");
      copyResource("/"+shpFileName, new File(inZShp.toString()));
      Path outPath = new Path(scratchPath(), "test");
      Path shpPath = new Path(outPath, shpFileName + ".shp");
      Path shxPath = new Path(outPath, shpFileName + ".shx");
      FileSystem fileSystem = outPath.getFileSystem(conf);
      fileSystem.mkdirs(outPath);

      try {
        reader.initialize(inZShp, new BeastOptions(conf));
        writer.initialize(shpPath, conf);

        while (reader.nextKeyValue()) {
          writer.write(reader.getCurrentValue());
        }
      } finally {
        reader.close();
        writer.close();
      }
      // Make sure that the output files are exactly the same as the input file
      ZipFile zshp = new ZipFile(inZShp.toString());
      // Compare the shape index file as well
      InputStream expectedShx = zshp.getInputStream(zshp.getEntry("simpleshape.shx"));
      FSDataInputStream actualShx = fileSystem.open(shxPath);
      try {
        assertEquals(expectedShx, actualShx);
      } finally {
        expectedShx.close();
        actualShx.close();
      }

      // Compare the shapefile (.shp)
      InputStream expectedShp = zshp.getInputStream(zshp.getEntry("simpleshape.shp"));
      InputStream actualShp = fileSystem.open(shpPath);
      try {
        assertEquals(expectedShp, actualShp);
      } finally {
        expectedShp.close();
        actualShp.close();
        zshp.close();
      }
    }
  }

  public void testPolylines() throws IOException {
    List<Geometry> geometries = new ArrayList();
    LineString linestring = GeoJSONFeatureWriterTest.factory.createLineString(GeoJSONFeatureWriterTest.createCoordinateSequence(
        0, 0,
        2, 3));
    geometries.add(linestring);

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals(geometries.get(count), actualGeometry);
        count++;
      }
      assertEquals(geometries.size(), count);
    } finally {
      reader.close();
    }
  }

  public void testMultiPolygonsWithOnePolygon() throws IOException {
    List<Geometry> geometries = new ArrayList();
    Polygon polygon = GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        0, 0,
        2, 3,
        3, 0,
        0, 0));
    geometries.add(polygon.getFactory().createMultiPolygon(new Polygon[] {polygon}));

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals(geometries.get(count).getGeometryN(0), actualGeometry);
        count++;
      }
      assertEquals(geometries.size(), count);
    } finally {
      reader.close();
    }
  }

  public void testMultiPolygonsWithTwoPolygons() throws IOException {
    List<Geometry> geometries = new ArrayList();
    Polygon polygon1 = GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        0, 0,
        2, 3,
        3, 0,
        0, 0));
    Polygon polygon2 = GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        10, 0,
        12, 3,
        13, 0,
        10, 0));
    geometries.add(polygon1.getFactory().createMultiPolygon(new Polygon[] {polygon1, polygon2}));

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals(geometries.get(count), actualGeometry);
        count++;
      }
      assertEquals(geometries.size(), count);
    } finally {
      reader.close();
    }
  }

  public void testMultiPoint() throws IOException {
    List<Geometry> geometries = new ArrayList();
    MultiPoint multipoint = GeoJSONFeatureWriterTest.factory.createMultiPoint(new Point[]{
        GeoJSONFeatureWriterTest.factory.createPoint(new CoordinateXY(100, 20)),
        GeoJSONFeatureWriterTest.factory.createPoint(new CoordinateXY(120, 20)),
    });
    geometries.add(multipoint);

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals(geometries.get(count), actualGeometry);
        count++;
      }
      assertEquals(geometries.size(), count);
    } finally {
      reader.close();
    }
  }

  public void testPolygons() throws IOException {
    List<Geometry> geometries = new ArrayList();
    Polygon polygon = GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        0, 0,
        2, 3,
        3, 0,
        0, 0));
    geometries.add(polygon);

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        writer.write(geom);
      }
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals(geometries.get(count), actualGeometry);
        count++;
      }
      assertEquals(geometries.size(), count);
    } finally {
      reader.close();
    }
  }

  public void testGeometryCollection() throws IOException {
    GeometryCollection geometryCollection = GeoJSONFeatureWriterTest.factory.createGeometryCollection(new Geometry[] {
    GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        0, 0,
        2, 3,
        3, 0,
        0, 0)),
    GeoJSONFeatureWriterTest.createPolygon(GeoJSONFeatureWriterTest.createCoordinateSequence(
        10, 0,
        12, 3,
        13, 0,
        10, 0))});

    Configuration conf = new Configuration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    Path shxFileName = new Path(outPath, "test.shx");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
    try {
      writer.initialize(shpFileName, conf);
      writer.write(geometryCollection);
    } finally {
      writer.close();
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    // Read the file back
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    reader.initialize(shpFileName, new BeastOptions(conf));
    try {
      int count = 0;
      while (reader.nextKeyValue()) {
        Geometry actualGeometry = reader.getCurrentValue();
        assertEquals((geometryCollection.getGeometryN(count)), actualGeometry);
        count++;
      }
      assertEquals(geometryCollection.getNumGeometries(), count);
    } finally {
      reader.close();
    }
  }
}