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
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShapefileGeometryReaderTest extends JavaSpatialSparkTest {

  public void testReadHeader() throws IOException {
    DataInputStream in = new DataInputStream(getClass().getResourceAsStream("/usa-major-cities/usa-major-cities.shp"));
    ShapefileHeader header = new ShapefileHeader();
    header.readFields(in);
    in.close();

    assertEquals(1000, header.version);
    assertEquals(3460 / 2, header.fileLength);
    assertEquals(1, header.shapeType);
    assertEquals(-157.8, header.xmin, 1E-1);
    assertEquals(-69.8, header.xmax, 1E-1);
  }

  public void testReadPoints() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.shp");
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue() instanceof Point);
        assertEquals(2, GeometryHelper.getCoordinateDimension(reader.getCurrentValue()));
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadFromZipFile() throws IOException {
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource("/usa-major-cities.zip", new File(zipPath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue() instanceof Point);
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadSparseFiles() throws IOException {
    // This test reads a file that has gaps in the .shp file that can only be detected through the .shx file
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource("/sparselines.zip", new File(zipPath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue() instanceof MultiLineString ||
            reader.getCurrentValue() instanceof LineString);
        recordCount++;
      }
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadMultipoint() throws IOException {
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource("/multipoint.zip", new File(zipPath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertEquals(GeometryType.MULTIPOINT.typename, reader.getCurrentValue().getGeometryType());
        recordCount++;
      }
      assertEquals(3, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadLineMZ() throws IOException {
    Path zipPath = new Path(scratchPath(), "temp.zip");
    copyResource("/polylinemz.zip", new File(zipPath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(zipPath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue().getGeometryType().equalsIgnoreCase(GeometryType.MULTILINESTRING.typename) ||
            reader.getCurrentValue().getGeometryType().equalsIgnoreCase(GeometryType.LINESTRING.typename));
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }


  public void testReadLines() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.shp");
    copyResource("/usa-major-highways/usa-major-highways.shp", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue() instanceof MultiLineString || reader.getCurrentValue() instanceof LineString);
        recordCount++;
      }
      assertEquals(233, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadPolygons() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/tl_2017_44_elsd.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue().getGeometryType().equals(GeometryType.MultiPolygonName) ||
            reader.getCurrentValue().getGeometryType().equals(GeometryType.PolygonName));
        recordCount++;
      }
      assertEquals(5, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadPolygonsMZ() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/polygonmz.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue().getGeometryType().equals("MultiPolygon") ||
            reader.getCurrentValue().getGeometryType().equals("Polygon"));
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testFilter() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.shp");
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.FilterMBR(), "-160,18,-140,64");
      reader.initialize(shapefilePath, conf);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        recordCount++;
      }
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testFilterPolygons() throws IOException {
    File shapefilePath = makeFileCopy("/tl_2017_44_elsd.zip");
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.FilterMBR(), "-71.449,41.416,-71.286,41.632");
      reader.initialize(new Path(shapefilePath.getPath()), conf);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadPolygonsWithHoles() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/simpleshape.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue().getGeometryType().equals(GeometryType.MultiPolygonName) ||
            reader.getCurrentValue().getGeometryType().equals(GeometryType.PolygonName));
        recordCount++;
      }
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testBadPolygon() throws IOException {
    // Test a polygon that does not have the starting and ending points matching
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/badpolygon.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue().getGeometryType().equals(GeometryType.MULTIPOLYGON.typename) ||
            reader.getCurrentValue().getGeometryType().equals(GeometryType.POLYGON.typename));
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadLinesWithM() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "linetest.zip");
    copyResource("/linetest.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        Geometry geom = reader.getCurrentValue();
        assertTrue(geom.getGeometryType().equals(GeometryType.MultiLineStringName) ||
            geom.getGeometryType().equals(GeometryType.LineStringName));
        recordCount++;
        LineString l = (LineString) geom;
        assertEquals(4, l.getNumPoints());
        assertEquals(1.1, l.getCoordinateN(0).getM());
        assertEquals(2.2, l.getCoordinateN(1).getM());
        assertEquals(3.3, l.getCoordinateN(2).getM());
        assertEquals(4.4, l.getCoordinateN(3).getM());
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadMutableObjects() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.shp");
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    List<Geometry> geometries = new ArrayList<>();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        geometries.add(reader.getCurrentValue());
        assertTrue(reader.getCurrentValue() instanceof Point);
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }
  public void testReadImmutableObjects() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.shp");
    Path indexfilePath = new Path(scratchPath(), "temp.shx");
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(shapefilePath.toString()));
    copyResource("/usa-major-cities/usa-major-cities.shx", new File(indexfilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    List<Geometry> geometries = new ArrayList<>();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        geometries.add(reader.getCurrentValue());
        assertTrue(reader.getCurrentValue() instanceof Point);
        recordCount++;
      }
      assertEquals(120, recordCount);
      assertNotSame(geometries.get(0), geometries.get(1));
    } finally {
      reader.close();
    }
  }

  public void testReadFileWithEmptyGeometries() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/test_empty.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      reader.initialize(shapefilePath, new BeastOptions());
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        if (reader.iShape == 2)
          assertTrue(reader.getCurrentValue().isEmpty());
        else
          assertTrue(reader.getCurrentValue() instanceof Point);
        recordCount++;
      }
      assertEquals(2, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testSkipEmptyGeometriesWhenFiltering() throws IOException {
    Path shapefilePath = new Path(scratchPath(), "temp.zip");
    copyResource("/test_empty.zip", new File(shapefilePath.toString()));
    ShapefileGeometryReader reader = new ShapefileGeometryReader();
    try {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.FilterMBR(), "0,0,10,10");
      reader.initialize(shapefilePath, conf);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        assertTrue(reader.getCurrentValue() instanceof Point);
        recordCount++;
      }
      assertEquals(1, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testReadEmptyGeometries() throws IOException {
    GeometryFactory factory = new GeometryFactory();
    Geometry[] geometries = {
        factory.createPoint(),
        factory.createLineString(),
        factory.createPolygon(),
        factory.createMultiLineString(),
        factory.createMultiPolygon(),
        factory.createMultiPoint(),
    };
    for (Geometry expected : geometries) {
      File shpfile = new File(scratchDir(), "file.shp");
      ShapefileGeometryWriter writer = new ShapefileGeometryWriter();
      writer.initialize(new Path(shpfile.getPath()), new Configuration());
      writer.write(expected);
      writer.close();

      // Read it back
      ShapefileGeometryReader reader = new ShapefileGeometryReader();
      reader.initialize(new Path(shpfile.getPath()), new BeastOptions());
      assertTrue("Must have one record", reader.nextKeyValue());
      Geometry actual = reader.getCurrentValue();
      assertTrue("Geometry should be empty "+actual, actual.isEmpty());
      assertFalse(reader.nextKeyValue());
      reader.close();
    }
  }

}