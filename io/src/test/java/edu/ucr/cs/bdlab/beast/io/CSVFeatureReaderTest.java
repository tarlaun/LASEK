package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class CSVFeatureReaderTest extends JavaSpatialSparkTest {

  public void testImmutableReadWithHeader() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(1,2)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.getAs("id"));
      }
    }
  }

  public void testUseColumnNames() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(x,y)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.getAs("id"));
      }
    }
  }

  public void testIgnoreCaseHeaderline() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(X,Y)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertEquals("Point", f.getGeometry().getGeometryType());
        assertNotNull(f.getAs("id"));
      }
    }
  }

  public void testIgnoreCaseHeaderline2() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points_uppercaseheader.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(x,y)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertEquals("Point", f.getGeometry().getGeometryType());
        assertNotNull(f.getAs("ID"));
      }
    }
  }
  public void testUseColumnNamesWithSpacesInHeader() throws IOException {
    Path inPath = new Path(scratchPath(), "input.csv");
    copyResource("/test-header-with-spaces.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(x,y)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      int count = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertNotNull(f.getAs("title"));
        count++;
      }
      assertEquals(2, count);
    }
  }

  public void testDisableQuoting() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points_quote.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(CSVFeatureReader.QuoteCharacters, "");
      conf.set(SpatialFileRDD.InputFormat(), "point(1,2)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(inPath, conf);
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
        if (count == 1)
          assertEquals("\'Eua", reader.getCurrentValue().getAs("name"));
      }
      assertEquals(2, count);
    }
  }

  public void testEmptyPoints() throws IOException {
    BeastOptions conf = new BeastOptions();
    Path inPath = new Path(scratchPath(), "in.points");
    FileSystem fs = inPath.getFileSystem(sparkContext().hadoopConfiguration());
    PrintStream ps = new PrintStream(fs.create(inPath));
    ps.println("name1,value1,100.0,25.0");
    ps.println("name2,value2,,");
    ps.println("name3,value3");
    ps.close();
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      conf.set(SpatialFileRDD.InputFormat(), "point(2,3)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      reader.initialize(inPath, conf);
      int i = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        if (i == 1 || i == 2)
          assertTrue("Point should be empty", f.getGeometry().isEmpty());
        i++;
      }
    }
  }

  public void testEmptyWKT() throws IOException {
    BeastOptions conf = new BeastOptions();
    Path inPath = new Path(scratchPath(), "in.wkt");
    FileSystem fs = inPath.getFileSystem(sparkContext().hadoopConfiguration());
    PrintStream ps = new PrintStream(fs.create(inPath));
    ps.println("name1;value1;POLYGON((12 13, 15 17, 20 20, 12 13))");
    ps.println("name2;value2;;");
    ps.println("name3;value3");
    ps.close();
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      conf.set(SpatialFileRDD.InputFormat(), "wkt(2)");
      conf.set(CSVFeatureReader.FieldSeparator, ";");
      reader.initialize(inPath, conf);
      int i = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        if (i == 1 || i == 2)
          assertTrue("Geometry should be empty", f.getGeometry().isEmpty());
        i++;
      }
    }
  }

  public void testAutoDetect() throws IOException {
    String inputFile = new Path(scratchPath(), "test.csv").toString();
    CSVFeatureReader csvFeatureReader = new CSVFeatureReader();

    // Test autodetect field separator and columns for a point dataset
    copyResource("/test-noheader.csv", new File(inputFile));
    BeastOptions detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputFile);
    assertNotNull("Should be able to autodetect the file", detectedOptions);
    assertEquals("point(0,1)", detectedOptions.getString(SpatialFileRDD.InputFormat()));
    assertEquals(",", detectedOptions.getString(CSVFeatureReader.FieldSeparator));

    // Test use the provided field separator (even if it is wrong)
    BeastOptions conf = new BeastOptions();
    conf.set(CSVFeatureReader.FieldSeparator, "\t");
    detectedOptions = csvFeatureReader.autoDetect(conf, inputFile);
    assertNull("Should not be able to autodetect the file", detectedOptions);

    // Test autodetect points with a header
    copyResource("/test-header.csv", new File(inputFile), true);
    detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputFile);
    assertNotNull("Should be able to autodetect the file", detectedOptions);
    assertEquals("point(0,1)", detectedOptions.getString(SpatialFileRDD.InputFormat()));
    assertEquals(",", detectedOptions.getString(CSVFeatureReader.FieldSeparator));
    assertTrue("Should set -skipheader", detectedOptions.getBoolean(CSVFeatureReader.SkipHeader, false));

    // Test autodetect points with a header
    copyResource("/test_wkt.csv", new File(inputFile), true);
    detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputFile);
    assertNotNull("Should be able to autodetect the file", detectedOptions);
    assertEquals("wkt(1)", detectedOptions.getString(SpatialFileRDD.InputFormat()));
    assertEquals("\t", detectedOptions.getString(CSVFeatureReader.FieldSeparator));
    assertTrue("Should set -skipheader", detectedOptions.getBoolean(CSVFeatureReader.SkipHeader, false));

    // Test autodetect points from the header with some empty points
    PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
    ps.println("ID,count,Longitude,Latitude");
    ps.println("10,100,123.44,55.7");
    ps.println("20,500,,");
    ps.close();
    detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputFile);
    assertNotNull("Should be able to autodetect the file", detectedOptions);
    assertEquals("point(2,3)", detectedOptions.get(SpatialFileRDD.InputFormat()).get());
    assertEquals(",", detectedOptions.get(CSVFeatureReader.FieldSeparator).get());
    assertTrue("Should set -skipheader", detectedOptions.getBoolean(CSVFeatureReader.SkipHeader, false));

    // Should work with input directories
    Path inputPath = new Path(new Path(scratchPath(), "subdir"), "subfile.csv");
    FileSystem fs = inputPath.getFileSystem(new Configuration());
    fs.mkdirs(inputPath.getParent());
    copyResource("/test-header.csv", new File(inputPath.toString()));
    detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputPath.getParent().toString());
    assertNotNull("Should be able to autodetect the file", detectedOptions);
    assertEquals("point(0,1)", detectedOptions.get(SpatialFileRDD.InputFormat()).get());
    assertEquals(",", detectedOptions.get(CSVFeatureReader.FieldSeparator).get());
    assertTrue("Should set -skipheader", detectedOptions.getBoolean(CSVFeatureReader.SkipHeader, false));
  }

  public void testAutoDetectWithShapefile() {
    String inputFile = locateResource("/usa-major-cities/usa-major-cities.shp.xml").getPath();
    CSVFeatureReader csvFeatureReader = new CSVFeatureReader();
    BeastOptions detectedOptions = csvFeatureReader.autoDetect(new BeastOptions(), inputFile);
    assertNull(detectedOptions);
  }

  public void testReadFileWithoutHeader() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-noheader.csv", new File(csvPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "nogeom");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
      long fileLength = fs.getFileStatus(csvPath).getLen();
      FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
      reader.initialize(fsplit, conf);
      assertTrue(reader.nextKeyValue());
      assertEquals("33", reader.getCurrentValue().get(1));
      assertTrue(reader.nextKeyValue());
      assertEquals("another name", reader.getCurrentValue().get(3));
      assertFalse(reader.nextKeyValue());
    }
  }

  public void testSkipHeaderNoGeometry() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-header.csv", new File(csvPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "nogeom");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
      long fileLength = fs.getFileStatus(csvPath).getLen();
      FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
      reader.initialize(fsplit, conf);
      assertTrue(reader.nextKeyValue());
      assertEquals("33", reader.getCurrentValue().get(1));
      assertTrue(reader.nextKeyValue());
      assertEquals("another name", reader.getCurrentValue().get(3));
      assertEquals("66", reader.getCurrentValue().getAs("id"));
      assertFalse(reader.nextKeyValue());
    }
  }

  public void testReadFileWithGeometries() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-geometries.csv", new File(csvPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "wkt(1)");
      conf.set(CSVFeatureReader.FieldSeparator, "\t");
      FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
      long fileLength = fs.getFileStatus(csvPath).getLen();
      FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
      reader.initialize(fsplit, conf);
      assertTrue(reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      assertEquals("1", feature.get(1));
      assertEquals("test", feature.get(2));
      assertEquals(new PointND(new GeometryFactory(), 2, 0, 1), feature.getGeometry());
      assertTrue(reader.nextKeyValue());
      assertEquals("test2", reader.getCurrentValue().get(2));
      assertFalse(reader.nextKeyValue());
    }
  }

  public void testReadFileWithPoints() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-noheader.csv", new File(csvPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
      long fileLength = fs.getFileStatus(csvPath).getLen();
      FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
      reader.initialize(fsplit, conf);
      assertTrue(reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      assertEquals("name", feature.get(1));
      assertEquals(new PointND(new GeometryFactory(), 2, 33.0, 123.0), feature.getGeometry());
      assertTrue(reader.nextKeyValue());
      assertEquals("another name", reader.getCurrentValue().get(1));
      assertEquals(new PointND(new GeometryFactory(), 2, 66.0, 154.0), reader.getCurrentValue().getGeometry());
      assertFalse(reader.nextKeyValue());
    }
  }

  public void testIteratorInterface() throws IOException {
    Path csvPath = new Path(locateResource("/test-noheader.csv").getPath());
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
      long fileLength = fs.getFileStatus(csvPath).getLen();
      FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
      reader.initialize(fsplit, conf);
      IFeature firstRecord = reader.next();
      assertNotNull("First record should not be null", firstRecord);
    }
  }

  public void testApplySpatialFilter() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-noheader.csv", new File(csvPath.toString()));
    BeastOptions conf = new BeastOptions();
    conf.set(SpatialFileRDD.FilterMBR(), "10,100,50,200");
    FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
    long fileLength = fs.getFileStatus(csvPath).getLen();
    FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      conf.set(SpatialFileRDD.InputFormat(), "point");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      reader.initialize(fsplit, conf);
      int recordCount = 0;
      while (reader.nextKeyValue())
        recordCount++;
      assertEquals(1, recordCount);
    }
  }

  public void testImmutableObjects() throws IOException {
    Path csvPath = new Path(scratchPath(), "temp.csv");
    copyResource("/test-header.csv", new File(csvPath.toString()));
    BeastOptions conf = new BeastOptions();
    FileSystem fs = csvPath.getFileSystem(conf.loadIntoHadoopConf(null));
    long fileLength = fs.getFileStatus(csvPath).getLen();
    FileSplit fsplit = new FileSplit(csvPath, 0, fileLength, new String[0]);
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      conf.set(CSVFeatureReader.SkipHeader, true);
      conf.set(SpatialFileRDD.InputFormat(), "point");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      reader.initialize(fsplit, conf);
      int recordCount = 0;
      List<IFeature> allFeatues = new ArrayList<>();
      while (reader.nextKeyValue()) {
        recordCount++;
        for (IFeature oldFeature : allFeatues) {
          assertFalse("Cannot reuse objects", reader.getCurrentValue() == oldFeature);
        }
        allFeatues.add(reader.getCurrentValue());
      }
      assertEquals(2, recordCount);
    }
  }

  public void testreadHeaderSecondSplit() throws IOException {
    Path inPath = new Path(scratchPath(), "in.points");
    copyResource("/test_points.csv", new File(inPath.toString()));
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      BeastOptions conf = new BeastOptions();
      conf.set(SpatialFileRDD.InputFormat(), "point(1,2)");
      conf.set(CSVFeatureReader.FieldSeparator, ",");
      conf.set(CSVFeatureReader.SkipHeader, true);
      long fileLength = new File(inPath.toString()).length();
      reader.initialize(new FileSplit(inPath, 10, fileLength - 10, null), conf);
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertEquals("id", f.getAttributeName(0));
        assertEquals("text", f.getAttributeName(1));
      }
    }
  }

  public void testSkipNewLines() throws IOException {
    BeastOptions conf = new BeastOptions();
    Path inPath = new Path(scratchPath(), "in.wkt");
    FileSystem fs = inPath.getFileSystem(sparkContext().hadoopConfiguration());
    PrintStream ps = new PrintStream(fs.create(inPath));
    ps.println("name1\\nanotherline;value1;POLYGON((12 13, 15 17, 20 20, 12 13))");
    ps.close();
    try (CSVFeatureReader reader = new CSVFeatureReader()) {
      conf.set(SpatialFileRDD.InputFormat(), "wkt(2)");
      conf.set(CSVFeatureReader.FieldSeparator, ";");
      reader.initialize(inPath, conf);
      int i = 0;
      while (reader.nextKeyValue()) {
        IFeature f = reader.getCurrentValue();
        assertEquals("name1\nanotherline", f.getString(1));
        i++;
      }
    }
  }

}