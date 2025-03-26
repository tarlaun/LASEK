package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.GregorianCalendar;

public class DBFReaderTest extends JavaSpatialSparkTest {

  public void testSimpleReader() throws IOException {
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource("/usa-major-cities/usa-major-cities.dbf", new File(dbfPath.toString()));
    try (DBFReader reader = new DBFReader()) {
      reader.initialize(dbfPath, new BeastOptions());
      assertEquals(120, reader.header.numRecords);
      assertEquals(4, reader.header.fieldDescriptors.length);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        assertEquals(5, feature.length());
        assertNotNull(feature.getAs("RuleID"));
        assertEquals(feature.get(4), feature.getAs("RuleID"));
        assertEquals(Integer.class, feature.getAs("RuleID").getClass());
        recordCount++;
      }
      assertEquals(120, recordCount);
    }
  }

  public void testParseFields() throws IOException {
    String[] dbfFiles = {
        "/usa-major-cities/usa-major-cities.dbf",
        "/usa-major-highways/usa-major-highways.dbf",
        "/simple-with-dates.dbf"
    };
    for (String dbfFile : dbfFiles) {
      Path dbfPath = new Path(scratchPath(), "temp.dbf");
      copyResource(dbfFile, new File(dbfPath.toString()));
      try (DBFReader reader = new DBFReader()) {
        reader.initialize(dbfPath, new BeastOptions());
        while (reader.nextKeyValue()) {
          IFeature feature = reader.getCurrentValue();
          for (int i : feature.iNonGeomJ())
            assertNotNull(feature.get(i));
        }
      } finally {
        new File(dbfPath.toString()).delete();
      }
    }
  }

  public void testParseDates() throws IOException {
    String dbfFile = "/simple-with-dates.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    try (DBFReader reader = new DBFReader()) {
      reader.initialize(dbfPath, new BeastOptions());
      assertTrue("Did not find records in the file", reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      GregorianCalendar expected = new GregorianCalendar(DBFConstants.UTC);
      expected.clear();
      expected.set(2018, 12 - 1, 26);
      java.sql.Date expectedDate = new java.sql.Date(expected.getTimeInMillis());
      assertEquals(expectedDate, feature.get(2));
    } finally {
      new File(dbfPath.toString()).delete();
    }
  }

  public void testShouldNotReturnNullOnEmptyStrings() throws IOException {
    try (DBFReader reader = new DBFReader()) {
      DataInputStream in = new DataInputStream(this.getClass().getResourceAsStream("/file_with_empty_numbers.dbf"));
      reader.initialize(in, new BeastOptions());
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        // No assertions. Just make sure that no errors are raised
      }
    }
  }

  public void testShouldIgnoreTrailingZerosInStrings() throws IOException {
    try (DBFReader reader = new DBFReader()) {
      DataInputStream in = new DataInputStream(this.getClass().getResourceAsStream("/nullstrings.dbf"));
      reader.initialize(in, new BeastOptions());
      assertTrue(reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      assertNull(feature.get(2));
      assertTrue(reader.nextKeyValue());
      feature = reader.getCurrentValue();
      assertEquals("Hello", feature.get(2));
    }
  }
}