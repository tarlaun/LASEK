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
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class DBFWriterTest extends JavaSpatialSparkTest {

  public void testSimpleWriter() throws IOException {
    DBFReader dbfReader = new DBFReader();
    DBFWriter dbfWriter = new DBFWriter();
    String[] dbfFileNames = {"usa-major-cities"};
    for (String dbFileName : dbfFileNames) {
      Path dbfPath = new Path(scratchPath(), dbFileName+".dbf");
      Configuration conf = new Configuration();
      // Create a DBF file that is a copy of the existing file
      try {
        DataInputStream dbfInput = new DataInputStream(getClass().getResourceAsStream("/" + dbFileName + "/" + dbFileName + ".dbf"));
        dbfReader.initialize(dbfInput, new BeastOptions(conf));
        dbfWriter.initialize(dbfPath, conf);
        while (dbfReader.nextKeyValue()) {
          dbfWriter.write(dbfReader.getCurrentValue());
        }
      } finally {
        dbfReader.close();
        dbfWriter.close();
      }

      // Read the written file back and make sure it is similar to the original one
      DBFReader reader2 = new DBFReader();
      try {
        DataInputStream dbfInput = new DataInputStream(getClass().getResourceAsStream("/"+dbFileName+"/"+dbFileName+".dbf"));
        dbfReader.initialize(dbfInput, new BeastOptions(conf));
        reader2.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), new BeastOptions(conf));
        while (dbfReader.nextKeyValue()) {
          assertTrue("The written file contains too few records", reader2.nextKeyValue());
          assertEquals(dbfReader.getCurrentValue(), reader2.getCurrentValue());
        }
      } finally {
        dbfReader.close();
        reader2.close();
      }
    }
  }

  public void testShouldWriteFeaturesWithoutNames() throws IOException {
    // A sample feature
    Feature f = Feature.create(null, null, null, new Object[]{"abc", "def"});

    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "test.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one record
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(f);
    } finally {
      dbfWriter.close();
    }

    // Read the written file back and make sure it is correct
    try (DBFReader reader = new DBFReader()) {
      reader.initialize(dbfPath, new BeastOptions(conf));
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
        assertEquals(reader.getCurrentValue(), f);
      }
      assertEquals(1, count);
    }
  }

  public void testWriteAllAttributeTypes() throws IOException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    GregorianCalendar date = new GregorianCalendar(DBFConstants.UTC);
    date.clear();
    date.set(2019, Calendar.OCTOBER, 9);
    java.sql.Date dateValue = new java.sql.Date(date.getTimeInMillis());
    scala.collection.immutable.Map<String, String> tags = new scala.collection.immutable.HashMap<String, String>();
    tags = tags.$plus(new Tuple2<>("tag", "value"));
    Feature f = Feature.create(
        null,
        new String[]{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"},
        null,
        new Object[]{Boolean.TRUE, 123, (byte) 12, (short) 13, (long) 6546543, 12.25f, 12.655847, "value9", dateValue, tags}
    );
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(f);
    } finally {
      try {
        dbfWriter.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    try (DBFReader dbfReader = new DBFReader()) {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), new BeastOptions(conf));
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      for (int i : f.iNonGeomJ()) {
        // Compare the object or its string representation since the map type is converted to string when loaded
        assertTrue("Features attribute #"+i+" should be equal",
            f.get(i).equals(f2.get(i)) || f.get(i).toString().equals(f2.get(i).toString()));
      }
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    }
  }

  public void testWriteNegativeIntegers() throws IOException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    Feature f = Feature.create(null, new String[]{"key1", "key2", "key3"},
        null, new Object[]{-123, (long) Integer.MIN_VALUE, Long.MAX_VALUE});
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(f);
    } finally {
      try {
        dbfWriter.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    try (DBFReader dbfReader = new DBFReader()) {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), new BeastOptions(conf));
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      assertEquals(f.getInt(1), f2.getInt(1));
      assertEquals(f.getLong(2), f2.getLong(2));
      assertEquals((double) f.getLong(3), f2.getDouble(3));
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    }
  }

  public void testWriteNullAttributeTypes() throws IOException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    Feature f = Feature.create(null,
        new String[] {"key1", "key2", "key3", "key4", "key5", "key6"},
        new DataType[]{
            DataTypes.BooleanType, DataTypes.IntegerType, DataTypes.LongType,
            DataTypes.DoubleType, DataTypes.StringType, DataTypes.TimestampType
        }, null
    );
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(f);
    } finally {
      try {
        dbfWriter.close();
      } catch (Exception e) {

      }
    }

    // Read the written file back and make sure it is similar to the original one
    DBFReader dbfReader = new DBFReader();
    try {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), new BeastOptions(conf));
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      for (int i = 0; i < 6; i++)
        assertNull("Attribute " + i + " should be null", f2.get(i+1));
      assertEquals(DataTypes.BooleanType, f2.getDataType(1));
      assertEquals(DataTypes.IntegerType, f2.getDataType(2));
      assertEquals(DataTypes.IntegerType, f2.getDataType(3));
      assertEquals(DataTypes.IntegerType, f2.getDataType(4));
      assertEquals(DataTypes.StringType, f2.getDataType(5));
      assertEquals(DataTypes.TimestampType, f2.getDataType(6));
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    } finally {
      dbfReader.close();
    }
  }

  public void testWriteUnicodeString() throws IOException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    String word = "\u0633\u0644\u0627\u0645";
    // Create a DBF file with one feature that has all the possible types
    Feature f = Feature.create(null, new String[]{"key1"},
        null, new Object[]{word});
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(f);
    } finally {
      try {
        dbfWriter.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    try (DBFReader dbfReader = new DBFReader()) {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), new BeastOptions(conf));
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      assertEquals("Non-matching unicode characters "+ Arrays.toString(word.getBytes(StandardCharsets.UTF_8)) + " <-> "+
              Arrays.toString(f2.getString(1).getBytes(StandardCharsets.UTF_8)), word, f2.getString(1));
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    }
  }

}