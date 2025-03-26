package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.shapefile.DBFConstants;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

public class CSVFeatureWriterTest extends JavaSpatialSparkTest {

  public void testWriteNoheader() throws IOException, InterruptedException {
    Path csvPath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, false);
    conf.set(SpatialWriter.OutputFormat(), "point(1,2)");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(csvPath, conf);
    PointND p = new PointND(new GeometryFactory(), 2, 0.1, 0.3);
    Feature f = Feature.create(p, new String[] {"att1", "att2"}, null, new Object[] {"abc", "def"});

    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(csvPath.toString());
    assertEquals(1, writtenFile.length);
    assertEquals("abc\t0.1\t0.3\tdef", writtenFile[0]);
  }

  public void testWriteWithHeader() throws IOException, InterruptedException {
    Path csvPath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, true);
    conf.set(SpatialWriter.OutputFormat(), "point(1,2)");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(csvPath, conf);
    PointND p = new PointND(new GeometryFactory(), 2, 0.1, 0.3);
    Feature f = Feature.create(p, new String[] {"name", "value"}, null, new Object[] {"abc", "def"});

    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(csvPath.toString());
    assertEquals(2, writtenFile.length);
    assertEquals("name\tx\ty\tvalue", writtenFile[0]);
    assertEquals("abc\t0.1\t0.3\tdef", writtenFile[1]);
  }

  public void testWriteWKTWithHeader() throws IOException, InterruptedException {
    Path csvPath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, true);
    conf.set(SpatialWriter.OutputFormat(), "wkt(1)");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(csvPath, conf);
    PointND p = new PointND(new GeometryFactory(), 2, 0.1, 0.3);
    Feature f = Feature.create(p, new String[] {"name", "value"}, null, new Object[] {"abc", "def"});

    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(csvPath.toString());
    assertEquals(2, writtenFile.length);
    assertEquals("name\tgeometry\tvalue", writtenFile[0]);
    assertEquals("abc\tPOINT(0.1 0.3)\tdef", writtenFile[1]);
  }

  public void testWriteEnvelopeWithHeader() throws IOException, InterruptedException {
    Path csvPath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, true);
    conf.set(SpatialWriter.OutputFormat(), "envelopek(2,1,2,4,5)");
    conf.set(CSVFeatureWriter.FieldSeparator, ",");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(csvPath, conf);
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), 2, 0.1, 0.3, 0.5, 0.8);
    Feature f = Feature.create(e, new String[] {"name", "value"}, null, new Object[] {"abc", "def"});

    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(csvPath.toString());
    assertEquals(2, writtenFile.length);
    assertEquals("name,xmin,ymin,value,xmax,ymax", writtenFile[0]);
    assertEquals("abc,0.1,0.3,def,0.5,0.8", writtenFile[1]);
  }

  public void testWriteTimestamp() throws IOException, InterruptedException {
    Path outpath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, false);
    conf.set(SpatialWriter.OutputFormat(), "point");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(outpath, conf);
    PointND p = new PointND(new GeometryFactory(), 2, 0.1, 0.3);
    GregorianCalendar t1 = new GregorianCalendar(DBFConstants.UTC);
    t1.clear();
    t1.set(2020, GregorianCalendar.OCTOBER, 15);
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    GregorianCalendar t2 = new GregorianCalendar(DBFConstants.UTC);
    t2.clear();
    t2.set(2020, GregorianCalendar.OCTOBER, 18, 11, 30);

    Feature f = Feature.create(p, new String[] {"date", "datetime"}, null,
        new Object[] {new java.sql.Date(t1.getTimeInMillis()), new java.sql.Timestamp(t2.getTimeInMillis())});
    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(outpath.toString());
    assertEquals(1, writtenFile.length);
    assertTrue(writtenFile[0].contains("2020-10-15"));
    assertTrue(writtenFile[0].contains("2020-10-18T11:30"));
  }

  public void testSkipNewLines() throws IOException, InterruptedException {
    Path csvPath = new Path(scratchPath(), "test.csv");
    Configuration conf = sparkContext().hadoopConfiguration();
    conf.setBoolean(CSVFeatureWriter.WriteHeader, false);
    conf.set(SpatialWriter.OutputFormat(), "point(1,2)");

    CSVFeatureWriter writer = new CSVFeatureWriter();
    writer.initialize(csvPath, conf);
    PointND p = new PointND(new GeometryFactory(), 2, 0.1, 0.3);
    Feature f = Feature.create(p, new String[] {"att1", "att2"}, null, new Object[] {"aaa\nbbb", "def"});

    writer.write(f);
    writer.close();

    String[] writtenFile = readFile(csvPath.toString());
    assertEquals(1, writtenFile.length);
    assertEquals("aaa\\nbbb\t0.1\t0.3\tdef", writtenFile[0]);
  }
}