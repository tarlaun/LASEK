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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import edu.ucr.cs.bdlab.beast.util.CounterOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.beast.sql.GeometryDataType;
import org.apache.spark.sql.types.*;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import scala.Tuple2;

import java.io.IOException;
import java.util.Random;


public class RTreeFeatureWriterTest extends JavaSpatialSparkTest {

  static IFeature[] features = {
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 1.0, 2.4), "abc", "def"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 3.0, 4.4), "abcc", "deff"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 5.0, 6.4), "abbc", "deef"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 7.0, 8.4), "aabc", "ddef"}, null),
  };

  public void testWrite() throws IOException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    FileSystem fs = rtreePath.getFileSystem(conf);
    long fileLength = fs.getFileStatus(rtreePath).getLen();
    FSDataInputStream in = fs.open(rtreePath);
    RTreeFeatureReader.readFeatureSchema(in);

    String crsWKT = in.readUTF();
    int rtreeSize = (int) (fileLength - in.getPos());

    RTreeGuttman rtree = new RTreeGuttman(4, 8);
    rtree.readFields(in, rtreeSize, input -> null);
    Iterable<RTreeGuttman.Entry> results = rtree.search(new EnvelopeNDLite(2, 0, 0, 4, 5));
    int resultCount = 0;
    for (Object o : results)
      resultCount++;
    assertEquals(2, resultCount);
    rtree.close();
  }

  public void testWriteFeaturesWithDifferentSchema() throws IOException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    StructType schema1 = new StructType(new StructField[]{
            new StructField("g", GeometryDataType.geometryType(), true, Metadata.empty()),
            new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("description", DataTypes.StringType, true, Metadata.empty()),
            new StructField("area", DataTypes.DoubleType, true, Metadata.empty()),
    });
    StructType schema2 = new StructType(new StructField[]{
            new StructField("g", GeometryDataType.geometryType(), true, Metadata.empty()),
            new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("description", DataTypes.StringType, true, Metadata.empty()),
            new StructField("area", DataTypes.DoubleType, true, Metadata.empty()),
    });
    IFeature[] features = {
            new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 3.0, 4.4), 2, "desc", 3.5}, schema1),
            new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 1.0, 2.4), 1, "name", "desc", 4.5}, schema2),
            new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 5.0, 2.4), 1, "name2", "desc2", 5.5}, schema2),
    };
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    FileSystem fs = rtreePath.getFileSystem(conf);
    long fileLength = fs.getFileStatus(rtreePath).getLen();
    RTreeFeatureReader reader = new RTreeFeatureReader();
    try {
      reader.initialize(new FileSplit(rtreePath, 0, fileLength, null), new BeastOptions());
      int resultCount = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        System.out.println(feature);
        assertFalse(feature.isNullAt(feature.schema().fieldIndex("description")));
        double area = feature.getDouble(feature.schema().fieldIndex("area"));
        assertTrue(area > 3 && area < 6);
        resultCount++;
      }
      assertEquals(features.length, resultCount);
    } finally {
      reader.close();
    }
  }

  public void testWriteMapAttributes() throws IOException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    Feature f = new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 1.0, 2.4),
      new scala.collection.immutable.HashMap<String, String>().$plus(new Tuple2<>("key", "value"))}, null);
    writer.write(f);
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), new BeastOptions(conf));
    assertTrue(reader.nextKeyValue());
    assertEquals(1, ((scala.collection.Map)reader.getCurrentValue().get(1)).size());
    reader.close();
  }

  public void testWriteWithEmptyGeometries() throws IOException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features)
      writer.write(f);
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), new BeastOptions(conf));
    int count = 0;
    for (Object o : reader) {
      count++;
    }
    assertEquals(4, count);
    reader.close();
  }

  public void testEstimateSize() throws IOException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Configuration conf = new Configuration();
    writer.initialize(new CounterOutputStream(), conf);
    long size = 0;
    for (IFeature f : features) {
      size += writer.estimateSize(f);
    }
    writer.close();
    // No actual assert is needed. Just make sure that it did not fail
    assertTrue(size > 0);
  }

  public void testWriteWithCRS() throws IOException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 3857);
    IFeature features[] = {
        new Feature(new Object[] {new PointND(geometryFactory, 1.0, 2.4), "abc", "def"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 3.0, 4.4), "abcc", "deff"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 5.0, 6.4), "abbc", "deef"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 7.0, 8.4), "aabc", "ddef"}, null),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), new BeastOptions(conf));
    assertTrue(reader.nextKeyValue());
    assertEquals(3857, reader.getCurrentValue().getGeometry().getSRID());
    reader.close();
  }

  public void testEmptyFeaturesShouldNotAffectCRS() throws IOException {
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 3857);
    IFeature features[] = {
        new Feature(new Object[] {EmptyGeometry.instance, "abc", "def"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 1.0, 2.4), "abc", "def"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 3.0, 4.4), "abcc", "deff"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 5.0, 6.4), "abbc", "deef"}, null),
        new Feature(new Object[] {new PointND(geometryFactory, 7.0, 8.4), "aabc", "ddef"}, null),
    };

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    reader.initialize(new FileSplit(rtreePath, 0, rtreePath.getFileSystem(conf).getFileStatus(rtreePath).getLen(), null), new BeastOptions(conf));
    assertTrue(reader.nextKeyValue());
    assertTrue(reader.nextKeyValue());
    assertEquals(3857, reader.getCurrentValue().getGeometry().getSRID());
    reader.close();
  }

  public void testWriteMultipleRTrees() throws IOException {
    // Writing too many data should create multiple internal R-trees
    int numRecords = 10000;
    Random rand = new Random(0);
    IFeature features[] = new IFeature[numRecords];
    for (int i = 0; i < numRecords; i++)
      features[i] = Feature.create(null, new PointND(GeometryReader.DefaultGeometryFactory, rand.nextDouble(), rand.nextDouble()));

    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    // Use a small number to ensure that multiple R-trees will be written
    conf.setInt(RTreeFeatureWriter.MaxSizePerRTree, 1000);
    writer.initialize(rtreePath, conf);
    for (IFeature f : features)
      writer.write(f);
    writer.close();

    // Now, read the data back
    FileSystem fs = rtreePath.getFileSystem(conf);
    long fileLength = fs.getFileStatus(rtreePath).getLen();
    try (FSDataInputStream in = fs.open(rtreePath)) {
      RTreeFeatureReader.readFeatureSchema(in);
      String crsWKT = in.readUTF();
      int numRTrees = 0;
      long dataStart = in.getPos();
      long treeEnd = fileLength;
      while (treeEnd > dataStart) {
        in.seek(treeEnd - 4);
        int treeSize = in.readInt();
        treeEnd -= treeSize + 4;
        numRTrees++;
      }
      assertEquals(dataStart, treeEnd);
      assertTrue("File should contain more than one R-tree", numRTrees > 1);
    }
    // Read the file back using RTreeFeatureReader
    try (RTreeFeatureReader reader = new RTreeFeatureReader()) {
      reader.initialize(new FileSplit(rtreePath, 0, fileLength, null), new BeastOptions(conf));
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        recordCount++;
      }
      assertEquals(numRecords, recordCount);
    }
  }
}