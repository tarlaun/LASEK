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
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RTreeFeatureReaderTest extends JavaSpatialSparkTest {

  static IFeature[] features = {
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 1.0, 2.4), "abc", "def"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 3.0, 4.4), "abcc", "deff"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 5.0, 6.4), "abbc", "deef"}, null),
      new Feature(new Object[] {new PointND(GeometryReader.DefaultGeometryFactory, 7.0, 8.4), "aabc", "ddef"}, null),
  };

  public void testReadAll() throws IOException {
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
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, new BeastOptions(conf));

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(features.length, count);
    } finally {
      reader.close();
    }
  }

  public void testRangeSearch() throws IOException, InterruptedException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.FilterMBR(), "0,0,4,5");
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, new BeastOptions(conf));

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(2, count);
    } finally {
      reader.close();
    }
  }

  public void testRangeSearchContained() throws IOException, InterruptedException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.FilterMBR(), "0,0,8,10");
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, new BeastOptions(conf));

      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
      }

      assertEquals(4, count);
    } finally {
      reader.close();
    }
  }

  public void testReadWithSchema() throws IOException, InterruptedException {
    RTreeFeatureWriter writer = new RTreeFeatureWriter();
    Path rtreePath = new Path(scratchPath(), "test.rtree");
    Configuration conf = new Configuration();
    writer.initialize(rtreePath, conf);
    for (IFeature f : features) {
      writer.write(f);
    }
    writer.close();

    // Now, read the data back
    List<IFeature> features2 = new ArrayList<>();
    RTreeFeatureReader reader = new RTreeFeatureReader();
    FileSystem fs = rtreePath.getFileSystem(conf);
    FileSplit fsplit = new FileSplit(rtreePath, 0, fs.getFileStatus(rtreePath).getLen(), new String[0]);
    try {
      reader.initialize(fsplit, new BeastOptions(conf));

      int count = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        features2.add(feature);
        assertEquals(3, feature.length());
        count++;
      }

      assertEquals(features.length, count);
    } finally {
      reader.close();
    }
  }
}