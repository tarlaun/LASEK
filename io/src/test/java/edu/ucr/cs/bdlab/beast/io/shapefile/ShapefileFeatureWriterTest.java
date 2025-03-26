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

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;

public class ShapefileFeatureWriterTest extends JavaSpatialSparkTest {

  public void testCreation() throws IOException, InterruptedException {
    GeometryFactory factory = GeometryReader.DefaultGeometryFactory;
    Geometry[] geometries = {
        new PointND(factory, 2, 0.0,1.0),
        new PointND(factory, 2, 3.0, 10.0)
    };

    Configuration conf = sparkContext().hadoopConfiguration();
    Path outPath = new Path(scratchPath(), "test");
    Path shpFileName = new Path(outPath, "test.shp");
    FileSystem fileSystem = outPath.getFileSystem(conf);
    fileSystem.mkdirs(outPath);

    ShapefileFeatureWriter writer = new ShapefileFeatureWriter();
    try {
      writer.initialize(shpFileName, conf);
      for (Geometry geom : geometries) {
        Feature f = Feature.create(geom, new String[] {"name"}, null, new Object[] {"name-value"});
        writer.write(f);
      }
    } finally {
      try {
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    assertTrue("Shapefile not found", fileSystem.exists(shpFileName));
    Path shxFileName = new Path(outPath, "test.shx");
    assertTrue("Shape index file not found", fileSystem.exists(shxFileName));
    Path dbfFileName = new Path(outPath, "test.dbf");
    assertTrue("DBF file not found", fileSystem.exists(dbfFileName));
    Path prjFileName = new Path(outPath, "test.prj");
    assertTrue("PRJ file not found", fileSystem.exists(prjFileName));
  }
}