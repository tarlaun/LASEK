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
package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ITiffReaderTest extends JavaSpatialSparkTest {

  public void testOpenRegularTIFF() throws IOException {
    // Test with a regular
    Path inputFile = new Path(locateResource("/tif/glc2000_small.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    FSDataInputStream in = fs.open(inputFile);
    try (ITiffReader reader = ITiffReader.openFile(in)) {
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      // Blue pixel (ocean)
      assertEquals(20, raster.getPixel(54, 24));
      // Gray pixel (desert)
      assertEquals(19, raster.getPixel(137, 58));
    }
  }

  public void testOpenBigTIFF() throws IOException {
    Path inputFile = new Path(locateResource("/tif/glc2000_bigtiff.tif").getPath());
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    FSDataInputStream in = fs.open(inputFile);
    try (ITiffReader reader = ITiffReader.openFile(in)) {
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertEquals(22, raster.getPixel(35, 21));
      assertEquals(3, raster.getPixel(71, 54));
    }
  }
}