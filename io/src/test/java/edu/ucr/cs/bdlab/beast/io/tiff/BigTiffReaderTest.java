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

import java.io.File;
import java.io.IOException;

public class BigTiffReaderTest extends JavaSpatialSparkTest {

  public void testReadBandedFile() throws IOException {
    Path inputFile = new Path(scratchPath(), "tif/simple.tif");
    copyResource("/tif/glc2000_bigtiff.tif", new File(inputFile.toString()));
    FileSystem fs = inputFile.getFileSystem(new Configuration());
    ITiffReader reader = new BigTiffReader();
    try {
      FSDataInputStream in = fs.open(inputFile);
      reader.initialize(in);
      assertEquals(1, reader.getNumLayers());
      TiffRaster raster = reader.getLayer(0);
      assertEquals(22, raster.getPixel(35, 21));
      assertEquals(3, raster.getPixel(71, 54));
    } finally {
      reader.close();
    }
  }
}