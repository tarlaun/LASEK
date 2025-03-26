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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class CatTest extends JavaSpatialSparkTest {

  public void testConvertToRTree() {
    String input = new Path(scratchPath(), "test.input").toString();
    copyResource("/test111.points", new File(input));
    String output = new Path(scratchPath(), "test.rtree").toString();
    BeastOptions opts = new BeastOptions(false).set("iformat","point").set("oformat", "rtree");
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    opts.set(SpatialWriter.OutputFormat(), "rtree");

    Cat.run(opts, new String[] {input}, new String[] {output}, sparkContext());
    assertFileExists(output);

    // Now convert it back to CSV to make sure it was written correctly
    String rtreeFile = output;
    output = new Path(scratchPath(), "converted_back.csv").toString();
    opts = new BeastOptions(false).set("iformat", "rtree").set("oformat", "point");
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    opts.set(SpatialWriter.OutputFormat(), "point");
    Cat.run(opts, new String[] {rtreeFile}, new String[] {output}, sparkContext());
    assertFileExists(output);
    String[] lines = readFile(new File(new File(output), "part-00000.csv").getPath());
    assertEquals(111, lines.length);
  }
}