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
package edu.ucr.cs.bdlab.beast.operations;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.test.JavaSparkTest;

import java.io.File;
import java.io.IOException;

public class SpatialJoinTest extends JavaSparkTest {
  public void testSimpleSpatialJoin() throws IOException {
    File inputFile1 = new File(scratchDir(), "in.rect");
    File inputFile2 = new File(scratchDir(), "in.point");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/test.rect", inputFile1);
    copyResource("/test2.points", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat[0]", "envelope")
        .set("iformat[1]", "point(1,2)")
        .set("separator", ",")
        .set("skipheader[1]", true)
        .set("predicate", "contains");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts, new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()}, sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(2, lines.length);
  }

  public void testSpatialJoinMoreGeometries() throws IOException {
    File inputFile1 = new File(scratchDir(), "in1");
    File inputFile2 = new File(scratchDir(), "in2");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/sjoinr.wkt", inputFile1);
    copyResource("/sjoins.wkt", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "wkt")
        .set("predicate", "intersects");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(5, lines.length);
  }

  public void testSpatialJoinPBSM() throws IOException {
    File inputFile1 = new File(scratchDir(), "in1");
    File inputFile2 = new File(scratchDir(), "in2");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/sjoinr.wkt", inputFile1);
    copyResource("/sjoins.wkt", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "wkt")
        .set("predicate", "intersects")
        .set("method", "pbsm");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(5, lines.length);
  }

  public void testSpatialJoinMoreGeometriesContains() throws IOException {
    File inputFile1 = new File(scratchDir(), "in1");
    File inputFile2 = new File(scratchDir(), "in2");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/sjoinr.wkt", inputFile1);
    copyResource("/sjoins.wkt", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "wkt")
        .set("predicate", "contains");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile2.getPath(), inputFile1.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(2, lines.length);
  }

  public void testBNLJDuplicateAvoidance() throws IOException {
    File inputFile1 = new File(scratchDir(), "in1");
    File inputFile2 = new File(scratchDir(), "in2");
    File outputFile = new File(scratchDir(), "out");
    makeResourceCopy("/sjoinr.grid", inputFile1);
    makeResourceCopy("/sjoins.grid", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "wkt")
        .set("predicate", "intersects")
        .set("method", "bnlj");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(5, lines.length);
  }

  public void testDistributedJoin() throws IOException {
    File inputFile1 = new File(scratchDir(), "in.rect");
    File inputFile2 = new File(scratchDir(), "in.point");
    File outputFile = new File(scratchDir(), "out");
    copyResource("/test.rect", inputFile1);
    copyResource("/test2.points", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat[0]", "envelope")
        .set("iformat[1]", "point(1,2)")
        .set("separator", ",")
        .set("skipheader[1]", true)
        .set("predicate", "contains")
        .set("method", "dj");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(2, lines.length);
  }

  public void testDistributedJoinDuplicateAvoidance() throws IOException {
    File inputFile1 = new File(scratchDir(), "in1");
    File inputFile2 = new File(scratchDir(), "in2");
    File outputFile = new File(scratchDir(), "out");
    makeResourceCopy("/sjoinr.grid", inputFile1);
    makeResourceCopy("/sjoins.grid", inputFile2);

    BeastOptions opts = new BeastOptions(false)
        .set("iformat", "wkt")
        .set("predicate", "intersects")
        .set("method", "dj");
    opts.set(SpatialWriter.OutputFormat(), "wkt");
    SpatialJoin.run(opts,new String[] {inputFile1.getPath(), inputFile2.getPath()},
        new String[] {outputFile.getPath()},  sparkContext());

    assertFileExists(outputFile.getPath());
    String[] lines = readFilesInDirAsLines(outputFile);
    assertEquals(5, lines.length);
  }
}