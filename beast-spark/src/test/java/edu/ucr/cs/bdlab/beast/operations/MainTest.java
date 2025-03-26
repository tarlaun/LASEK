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
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.synopses.Synopsis;
import edu.ucr.cs.bdlab.beast.util.OperationHelper;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import edu.ucr.cs.bdlab.beast.util.OperationMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class MainTest extends JavaSparkTest {

  public void testCommandLineOperationScala() throws IOException {
    Path inPath = new Path(scratchPath(), "test111.points");
    copyResource("/test111.points", new File(inPath.toString()));
    Path outPath = new Path(scratchPath(), "test111.wkt_points");
    String[] args = {"cat", "iformat:point(0,1)", "oformat:wkt(0)", "separator:,",
      inPath.toString(), outPath.toString()};
    // Spark does not allow (by default) two concurrent contexts.
    // We close the current context and reopen it after the test is done
    closeSC();
    Synopsis.cachedSynopses().clear();
    Main.main(args);
    FileSystem fs = outPath.getFileSystem(new Configuration());
    assertTrue("Output file not found", fs.exists(outPath));
    String[] lines = readFile(new Path(outPath, "part-00000.csv").toString());
    assertEquals(111, lines.length);
    assertEquals("POINT (101.7075756 3.2407152)", lines[0]);
  }

  public void testCommandLineWithAutodetect() throws IOException {
    Path inPath = new Path(scratchPath(), "test111.points");
    copyResource("/test111.points", new File(inPath.toString()));
    Path outPath = new Path(scratchPath(), "test111.wkt_points");
    String[] args = {"cat", "oformat:wkt", inPath.toString(), outPath.toString()};
    // Spark does not allow (by default) two concurrent contexts.
    // We close the current context and reopen it after the test is done
    closeSC();
    Synopsis.cachedSynopses().clear();
    Main.main(args);
    FileSystem fs = outPath.getFileSystem(new Configuration());
    assertTrue("Output file not found", fs.exists(outPath));
    String[] lines = readFile(new Path(outPath, "part-00000.csv").toString());
    assertEquals(111, lines.length);
    assertEquals("POINT (101.7075756 3.2407152)", lines[0]);
  }

  public void testCommandLineOperationScalaWithParameters() throws IOException {
    Path inPath1 = new Path(scratchPath(), "test111.points");
    Path inPath2 = new Path(scratchPath(), "test222.points");
    copyResource("/test111.points", new File(inPath1.toString()));
    copyResource("/test111.points", new File(inPath2.toString()));
    Path outPath = new Path(scratchPath(), "result");
    String[] args = {"sj", "iformat:point(0,1)", "separator:,", "method:bnlj",
        inPath1.toString(), inPath2.toString(), outPath.toString()};
    // Spark does not allow (by default) two concurrent contexts.
    // We close the current context and reopen it after the test is done
    closeSC();
    Synopsis.cachedSynopses().clear();    Main.main(args);
    FileSystem fs = outPath.getFileSystem(new Configuration());
    assertTrue("Output file not found", fs.exists(outPath));
  }

  @OperationMetadata(shortName = "testjava", description = "...", inputArity = "0", outputArity = "0")
  public static class JavaMethod implements JCLIOperation {

    public static boolean methodRun = false;

    @Override
    public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
      methodRun = true;
      return null;
    }
  }

  public void testCommandLineOperationJava() {
    JavaMethod.methodRun = false;
    // Force the test operation into the configuration
    OperationHelper.readConfigurationXML("beast.xml").get("Operations").add(JavaMethod.class.getName());
    String[] args = {"testjava"};
    // Spark does not allow (by default) two concurrent contexts.
    // We close the current context and reopen it after the test is done
    closeSC();
    Synopsis.cachedSynopses().clear();
    Main.main(args);
    assertTrue("Method did not run correctly", JavaMethod.methodRun);
   }

}