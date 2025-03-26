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
package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.test.JavaSparkTest;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class FileUtilTest extends JavaSparkTest {

  public void testRelativize() {
    Path path = new Path("path/to/directory");
    Path refPath = new Path("path/to/another");

    Path relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("../directory"), relativePath);

    // Try with a common prefix in the name
    path = new Path("long_directory_name");
    refPath = new Path("longer_directory_name/");

    relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("../long_directory_name"), relativePath);

    // Try with a subdirectory
    refPath = new Path("basepath/");
    path = new Path(refPath, "subdir");

    relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("subdir"), relativePath);

    // Test equal paths
    relativePath = FileUtil.relativize(new Path("basepath/"), new Path("basepath/"));
    assertEquals(new Path("."), relativePath);

    // Test with an empty basepath
    refPath = new Path(".");
    path = new Path(refPath, "subdir");
    relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("subdir"), relativePath);

  }

  public void testConcatenate() throws IOException {
    File file1 = new File(scratchDir(), "file1");
    File file2 = new File(scratchDir(), "file2");
    makeResourceCopy("/beast.properties", file1);
    makeResourceCopy("/test.properties", file2);

    String[] lines1 = readFile(file1.toString());
    String[] lines2 = readFile(file2.toString());
    Configuration conf = sparkContext().hadoopConfiguration();
    FileSystem fs = new Path(file1.toString()).getFileSystem(conf);

    File file3 = new File(scratchDir(), "file3");
    FileUtil.concat(fs, new Path(file3.toString()), new Path(file1.toString()), new Path(file2.toString()));
    String[] allLines = readFile(file3.toString());
    assertEquals(String.join("\n", lines1)+String.join("\n", lines2),
        String.join("\n", allLines));

    // Assert that the input files were deleted
    assertFalse("Input file '"+file1+"' should be deleted", file1.exists());
    assertFalse("Input file '"+file2+"' should be deleted", file2.exists());
  }

  public void testFlattenEmptyDirectories() throws IOException {
    File root = new File(scratchDir(), "rootDir");
    File subdir = new File(root, "subDir");
    subdir.mkdirs();
    FileUtil.flattenDirectory(FileSystem.getLocal(sparkContext().hadoopConfiguration()), new Path(root.getPath()));
    assertTrue(root.exists());
    assertEquals(0, root.list().length);
  }
}