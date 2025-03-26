/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class SpatialFilePartitionerTest extends AnyFunSuite with ScalaSparkTest {
  test("If the input is a file, return it as-is") {
    val conf = sparkContext.hadoopConfiguration
    val initialPath = new Path(locateResource("/features.geojson").getPath)
    val fileSystem = initialPath.getFileSystem(conf)
    val files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator).toArray
    assertResult(1)(files.length)
    assertResult("features.geojson")(new Path(files(0).filePath).getName)
  }

  test("If the input is a directory, return all files") {
    val conf = sparkContext.hadoopConfiguration
    val initialPath = new Path(locateResource("/linetest").getPath)
    val fileSystem = initialPath.getFileSystem(conf)
    val files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator).toArray
    assertResult(5)(files.length)
    assertResult(Array("linetest.dbf", "linetest.prj", "linetest.qpj", "linetest.shp", "linetest.shx"))(files.map(p => new Path(p.filePath).getName).sorted)
  }

  test("Return contents of a directory with extension filter") {
    val conf = sparkContext.hadoopConfiguration
    val initialPath = new Path(locateResource("/linetest").getPath)
    val fileSystem = initialPath.getFileSystem(conf)
    val files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, Array(".shp")).toArray
    assertResult(1)(files.length)
    assertResult("linetest.shp")(new Path(files.head.filePath).getName)
  }

  test("Skip hidden files") {
    val path = new File(scratchDir, "subdir")
    path.mkdirs()
    try {
      copyResource("/test.points", new File(path, "input.txt"))
      copyResource("/test.points", new File(path, "_input.txt"))
      copyResource("/test.points", new File(path, ".input.txt"))
      val conf = sparkContext.hadoopConfiguration
      val initialPath = new Path(path.getPath)
      val fileSystem = initialPath.getFileSystem(conf)
      var files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = true).toArray
      assertResult(1)(files.length)
      assertResult("input.txt")(new Path(files.head.filePath).getName)
      files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = false).toArray
      assertResult(3)(files.length)
    } finally {
      // Delete the temporary directory
      FileUtils.deleteDirectory(path)
    }
  }

  test("Recursively go into subdirectories") {
    val path = new File(scratchDir, "subdir")
    path.mkdirs()
    try {
      copyResource("/test.points", new File(path, "input.txt"))
      copyResource("/test.points", new File(path, "_input.txt"))
      copyResource("/test.points", new File(path, ".input.txt"))
      val subdir = new File(path, "subdir")
      subdir.mkdirs()
      copyResource("/test.points", new File(subdir, "input2.txt"))
      copyResource("/test.points", new File(subdir, "_input2.txt"))
      // A hidden subdirectory
      val subdir2 = new File(path, "_subdir")
      subdir2.mkdirs()
      copyResource("/test.points", new File(subdir2, "input3.txt"))
      copyResource("/test.points", new File(subdir2, "_input3.txt"))
      val conf = sparkContext.hadoopConfiguration
      val initialPath = new Path(path.getPath)
      val fileSystem = initialPath.getFileSystem(conf)
      var files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = true, recursive = true).toArray
      assertResult(2)(files.length)
      files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = false, recursive = true).toArray
      assertResult(7)(files.length)
      files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = true, recursive = false).toArray
      assertResult(1)(files.length)
      files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, skipHidden = false, recursive = false).toArray
      assertResult(3)(files.length)
    } finally {
      // Delete the temporary directory
      FileUtils.deleteDirectory(path)
    }
  }

  test("Use master file if exists") {
    val conf = sparkContext.hadoopConfiguration
    val initialPath = new Path(locateResource("/test_index").getPath)
    val fileSystem = initialPath.getFileSystem(conf)
    val files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, useMaster = true, skipHidden = false).toArray
    assertResult(2)(files.length)
    assertResult(Array("part-00.csv", "part-11.csv"))(files.map(p => new Path(p.filePath).getName).sorted)
  }

  test("Split files") {
    val conf = sparkContext.hadoopConfiguration
    conf.setLong(SpatialFilePartitioner.MaxSplitSize, 500)
    conf.setBoolean("fs.file.impl.disable.cache", true)
    val initialPath = new Path(locateResource("/features.geojson").getPath)
    val fileSystem = initialPath.getFileSystem(conf)
    val files = new SpatialFilePartitioner(fileSystem, Seq(initialPath).iterator, splitFiles = _ => true).toArray
    assertResult(2)(files.length)
  }
}
