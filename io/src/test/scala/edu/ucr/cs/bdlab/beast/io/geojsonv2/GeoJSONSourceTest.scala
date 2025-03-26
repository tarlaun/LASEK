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
package edu.ucr.cs.bdlab.beast.io.geojsonv2

import edu.ucr.cs.bdlab.beast.geolite.{Feature, PointND}
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.shaded.org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, FileInputStream, FileOutputStream, FilenameFilter, PrintStream}
import java.util.Random

@RunWith(classOf[JUnitRunner])
class GeoJSONSourceTest extends AnyFunSuite with BeastSpatialTest {
  test("schema of a geojson file") {
    val input = locateResource("/linestrings.geojson")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    val schema = dataframe.schema
    assertResult(4)(schema.size)
    assertResult(2)(dataframe.count())
  }

  test("read compressed file") {
    val input = locateResource("/allfeatures.geojson.bz2")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    assertResult(7)(dataframe.count())
  }

  test("Big compressed file") {
    val inFile = new File(scratchDir, "test.geojson.bz2")
    try {
      // TODO: Use the new GeoJSONWriter
      val out = new BZip2CompressorOutputStream(new FileOutputStream(inFile.toString))
      val writer = new GeoJSONFeatureWriter
      writer.initialize(out, new Configuration)
      val numPoints = 10000
      var random = new Random(0)
      for (_ <- 0 until numPoints) {
        val p = new PointND(new GeometryFactory, 2)
        p.setCoordinate(0, random.nextDouble)
        p.setCoordinate(1, random.nextDouble)
        val f = Feature.create(null, p)
        writer.write(f)
      }
      writer.close()

      // Now read the file in two splits
      val fileLength: Long = new File(inFile.toString).length
      random = new Random(0)
      val dataframe = sparkSession.read
        .option(FileInputFormat.SPLIT_MAXSIZE, fileLength / 2 + 1)
        .option(FileInputFormat.SPLIT_MINSIZE, fileLength / 2)
        .format("geojson").load(inFile.getPath)
      assertResult(2)(dataframe.rdd.getNumPartitions)
      assertResult(numPoints)(dataframe.count())
    } finally {
      inFile.delete()
    }
  }

  test("small GZ compressed file") {
    val input = locateResource("/MSBuilding.geojson.gz")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    assertResult(1)(dataframe.count())
  }

  test("Read a split that ends with a start object token") {
    // In this test, the split ends exactly at the START_OBJECT token "{" of a feature and it should read it.
    val input = locateResource("/features.geojson")
    for (splitPos <- 50 to 70) {
      val dataframe = sparkSession.read
        .option(FileInputFormat.SPLIT_MAXSIZE, splitPos)
        .format("geojson").load(input.getPath)
      assertResult(2)(dataframe.count())
    }
  }

  test("Read three splits") {
    val testFile = new File(scratchDir, "input.geojson")
    try {
      val out = new PrintStream(new FileOutputStream(testFile))
      val expectedNumRecords = 4
      out.println("""{"type":"FeatureCollection","features":[""")
      for (_ <- 1 until expectedNumRecords) {
        out.println("""{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-70,453],[-75,51],[150,30],[-70,453]]]},"properties":{}},""")
      }
      out.println("""{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-70,453],[-75,51],[150,30],[-70,453]]]},"properties":{}}""")
      out.println("]}")
      out.close()

      val fileLength = testFile.length()
      val dataframe = sparkSession.read
        .option(FileInputFormat.SPLIT_MAXSIZE, fileLength / 3)
        .format("geojson").load(testFile.getPath)
      assertResult(expectedNumRecords)(dataframe.count())
    } finally {
      testFile.delete()
    }
  }

  test("Write dataframe to geojson") {
    val input = locateResource("/linestrings.geojson")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    val outputPath = new File(scratchDir, "test_output")
    dataframe.write.format("geojson").mode(SaveMode.Overwrite).save(outputPath.getPath)
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(1)(files.length)
  }

  test("Write GeoJSON compressed file") {
    val input = locateResource("/linestrings.geojson")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    val outputPath = new File(scratchDir, "test_output.bz2")
    dataframe.write.format("geojson").mode(SaveMode.Overwrite).save(outputPath.getPath)
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(1)(files.length)
    assert(files(0).getPath.endsWith(".geojson.bz2"))
    // Try to read it back and confirm it is compressed
    val in = new BZip2CompressorInputStream(new FileInputStream(files(0)))
    in.read()
    in.close()
  }
}
