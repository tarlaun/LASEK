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
package edu.ucr.cs.bdlab.beast.io.kmlv2

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.FileUtils
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateXY, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io._
import java.util.zip.ZipFile

@RunWith(classOf[JUnitRunner])
class KMLSourceTest extends AnyFunSuite with BeastSpatialTest {
  val geometryFactory = new GeometryFactory

  test("Write a KML file") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("key1", StringType),
      StructField("key2", IntegerType),
      StructField("key3", BooleanType),
    ))
    val geometry = geometryFactory.createPoint(new CoordinateXY(10, 20))
    val row = Row.apply(geometry, "value1", 999, true)
    val df = sparkSession.createDataFrame(sparkContext.parallelize(Seq(row)), schema)

    val outputPath = new File(scratchDir, "file1")
    try {
      df.write.format("kml").save(outputPath.getPath)
      // Check the output
      val files = outputPath.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.startsWith("part")
      })
      assertResult(1)(files.length)
      assert(files.head.getPath.endsWith(".kml"))
    } finally {
      FileUtils.deleteDirectory(outputPath)
    }
  }


  test("Write a KMZ file") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("key1", StringType),
      StructField("key2", IntegerType),
      StructField("key3", BooleanType),
    ))
    val geometry = geometryFactory.createPoint(new CoordinateXY(10, 20))
    val row = Row.apply(geometry, "value1", 999, true)
    val df = sparkSession.createDataFrame(sparkContext.parallelize(Seq(row)), schema)

    val outputPath = new File(scratchDir, "file1")
    try {
      df.write.format("kmz").save(outputPath.getPath)
      // Check the output
      val files = outputPath.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.startsWith("part")
      })
      assertResult(1)(files.length)
      assert(files.head.getPath.endsWith(".kmz"))
      // Verify that it is a ZIP file
      val kmzFile = new ZipFile(files.head)
      val entries = kmzFile.entries()
      assert(entries.hasMoreElements)
      val entry = entries.nextElement()
      assertResult("index.kml")(entry.getName)
      kmzFile.close()
    } finally {
      FileUtils.deleteDirectory(outputPath)
    }
  }
}
