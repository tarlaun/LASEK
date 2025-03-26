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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.io.PushDownSpatialFilter
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, FileOutputStream, FilenameFilter, PrintStream}
import java.util.zip.ZipFile
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter

@RunWith(classOf[JUnitRunner])
class ShapefileSourceTest extends AnyFunSuite with BeastSpatialTest {
  override def sparkSession: SparkSession = {
    val session = super.sparkSession
    if (!session.experimental.extraOptimizations.contains(PushDownSpatialFilter))
      session.experimental.extraOptimizations = session.experimental.extraOptimizations :+ PushDownSpatialFilter
    session
  }

  test("Schema of a Shapefile and record count from a compressed file") {
    val input = locateResource("/tl_2017_44_elsd.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val schema = dataframe.schema
    assertResult(15)(schema.size)
    assertResult(5)(dataframe.count())
  }

  test("Read directory") {
    val input = locateResource("/usa-major-cities")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val schema = dataframe.schema
    assertResult(5)(schema.size)
    val records = dataframe.collect()
    assert(records.forall(x => x.get(1) != null))
    assertResult(120)(records.length)
  }

  test("Read non compressed Shapefile with empty trail") {
    val input = locateResource("/linetest")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val records = dataframe.collect()
    assertResult(1)(records.length)
    assert(records(0).get(1) != null)
  }

  test("Read compressed sparse file") {
    val input = locateResource("/sparselines.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val records = dataframe.collect()
    assertResult(2)(records.length)
    assert(records.forall(x => x.get(1) != null))
  }

  test("Read compressed file with upper-case extension") {
    val input = locateResource("/linetest2.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val records = dataframe.collect()
    assertResult(1)(records.length)
  }

  test("Read compressed zip file") {
    val input = locateResource("/usa-major-cities.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    assertResult(120)(dataframe.count())
  }

  test("Read compressed ZIP file with multiple shapefiles") {
    val input = locateResource("/points.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    assertResult(6)(dataframe.count())
  }

  test("Filter push down with ST_Intersects should give the correct result") {
    val input = locateResource("/usa-major-cities")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val filtered = dataframe.filter("ST_Intersects(geometry, ST_CreateBox(-160, 18, -140, 64))")
    val names = filtered.select("NAME")
    val results = names.orderBy("NAME").collect().map(_.getAs[String](0))
    assertResult(Array("Anchorage", "Honolulu"))(results)
  }

  test("Filter push down should skip non-relevant files") {
    // This test creates a fake index with a master file and two files, a valid file and a corrupted file
    // The master file indicates that the valid file matches the query while the corrupted file does not
    // If filter push-down works correctly, then the corrupted file should never be read, hence, no errors
    val input = new File(scratchDir, "test")
    try {
      input.mkdirs()
      val validFile = new File(input, "points.zip")
      copyResource("/points.zip", validFile)
      val corruptedFile = new File(input, "points2.zip")
      copyResource("/ipoint.geojson", corruptedFile)
      val masterFile = new File(input, "_master")
      val masterOut = new PrintStream(masterFile)
      masterOut.println("ID\tFile Name\tRecord Count\tData Size\tGeometry\txmin\tymin\txmax\tymax")
      masterOut.println(s"1\t${validFile.getName}\t1\t${validFile.length()}\tnull\t-125\t67\t-115\t74")
      masterOut.println(s"1\t${corruptedFile.getName}\t1\t${corruptedFile.length()}\tnull\t0\t0\t10\t10")
      masterOut.close()

      val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
      val filtered = dataframe.filter("ST_Intersects(geometry, ST_CreateBox(-125, 67, -115, 74))")
      assertResult(6)(filtered.count())
    } finally {
      if (input.exists())
        FileUtils.deleteDirectory(input)
    }
  }

  test("Push-down projection should skip reading the Shapefile when the geometry attribute is not needed") {
    // To run this test, we place a corrupted .shp file and a valid .dbf file in a directory
    // Then, we try to run a query on this data. If the entire .shp file is skipped, then no error should happen
    // and the valid result should be returned
    val input = new File(scratchDir, "test")
    try {
      input.mkdirs()
      // Make a valid copy of the DBF file
      makeResourceCopy("/usa-major-cities/usa-major-cities.dbf", new File(input, "data.dbf"))
      // Make a corrupted copy of the .shp file
      val corruptedShpFile = new File(input, "data.shp")
      val originalShpFile = getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.shp")
      val corruptedWrite = new FileOutputStream(corruptedShpFile)
      val data = new Array[Byte](100)
      originalShpFile.read(data)
      corruptedWrite.write(data)
      originalShpFile.close()
      corruptedWrite.close()

      // Now try to read the file and run a query that does not touch the geometry attribute
      val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
      val filtered = dataframe.filter("NAME='Riverside'")
      assertResult(1)(filtered.count())
    } finally {
      if (input.exists())
        FileUtils.deleteDirectory(input)
    }
  }

  test("Projection push-down prune all attributes") {
    val input = locateResource("/usa-major-cities")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val schema = dataframe.schema
    assertResult(120)(dataframe.count)
  }

  test("Projection push-down prune non-geometric attributes") {
    // To run this test, we place a corrupted .dbf file and a valid .shp file in a directory
    // Notice that the header must be valid for inferSchema to work
    // Then, we try to run a query on this data. If the entire .dbf file is skipped, then no error should happen
    // and the valid result should be returned
    val input = new File(scratchDir, "test")
    try {
      input.mkdirs()
      // Make a valid copy of the SHP file
      makeResourceCopy("/usa-major-cities/usa-major-cities.shp", new File(input, "data.shp"))
      // Make a corrupted copy of the .dbf file
      val corruptedDBFFile = new File(input, "data.dbf")
      val originalDBFFile = getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.dbf")
      val corruptedWrite = new FileOutputStream(corruptedDBFFile)
      val data = new Array[Byte](200)
      originalDBFFile.read(data)
      corruptedWrite.write(data)
      originalDBFFile.close()
      corruptedWrite.close()

      // Now try to read the file and run a query that does not touch the geometry attribute
      val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
      val filtered = dataframe.filter("geometry IS NOT NULL")
      assertResult(120)(filtered.count())
    } finally {
      if (input.exists())
        FileUtils.deleteDirectory(input)
    }
  }

  test("write dataframe to shapefile") {
    val input = locateResource("/tl_2017_44_elsd.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val outputPath = new File(scratchDir, "shapefile_folder")
    dataframe.write.format("shapefile").mode(SaveMode.Append).save(outputPath.getPath)
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(1)(files.length)

    // Verify .shp file is present inside .zip file
    files.foreach(f => {
      val zf = new ZipFile(f)
      val shpFiles = zf.entries().asScala.filter( ze => ze.getName.endsWith(".shp") )
      assertResult(1)(shpFiles.length)
      val zipFiles = zf.entries().asScala.filter( ze => ze.getName.endsWith(".zip") )
      assertResult(0)(zipFiles.length)
      zf.close()
    })

    dataframe.write.format("shapefile").mode(SaveMode.Append).save(outputPath.getPath)
    val files2 = new File(outputPath.getPath).listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(2)(files2.length)

    dataframe.write.format("shapefile").mode(SaveMode.Overwrite).save(outputPath.getPath)
    val files3 = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(1)(files3.length)

    var errorThrown: Boolean = false
    try {
      dataframe.write.format("shapefile").mode(SaveMode.ErrorIfExists).save(outputPath.getPath)
    } catch {
      case _: Exception => errorThrown = true
    }
    assert(errorThrown)
    val files4 = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith("part")
    })
    assertResult(1)(files4.length)
  }

  test("write dataframe to shapefile and verify schema") {
    val input = locateResource("/tl_2017_44_elsd.zip")
    val dataframe = sparkSession.read.format("shapefile").load(input.getPath)
    val outputPath = new File(scratchDir, "shapefile_folder")
    dataframe.write.format("shapefile").mode(SaveMode.Append).save(outputPath.getPath)
    val written_dataframe = sparkSession.read.format("shapefile").load(outputPath.getPath)
    written_dataframe.printSchema()
  }
}
