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

import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileGeometryWriter
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Envelope, GeometryFactory, LineString}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{DataInputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.ZipFile

@RunWith(classOf[JUnitRunner])
class ShapefileGeometryReaderTest extends AnyFunSuite with BeastSpatialTest {
  test("Read header") {
    val in = new DataInputStream(getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.shp"))
    val reader = new ShapefileGeometryReader(in, 4326)
    assertResult(1000)(reader.header.version)
    assertResult(3460 / 2)(reader.header.fileLength)
    assertResult(1)(reader.header.shapeType)
    assert((reader.header.getMinX - (-157.8)).abs < 1E-1)
    assert((reader.header.getMaxX - (-69.8)).abs < 1E-1)
  }

  test("Read points") {
    val in = new DataInputStream(getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.shp"))
    val reader = new ShapefileGeometryReader(in, 4326)
    try {
      var recordCount = 0
      for (g <- reader) {
        assertResult("Point")(g.getGeometryType)
        assertResult(2)(GeometryHelper.getCoordinateDimension(g))
        recordCount += 1
      }
      assertResult(120)(recordCount)
    } finally {
      reader.close()
    }
  }

  test("Read from zip file") {
    val (zipFile, reader) = readFromZip("/usa-major-cities.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        assertResult("Point")(g.getGeometryType)
        recordCount += 1
      }
      assertResult(120)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  def readFromZip(zipFileName: String, filterRange: Envelope = null): (ZipFile, ShapefileGeometryReader) = {
    import scala.collection.JavaConverters._
    val zipFile = new ZipFile(locateResource(zipFileName))
    val shpEntry = zipFile.entries().asScala.find(_.getName.endsWith(".shp"))
    if (shpEntry.isEmpty)
      return null
    val shpIn = new DataInputStream(zipFile.getInputStream(shpEntry.get))
    val reader = new ShapefileGeometryReader(shpIn, 4326, filterRange = filterRange)
    (zipFile, reader)
  }

  test("Read Line MZ") {
    val (zipFile, reader) = readFromZip("/polylinemz.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        assert(g.getGeometryType.toLowerCase().contains("linestring"))
        recordCount += 1
      }
      assertResult(1)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Filter polygons") {
    val filterRange = new Envelope(-71.449, -71.286, 41.416, 41.632)
    val (zipFile, reader) = readFromZip("/tl_2017_44_elsd.zip", filterRange)
    try {
      val recordCount = reader.size
      assertResult(1)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Read polygons with holes") {
    val (zipFile, reader) = readFromZip("/simpleshape.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        assert(g.getGeometryType.toLowerCase().contains("polygon"))
        recordCount += 1
      }
      assertResult(2)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Read bad polygons") {
    val (zipFile, reader) = readFromZip("/badpolygon.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        assert(g.getGeometryType.toLowerCase().contains("polygon"))
        recordCount += 1
      }
      assertResult(1)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Read lines with M") {
    val (zipFile, reader) = readFromZip("/linetest.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        assert(g.getGeometryType.toLowerCase().contains("linestring"))
        recordCount += 1
        val l: LineString = g.asInstanceOf[LineString]
        assertResult(4)(l.getNumPoints)
        assertResult(1.1)(l.getCoordinateN(0).getM)
        assertResult(2.2)(l.getCoordinateN(1).getM)
        assertResult(3.3)(l.getCoordinateN(2).getM)
        assertResult(4.4)(l.getCoordinateN(3).getM)
      }
      assertResult(1)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Read file with empty geometries") {
    val (zipFile, reader) = readFromZip("/test_empty.zip")
    try {
      var recordCount = 0
      for (g <- reader) {
        if (reader.currentRecordNumber == 2) {
          assert(g.isEmpty)
        } else {
          assertResult("Point")(g.getGeometryType)
        }
        recordCount += 1
      }
      assertResult(2)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Skip empty geometries when filtering") {
    val filterRange = new Envelope(0, 10, 0, 10)
    val (zipFile, reader) = readFromZip("/test_empty.zip", filterRange)
    try {
      var recordCount = 0
      for (g <- reader) {
        assertResult("Point")(g.getGeometryType)
        recordCount += 1
      }
      assertResult(1)(recordCount)
    } finally {
      reader.close()
      zipFile.close()
    }
  }

  test("Read empty geometries") {
    val factory = new GeometryFactory
    val geometries = Array(factory.createPoint,
      factory.createLineString,
      factory.createPolygon,
      factory.createMultiLineString,
      factory.createMultiPolygon,
      factory.createMultiPoint)
    for (expected <- geometries) {
      val shpfile = new File(scratchDir, "file.shp")
      val writer = new ShapefileGeometryWriter(shpfile)
      writer.write(expected)
      writer.close()
      // Read it back
      val in = new DataInputStream(new FileInputStream(shpfile))
      val reader = new ShapefileGeometryReader(in, 4326)
      assert(reader.hasNext, "Must have one record")
      val actual = reader.next
      assert(actual.isEmpty, s"Geometry '$actual' should be empty")
      assert(!reader.hasNext)
      reader.close()
    }
  }


  test("Skip file if does not mach the range") {
    // Make a copy of a file and keep only the header
    // It should find from the header that nothing matches and skips the entire file
    // Otherwise, it will throw an exception because the file is corrupted
    val corruptedFile = new File(scratchDir, "file.shp")
    try {
      val originalCopy = getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.shp")
      val buffer = new Array[Byte](108)
      originalCopy.read(buffer)
      val corruptedCopy = new FileOutputStream(corruptedFile)
      corruptedCopy.write(buffer)
      corruptedCopy.close()
      originalCopy.close()
      // Now, try to read it with a filter that skips the entire file
      // File Extent: -157.8234361548175002,21.3057816390303287 : -69.7652635730557904,61.1919001760917922

      val in = new DataInputStream(new FileInputStream(corruptedFile))
      val reader = new ShapefileGeometryReader(in, 4326, filterRange = new Envelope(0, 10, 0, 10))
      assert(!reader.hasNext, "File should not match any records")
      reader.close()
    } finally {
      if (corruptedFile.exists())
        corruptedFile.delete()
    }
  }
}
