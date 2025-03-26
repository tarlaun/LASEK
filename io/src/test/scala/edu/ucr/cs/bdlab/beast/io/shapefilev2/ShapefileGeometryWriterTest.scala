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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{DataInputStream, File, FileInputStream}
import java.util.zip.ZipFile

@RunWith(classOf[JUnitRunner])
class ShapefileGeometryWriterTest extends AnyFunSuite with BeastSpatialTest {
  val factory = new GeometryFactory

  def writeShapefile(geometries: Iterator[Geometry], shpFileName: File) = {
    val writer = new ShapefileGeometryWriter(shpFileName)
    try {
      for (geom <- geometries)
        writer.write(geom)
    } finally {
      try {
        writer.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  test("Creation") {
    val geometries = Array[Geometry](
      new PointND(factory, 2, 0.0, 1.0),
      new PointND(factory, 2, 3.0, 10.0)
    )

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Write envelopes") {
    val geometries = Array[Geometry](
      new EnvelopeND(factory, 2, 0.0, 1.0, 2.0, 3.0),
      new EnvelopeND(factory, 2, 3.0, 10.0, 13.0, 13.0)
    )

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          val e = new EnvelopeND(factory).merge(geom)
          assertResult(geometries(count))(e)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Write contents") {
    val shpFileNames = Array("usa-major-cities"/*, "usa-major-highways"*/)
    for (shpFileName <- shpFileNames) {
      val outDir = new File(scratchDir, "test")
      outDir.mkdirs()
      try {
        val shpFile = new File(outDir, s"$shpFileName.shp")
        val originalShapeFile = s"/$shpFileName/$shpFileName.shp"
        val shpIn = new DataInputStream(getClass.getResourceAsStream(originalShapeFile))
        val shpReader = new ShapefileGeometryReader(shpIn)
        writeShapefile(shpReader, shpFile)
        shpReader.close()

        // Now read the file back and make sure it is byte-to-byte identical to the input
        assertContentsEqual(locateResource(originalShapeFile), shpFile)
        assertContentsEqual(locateResource(s"/$shpFileName/$shpFileName.shx"), new File(outDir, s"$shpFileName.shx"))
      } finally {
        if (outDir.exists())
          FileUtils.deleteDirectory(outDir)
      }
    }
  }

  test("Contents for polygons") {
    val outDir = new File(scratchDir, "test")
    outDir.mkdirs()
    try {
      val shpFile = new File(outDir, s"sampleout.shp")
      val zipIn = new ZipFile(locateResource("/simpleshape.zip"))
      val shpIn = new DataInputStream(zipIn.getInputStream(zipIn.getEntry("simpleshape.shp")))
      val shpReader = new ShapefileGeometryReader(shpIn)
      writeShapefile(shpReader, shpFile)
      shpReader.close()

      // Now read the file back and make sure it is byte-to-byte identical to the input
      val expectedResult = zipIn.getInputStream(zipIn.getEntry("simpleshape.shp"))
      val actualResult = new FileInputStream(shpFile)
      try {
        assertEquals(expectedResult, actualResult)
      } finally {
        expectedResult.close()
        actualResult.close()
      }
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Contents for polylines") {
    val geometries = Array[Geometry](
      factory.createLineString(Array(new Coordinate(0, 0), new Coordinate(2, 3)))
    )

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(geometries(count))(geom)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }


  test("Contents for MultiPolygons with one polygon") {
    val polygon = factory.createPolygon(Array(
      new Coordinate(0, 0),
      new Coordinate(2, 3),
      new Coordinate(3, 0),
      new Coordinate(0, 0),
    ))
    val geometries = Array[Geometry](factory.createMultiPolygon(Array(polygon)))

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(polygon)(geom)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Contents for MultiPolygons with two polygons") {
    val geometries = Array[Geometry](
      factory.createMultiPolygon(Array(
        factory.createPolygon(Array(
          new Coordinate(0, 0),
          new Coordinate(2, 3),
          new Coordinate(3, 0),
          new Coordinate(0, 0),
        )),
        factory.createPolygon(Array(
          new Coordinate(10, 0),
          new Coordinate(12, 3),
          new Coordinate(13, 0),
          new Coordinate(10, 0)
        ))
      ))
    )

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(geometries(count))(geom)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Contents for MultiPoint") {
    val geometries = Array[Geometry](
      factory.createMultiPoint(Array(
        factory.createPoint(new Coordinate(100, 20)),
        factory.createPoint(new Coordinate(120, 20)),
      ))
    )

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(geometries(count))(geom)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }

  test("Contents for Polygon") {
    val polygon = factory.createPolygon(Array(
      new Coordinate(0, 0),
      new Coordinate(2, 3),
      new Coordinate(3, 0),
      new Coordinate(0, 0),
    ))
    val geometries = Array[Geometry](polygon)

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(geometries(count))(geom)
          count += 1
        }
        assertResult(geometries.length)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }


  test("Contents for GeometryCollection") {
    val polygon1 = factory.createPolygon(Array(
      new Coordinate(0, 0), new Coordinate(2, 3), new Coordinate(3, 0), new Coordinate(0, 0)
    ))
    val polygon2 = factory.createPolygon(Array(
      new Coordinate(10, 0), new Coordinate(12, 3), new Coordinate(13, 0), new Coordinate(10, 0)
    ))
    val geometries = Array[Geometry](factory.createGeometryCollection(Array(polygon1, polygon2)))

    val outDir = new File(scratchDir, "test")
    try {
      outDir.mkdirs()
      val shpFileName = new File(outDir, "test.shp")
      writeShapefile(geometries.iterator, shpFileName)

      val shxFileName = new File(outDir, "test.shx")
      assert(shpFileName.exists(), "Shapefile not found")
      assert(shxFileName.exists(), "Shape index file not found")

      // Read the file back
      val shpIn = new DataInputStream(new FileInputStream(shpFileName))
      val reader = new ShapefileGeometryReader(shpIn)
      try {
        var count: Int = 0
        for (geom <- reader) {
          assertResult(geometries(0).getGeometryN(count))(geom)
          count += 1
        }
        assertResult(geometries(0).getNumGeometries)(count)
      } finally reader.close()
    } finally {
      if (outDir.exists())
        FileUtils.deleteDirectory(outDir)
    }
  }
}
