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
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateSequence, CoordinateXY, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KMLWriterTest extends AnyFunSuite with BeastSpatialTest {
  val geometryFactory = new GeometryFactory

  test("Write points") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("key1", StringType),
      StructField("key2", IntegerType),
      StructField("key3", BooleanType),
    ))
    val geometry = geometryFactory.createPoint(new CoordinateXY(10, 20))
    val row = InternalRow.apply(GeometryDataType.setGeometryInRow(geometry), "value1", 999, true)

    val testData = new ByteArrayOutputStream()
    val writer = new KMLWriter(testData, schema)
    writer.write(row)
    writer.close()

    // Now, read the results back
    val content = new String(testData.toByteArray)
    assert(content.contains("key1"))
    assert(content.contains("value1"))
    assert(content.contains("999"))
  }

  test("Write multipoint") {
    val schema = StructType(Seq(StructField("geometry", GeometryDataType)))
    val geometry = geometryFactory.createMultiPoint(Array(
      geometryFactory.createPoint(new CoordinateXY(10, 20)),
      geometryFactory.createPoint(new CoordinateXY(10, 20)),
    ))
    val row = InternalRow.apply(GeometryDataType.setGeometryInRow(geometry))

    val testData = new ByteArrayOutputStream()
    val writer = new KMLWriter(testData, schema)
    writer.write(row)
    writer.close()

    // Now, read the results back
    val content = new String(testData.toByteArray)
    assert(content.contains("MultiGeometry"))
  }

  test("Write linestring") {
    def createCS(coords: Double*): CoordinateSequence = {
      val cs = geometryFactory.getCoordinateSequenceFactory.create(coords.length / 2, 2)
      for (i <- 0 until coords.length / 2) {
        cs.setOrdinate(i, 0, coords(2 * i))
        cs.setOrdinate(i, 1, coords(2 * i + 1))
      }
      cs
    }
    val schema = StructType(Seq(StructField("geometry", GeometryDataType)))
    val geometry = geometryFactory.createLineString(createCS(
      15.2, 20.3, 20.1, 20.5, 30.3, 30.4, 10.7, 20.9
    ))
    val row = InternalRow.apply(GeometryDataType.setGeometryInRow(geometry))

    val testData = new ByteArrayOutputStream()
    val writer = new KMLWriter(testData, schema)
    writer.write(row)
    writer.close()

    // Now, read the results back
    val content = new String(testData.toByteArray)
    val numericRegex = "[-\\d+\\.]+".r
    val allNumbers = numericRegex.findAllIn(content)
    assert(allNumbers.contains("15.2"))
    assert(allNumbers.contains("20.3"))
    assert(allNumbers.contains("10.7"))
  }
}
