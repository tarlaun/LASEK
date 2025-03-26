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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, IntNode, NumericNode, TextNode}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, CoordinateXY, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.ByteArrayInputStream

@RunWith(classOf[JUnitRunner])
class GeoJSONWriterTest extends AnyFunSuite with BeastSpatialTest {
  val geometryFactory = new GeometryFactory

  test("Write points") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("name", StringType),
    ))
    val geometries = Array(
      new PointND(geometryFactory, 2, 0.0, 1.0),
      new PointND(geometryFactory, 2, 3.0, 10.0)
    )
    val testData = new ByteArrayOutputStream()
    val writer = new GeoJSONWriter(testData, schema)
    for (g <- geometries) {
      val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g), "name-value")
      writer.write(row)
    }
    writer.close()

    // Now, read the results back
    val mapper = new ObjectMapper()
    val rootNode = mapper.readTree(new ByteArrayInputStream(testData.toByteArray))
    assertResult("FeatureCollection")(rootNode.get("type").asText())
    val features = rootNode.get("features")
    assert(features.isArray)
    assertResult(geometries.length)(features.size())
    val firstFeature = features.get(0)
    assertResult("Feature")(firstFeature.get("type").asText())
    val firstGeometry = firstFeature.get("geometry")
    assertResult("Point")(firstGeometry.get("type").asText())
    assertResult(2)(firstGeometry.get("coordinates").size())
  }

  test("Attribute types") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("att_s", StringType),
      StructField("att_i", IntegerType),
      StructField("att_d", DoubleType),
      StructField("att_as", ArrayType.apply(StringType)),
      StructField("att_ms", MapType.apply(StringType, StringType)),
    ))
    val testData = new ByteArrayOutputStream()
    val writer = new GeoJSONWriter(testData, schema)
    val g = geometryFactory.createPoint(new Coordinate(1, 2))
    val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g),
      "str", 12, 13.5, Seq("lable1", "label2"), Map("key1" -> "val1", "key2" -> "val2")
    )
    writer.write(row)
    writer.close()

    // Now, read the results back
    val mapper = new ObjectMapper()
    val rootNode = mapper.readTree(new ByteArrayInputStream(testData.toByteArray))
    assertResult("FeatureCollection")(rootNode.get("type").asText())
    val features = rootNode.get("features")
    assert(features.isArray)
    assertResult(1)(features.size)
    val firstFeature = features.get(0)
    val properties = firstFeature.get("properties")
    assertResult(schema.length - 1)(properties.size)
    assertResult(classOf[TextNode])(properties.get(schema(1).name).getClass)
    assertResult(classOf[IntNode])(properties.get(schema(2).name).getClass)
    assert(classOf[NumericNode].isAssignableFrom(properties.get(schema(3).name).getClass))
    assert(classOf[ArrayNode].isAssignableFrom(properties.get(schema(4).name).getClass))
  }

  test("Write with null attributes") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("name1", StringType),
      StructField("name2", StringType),
    ))
    val geometries = Array(
      new PointND(geometryFactory, 2, 0.0, 1.0),
      new PointND(geometryFactory, 2, 3.0, 10.0)
    )
    val testData = new ByteArrayOutputStream()
    val writer = new GeoJSONWriter(testData, schema)
    for (g <- geometries) {
      val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g), null, "name-value")
      writer.write(row)
    }
    writer.close()

    // Now, read the results back
    val mapper = new ObjectMapper()
    val rootNode = mapper.readTree(new ByteArrayInputStream(testData.toByteArray))
    assertResult("FeatureCollection")(rootNode.get("type").asText())
    val features = rootNode.get("features")
    assert(features.isArray)
    assertResult(2)(features.size)
    val firstFeature = features.get(0)
    assertResult(1)(firstFeature.get("properties").size)
  }

  test("Write with all geometry types") {
    def createCS(coords: Double*): CoordinateSequence = {
      val cs = geometryFactory.getCoordinateSequenceFactory.create(coords.length / 2, 2)
      for (i <- 0 until coords.length / 2) {
        cs.setOrdinate(i, 0, coords(2 * i))
        cs.setOrdinate(i, 1, coords(2 * i + 1))
      }
      cs
    }
    var geometries = Array(
      new PointND(geometryFactory, 2, 0.0, 1.0),
      geometryFactory.createLineString(createCS(12.0, 13.5, 15.3, -27.5)),
      new EnvelopeND(geometryFactory, 2, 0.0, 1.0, 25.0, 13.0),
      geometryFactory.createPolygon(createCS(25.0, 33.0, 45.0, 77.0, 10.0, 88.0, 25.0, 33.0)),
      geometryFactory.createMultiLineString(Array(
        geometryFactory.createLineString(createCS(1.0, 2.0, 3.0, 4.0, 1.0, 5.0)),
        geometryFactory.createLineString(createCS(11.0, 12.0, 13.0, 14.0, 11.0, 15.0))
      )),
      geometryFactory.createMultiPolygon(Array(
        geometryFactory.createPolygon(createCS(15.0, 33.0, 25.0, 35.0, -10.0, 7.0, 15.0, 33.0)),
        geometryFactory.createPolygon(createCS(115.0, 33.0, 125.0, 35.0, -110.0, 7.0, 115.0, 33.0))
      )),
      geometryFactory.createMultiPoint(Array(
        geometryFactory.createPoint(new CoordinateXY(100, 20)),
        geometryFactory.createPoint(new CoordinateXY(100, 21)),
        geometryFactory.createPoint(new CoordinateXY(101, 21)),
      ))
    )
    geometries = geometries :+ geometryFactory.createGeometryCollection(geometries)

    val testData = new ByteArrayOutputStream()
    val schema = StructType(Seq(StructField("geometry", GeometryDataType)))
    val writer = new GeoJSONWriter(testData, schema)
    for (g <- geometries) {
      val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g))
      writer.write(row)
    }
    writer.close()

    // Read the results back
    val reader = new GeoJSONReader(new ByteArrayInputStream(testData.toByteArray), schema)
    var i: Int = 0
    for (row <- reader) {
      val g = GeometryDataType.getGeometryFromRow(row, 0)
      assertResult(geometries(i).getArea, s"Error with geometry #$i")(g.getArea)
      assertResult(geometries(i).getLength, s"Error with geometry #$i")(g.getLength)
      i += 1
    }
    assertResult(geometries.length)(i)
  }

  test("Write in JSON line format") {
    val schema = StructType(Seq(
      StructField("geometry", GeometryDataType),
      StructField("name", StringType),
    ))
    val geometries = Array(
      new PointND(geometryFactory, 2, 0.0, 1.0),
      new PointND(geometryFactory, 2, 3.0, 10.0),
      new PointND(geometryFactory, 2, 4.0, 5.0),
    )
    val testData = new ByteArrayOutputStream()
    val writer = new GeoJSONWriter(testData, schema, jsonLineFormat = true, prettyPrint = false)
    for (g <- geometries) {
      val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g), "name-value")
      writer.write(row)
    }
    writer.close()

    val lines: Array[String] = new String(testData.toByteArray).split("\n")
    assertResult(geometries.length)(lines.length)
    assert(!lines(1).startsWith(","))
    assert(!lines(1).endsWith(","))
  }

  test("Write empty properties") {
    val schema = StructType(Seq(StructField("geometry", GeometryDataType)))
    val geometries = Array(
      new PointND(geometryFactory, 2, 0.0, 1.0),
      new PointND(geometryFactory, 2, 3.0, 10.0)
    )
    val testData = new ByteArrayOutputStream()
    val writer = new GeoJSONWriter(testData, schema, jsonLineFormat = true, prettyPrint = false,
      writeEmptyProperties = true)
    for (g <- geometries) {
      val row = InternalRow.apply(GeometryDataType.setGeometryInRow(g))
      writer.write(row)
    }
    writer.close()

    // Now, read the results back
    val lines: Array[String] = new String(testData.toByteArray).split("\n")
    assertResult(geometries.length)(lines.length)
    assert(lines(0).contains("properties"))
  }
}
