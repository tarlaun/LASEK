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

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJSONReaderTest extends AnyFunSuite with BeastSpatialTest {
  test("Infer schema of a GeoJSON file") {
    val in = getClass.getResourceAsStream("/features.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      assertResult(4)(schema.length)
      assert(schema.contains(StructField("geometry", GeometryDataType)))
      assert(schema.contains(StructField("id", StringType)))
      assert(schema.contains(StructField("prop0", StringType)))
      assert(schema.contains(StructField("prop1", StringType)))
    } finally {
      in.close()
    }
  }

  test("Infer schema of a GeoJSON file in JsonLine format") {
    val in = getClass.getResourceAsStream("/features.geojsonl")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      assertResult(2)(schema.length)
      assert(schema.contains(StructField("geometry", GeometryDataType)))
      assert(schema.contains(StructField("name", StringType)))
    } finally {
      in.close()
    }
  }

  test("Infer schema with no properties") {
    val in = getClass.getResourceAsStream("/allfeatures.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      assertResult(1)(schema.length)
      assert(schema.contains(StructField("geometry", GeometryDataType)))
    } finally {
      in.close()
    }
  }

  test("Read points") {
    var in = getClass.getResourceAsStream("/point.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/point.geojson")
      val reader = new GeoJSONReader(in, schema)
      var numRecords: Int = 0
      for (f <- reader) {
        val c = GeometryDataType.getGeometryFromRow(f, 0).getCoordinate
        assertResult(100.0)(c.getX)
        assertResult(0.0)(c.getY)
        assertResult("value0")(f.getString(1))
        assertResult("value1")(f.getString(2))
        numRecords += 1
      }
      assertResult(2)(numRecords)
    } finally {
      in.close()
    }
  }

  test("Read points with integer coordinates") {
    var in = getClass.getResourceAsStream("/ipoint.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/ipoint.geojson")
      val reader = new GeoJSONReader(in, schema)
      var numRecords: Int = 0
      for (f <- reader) {
        val c = GeometryDataType.getGeometryFromRow(f, 0).getCoordinate
        assertResult(100.0)(c.getX)
        assertResult(0.0)(c.getY)
        numRecords += 1
      }
      assertResult(2)(numRecords)
    } finally {
      in.close()
    }
  }

  test("Read features") {
    var in = getClass.getResourceAsStream("/features.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/features.geojson")
      val reader = new GeoJSONReader(in, schema)
      // Read first record
      assert(reader.hasNext)
      var value: InternalRow = reader.next()
      var geom = GeometryDataType.getGeometryFromRow(value, 0)
      assertResult("LineString")(geom.getGeometryType)
      assertResult(4)(geom.getNumPoints)
      assertResult("id0")(value.getString(schema.fieldIndex("id")))
      // Read second record
      assert(reader.hasNext)
      value = reader.next()
      geom = GeometryDataType.getGeometryFromRow(value, 0)
      assertResult("Polygon")(geom.getGeometryType)
      assertResult(5)(geom.getNumPoints)
      assertResult("id1")(value.getString(schema.fieldIndex("id")))
      assert(!reader.hasNext)
    } finally {
      in.close()
    }
  }

  test("Read all feature type") {
    var in = getClass.getResourceAsStream("/allfeatures.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/allfeatures.geojson")
      val reader = new GeoJSONReader(in, schema)
      // Just count number of records. No need to verify values.
      assertResult(7)(reader.size)
    } finally {
      in.close()
    }
  }

  test("Read complex attributes") {
    var in = getClass.getResourceAsStream("/properties.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      assertResult(MapType.apply(StringType, StringType))(schema(schema.fieldIndex("tags")).dataType)
      assertResult(MapType.apply(StringType, DoubleType))(schema(schema.fieldIndex("values")).dataType)
      assertResult(ArrayType.apply(StringType))(schema(schema.fieldIndex("labels")).dataType)
      assertResult(ArrayType.apply(DoubleType))(schema(schema.fieldIndex("location")).dataType)
      assertResult(NullType)(schema(schema.fieldIndex("other")).dataType)
      in.close()
      in = getClass.getResourceAsStream("/properties.geojson")
      val reader = new GeoJSONReader(in, schema)
      assert(reader.hasNext)
      val value: InternalRow = reader.next()
      val rowWithSchema = new GenericRowWithSchema(value.toSeq(schema).toArray, schema)
      // Check the tags map field
      val tagsMap = rowWithSchema.getAs[Map[String, String]]("tags")
      assertResult(2)(tagsMap.size)
      // Check the values map field
      val valuesMap = rowWithSchema.getAs[Map[String, Double]]("values")
      assertResult(100.0)(valuesMap("area"))
      // Check the string array field
      val labels: Seq[String] = rowWithSchema.getSeq[String](schema.fieldIndex("labels"))
      assertResult(2)(labels.size)
      // Check the double array field
      val location = rowWithSchema.getSeq[Double](schema.fieldIndex("location"))
      assertResult(2)(location.length)
    } finally {
      in.close()
    }
  }

  test("Read corrupted file") {
    var in = getClass.getResourceAsStream("/exampleinput.geojson")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/exampleinput.geojson")
      val reader = new GeoJSONReader(in, schema)
      // Just count number of records. No need to verify values.
      assertResult(1)(reader.size)
    } finally {
      in.close()
    }
  }

  test("Read GeoJSONLine format with feature at the end") {
    var in = getClass.getResourceAsStream("/features.geojsonl")
    try {
      val schema = GeoJSONReader.inferSchema(in, Long.MaxValue)
      in.close()
      in = getClass.getResourceAsStream("/features.geojsonl")
      val reader = new GeoJSONReader(in, schema)
      // Just count number of records. No need to verify values.
      assertResult(3)(reader.size)
    } finally {
      in.close()
    }
  }
}
