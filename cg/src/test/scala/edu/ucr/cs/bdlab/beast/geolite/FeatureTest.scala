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
package edu.ucr.cs.bdlab.beast.geolite

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Geometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

@RunWith(classOf[JUnitRunner])
class FeatureTest extends AnyFunSuite with ScalaSparkTest {

  test("geometry in middle") {
    val feature = Feature.create(Row.apply(123.25, "name",
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0), "name2"),
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.iGeom == 2)
    assert(feature.length == 4)
    assert(feature.getAs[Double](0) == 123.25)
    assert(feature.getAs[String](1) == "name")
    assert(feature.getAs[Geometry](2) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.getAs[String](3) == "name2")
  }

  test("create from a row without geometry") {
    val feature = Feature.create(
      Row.apply(123.25, "name"),
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.iGeom == 0)
    assert(feature.length == 3)
    assert(feature.getAs[Geometry](0) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.getAs[Double](1) == 123.25)
    assert(feature.getAs[String](2) == "name")
  }

  test("create from a row with geometry") {
    val feature = Feature.create(
      Row.apply(123.25, "name", new PointND(GeometryReader.DefaultGeometryFactory, 2, 5.0, 6.0)),
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
    assert(feature.iGeom == 2)
    assert(feature.length == 3)
    assert(feature.getAs[Double](0) == 123.25)
    assert(feature.getAs[String](1) == "name")
    assert(feature.getAs[Geometry](2) == new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0))
  }

  test("getGeometrySize with null values") {
    val feature = Feature.create(Row.apply(123.25, "name",
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0), "name2", null), null)
    // Just test that it will not throw an exception
    assert(feature.getStorageSize > 0)
  }

  test("default constructor") {
    // Test the default constructor to ensure correct serialization/deserialization
    try {
      val feature = new Feature()
    } catch {
      case e: Throwable => fail("Exception was thrown", e)
    }
  }

  test("serialization/deserialization") {
    // Test the default constructor to ensure correct serialization/deserialization
    val feature = new Feature(Array(123.25, "name",
      new PointND(GeometryReader.DefaultGeometryFactory, 2, 0.0, 1.0), "name2", null,
      Map("name" -> "Lake Michigan", "type" -> "Lake"),
      Seq(1, 2, 3)),
      StructType(Seq(
        StructField("value", DoubleType),  StructField("key", StringType),
        StructField("g", GeometryDataType),
        StructField("name", StringType), StructField("value", IntegerType),
        StructField("tags", MapType(StringType, StringType, true)),
        StructField("classes", ArrayType(IntegerType, true))
      )))
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(feature)
    oos.close()

    // Read it back
    val readFeature: IFeature = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray)).readObject().asInstanceOf[IFeature]

    assertResult(true)(feature.equals(readFeature))
    assertResult(feature.schema)(readFeature.schema)
  }
}
