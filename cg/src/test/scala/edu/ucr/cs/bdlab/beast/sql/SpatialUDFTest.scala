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
package edu.ucr.cs.bdlab.beast.sql

import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialUDFTest extends AnyFunSuite with ScalaSparkTest {

  test("various types") {
    val s: SparkSession = sparkSession
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(s)
    import s.implicits._
    val wkts = Seq(
      "POINT(0 1)",
      "LINESTRING(0 1, 2 3, 5 5)",
      "POLYGON((0 1, 2 3, 5 5, 0 1))",
    )
    val dataframe = wkts.toDF("wkt")
    val spatialDataframe = dataframe.selectExpr("ST_FromWKT(wkt) AS geom")
    assertResult(1)(spatialDataframe.schema.length)
    assertResult(GeometryDataType)(spatialDataframe.schema(0).dataType)
    spatialDataframe.count() // Execute the UDF

    val spatialAreas = spatialDataframe.selectExpr("ST_Area(geom) AS area")
    spatialAreas.count()
  }
}
