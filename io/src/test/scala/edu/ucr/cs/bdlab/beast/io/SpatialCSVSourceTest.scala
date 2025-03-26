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
package edu.ucr.cs.bdlab.beast.io

import org.apache.spark.beast.sql.GeometryDataType
import edu.ucr.cs.bdlab.beast.geolite.GeometryType
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.locationtech.jts.geom.Geometry

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class SpatialCSVSourceTest extends AnyFunSuite with BeastSpatialTest {
  // Support quotes around fields that might contain the separator
  // Support three-dimensional points and measures

  test("parse CSV points with no header") {
    val input = locateResource("/test.points")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "point",
      SpatialCSVSource.DimensionColumns -> "0,1", "delimiter" -> ",").toMap.asJava)
    val schema = dataframe.schema
    assertResult(1)(schema.size)
    assertResult(GeometryDataType)(schema.head.dataType)
    assertResult(11)(dataframe.count())
  }

  test("parse TSV with WKT geometries and no header") {
    val input = locateResource("/test-geometries.csv")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "wkt",
      SpatialCSVSource.DimensionColumns -> "1", "delimiter" -> "\t").toMap.asJava)
    val schema = dataframe.schema
    assertResult(3)(schema.size)
    assertResult(GeometryDataType)(schema(1).dataType)
    assertResult(2)(dataframe.count())
  }

  test("parse CSV points with header") {
    val input = locateResource("/test-header.csv")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "point",
      "header" -> "true", SpatialCSVSource.DimensionColumns -> "id,value", "delimiter" -> ",").toMap.asJava)
    val schema = dataframe.schema
    assertResult(2)(schema.size)
    assertResult(GeometryDataType)(schema.head.dataType)
    assertResult(2)(dataframe.count())
  }

  test("parse TSV with WKT geometries and header") {
    val input = locateResource("/test_wkt.csv")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "wkt",
      "header" -> "true", SpatialCSVSource.DimensionColumns -> "geom", "delimiter" -> "\t").toMap.asJava)
    val schema = dataframe.schema
    assertResult(3)(schema.size)
    assertResult(GeometryDataType)(schema(1).dataType)
    assertResult("geom")(schema(1).name)
    assertResult(2)(dataframe.count())
  }

  test("parse envelopes as envelopes") {
    val input = locateResource("/test.partitions")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "envelope",
      "header" -> "true", SpatialCSVSource.DimensionColumns -> "xmin,ymin,xmax,ymax", "delimiter" -> "\t").toMap.asJava)
    val schema = dataframe.schema
    assertResult(6)(schema.size)
    assertResult(GeometryDataType)(schema(5).dataType)
    assertResult("envelope")(schema(5).name)
    assertResult(44)(dataframe.count())
    assertResult(GeometryType.EnvelopeName)(dataframe.first().getAs[Geometry](5).getGeometryType)
  }

  test("parse envelopes as geometries") {
    val input = locateResource("/test.partitions")
    val dataframe = SpatialCSVSource.read(sparkSession, input.getPath, Seq(SpatialCSVSource.GeometryType -> "envelope",
      "header" -> "true", SpatialCSVSource.DimensionColumns -> "xmin,ymin,xmax,ymax", "delimiter" -> "\t",
      SpatialCSVSource.JTSEnvelope -> "true").toMap.asJava)
    val schema = dataframe.schema
    assertResult(6)(schema.size)
    assertResult(GeometryDataType)(schema(5).dataType)
    assertResult("envelope")(schema(5).name)
    assertResult(44)(dataframe.count())
    assertResult(GeometryType.PolygonName)(dataframe.first().getAs[Geometry](5).getGeometryType)
  }
}
