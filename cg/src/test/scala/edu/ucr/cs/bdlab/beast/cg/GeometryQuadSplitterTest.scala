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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{Feature, GeometryReader}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, Envelope}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometryQuadSplitterTest extends AnyFunSuite with ScalaSparkTest {

  test("complex geometry") {
    val wkt: String = readTextResource("/test.wkt")(0)
    val geom = new WKTReader().read(wkt)
    val splitter = new GeometryQuadSplitter(geom, 100)
    val splits = splitter.toArray
    assert(splits.length > 0)
    assert(splits(0) != null)
    // Assert that the point 27o 27' 27" and 27o 27' 27" is in at least one polygon
    val coord = 27.0 + 27.0 /60 + 27.0 / 60 / 60
    val point = geom.getFactory.createPoint(new Coordinate(coord, coord))
    assert(splits.exists(g => g.contains(point)), "Test point 27 27 27 is not in any polygon")
  }

  test("split across the dateline") {
    val geometry = GeometryReader.DefaultGeometryFactory.createPolygon(
      Array(new Coordinate(170, 50), new Coordinate(-170, 60), new Coordinate(-170, 50), new Coordinate(170, 50))
    )
    val split = GeometryQuadSplitter.splitGeometryAcrossDateLine(geometry)
    assertResult(2)(split.getNumGeometries)
    assertResult(9)(split.getNumPoints)
  }
}
