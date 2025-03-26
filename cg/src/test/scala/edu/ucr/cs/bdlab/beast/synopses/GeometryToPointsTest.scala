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
package edu.ucr.cs.bdlab.beast.synopses

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Coordinate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometryToPointsTest extends AnyFunSuite with ScalaSparkTest {

  test("various types") {
    val factory = GeometryReader.DefaultGeometryFactory
    val point = factory.createPoint(new Coordinate(3, 4))
    assert(new GeometryToPoints(point).length == 1)
    val polygon = factory.createPolygon(Array(new Coordinate(3, 4), new Coordinate(4, 6), new Coordinate(4, 0), new Coordinate(3, 4)))
    assert(new GeometryToPoints(polygon).length == 3)
    assert(new GeometryToPoints(factory.createGeometryCollection(Array(point, polygon))).length == 4)
    assert(new GeometryToPoints(factory.createGeometryCollection(Array(point, factory.createEmpty(2), polygon))).length == 4)

  }
}
