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
import org.locationtech.jts.geom.Envelope
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlaneSweepSpatialJoinIteratorTest extends AnyFunSuite with ScalaSparkTest {

  test("touching geometries") {
    val factory = GeometryReader.DefaultGeometryFactory
    val s1 = Array(
      Feature.create(null, factory.toGeometry(new Envelope(0, 1, 0, 1))),
      Feature.create(null, factory.toGeometry(new Envelope(1, 2, 0, 1)))
    )
    val s2 = Array(
      Feature.create(null, factory.toGeometry(new Envelope(0, 1, 0.5, 1))),
      Feature.create(null, factory.toGeometry(new Envelope(1, 2, 0.5, 1)))
    )
    val joinResults = new PlaneSweepSpatialJoinIterator(s1, s2, null)
    assert(joinResults.size == 4)
  }
}
