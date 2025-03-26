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
package org.apache.spark.beast

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, PointND}
import edu.ucr.cs.bdlab.beast.synopses.{HistogramOP, UniformHistogram}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HistogramOpTest extends AnyFunSuite with ScalaSparkTest {
  test("Compute a sparse histogram") {
    val points: SpatialRDD = sparkContext.parallelize(Array(
      Feature.create(null, new PointND(new GeometryFactory, 2, 1.0, 1.0)),
      Feature.create(null, new PointND(new GeometryFactory, 2, 3.0, 3.0))))
    val mbr = points.summary
    val h: UniformHistogram = HistogramOP.computePointHistogramSparse(points, _=>1, mbr, 4)
    assert(2 == h.getNumPartitions(0))
    assert(2 == h.getNumPartitions(1))
    assert(1 == h.getValue(Array[Int](0, 0), Array[Int](1, 1)))
    assert(0 == h.getValue(Array[Int](1, 0), Array[Int](1, 1)))
    assert(2 == h.getValue(Array[Int](0, 0), Array[Int](2, 2)))
  }

  test("Compute a sparse histogram with out-of-bound points") {
    val points: SpatialRDD = sparkContext.parallelize(Array(
      Feature.create(null, new PointND(new GeometryFactory, 2, 1.0, 1.0)),
      Feature.create(null, new PointND(new GeometryFactory, 2, 3.0, 3.0)),
      Feature.create(null, new PointND(new GeometryFactory, 2, 5.0, 5.0)),
    ))
    val mbr = new EnvelopeNDLite(2, 1.0, 1.0, 3.0, 3.0)
    val h: UniformHistogram = HistogramOP.computePointHistogramSparse(points, _=>1, mbr, 4)
    assert(2 == h.getNumPartitions(0))
    assert(2 == h.getNumPartitions(1))
    assert(1 == h.getValue(Array[Int](0, 0), Array[Int](1, 1)))
    assert(0 == h.getValue(Array[Int](1, 0), Array[Int](1, 1)))
    assert(2 == h.getValue(Array[Int](0, 0), Array[Int](2, 2)))
  }
}
