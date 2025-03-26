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

import edu.ucr.cs.bdlab.beast.geolite.{Feature, GeometryReader, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Coordinate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PointSamplerTest extends AnyFunSuite with ScalaSparkTest {

  test("should not fail with empty partitions") {
    val factory = GeometryReader.DefaultGeometryFactory
    val point = factory.createPoint(new Coordinate(3, 4))
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(Feature.create(null, point)), 4)
    val sample = PointSampler.pointSample(features, 1, 1.0)
    assertResult(2)(sample.length)
    assertResult(1)(sample(0).length)
  }

  test("should not fail with null geometries") {
    val factory = GeometryReader.DefaultGeometryFactory
    val point = factory.createPoint(new Coordinate(3, 4))
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(Feature.create(null, point), Feature.create(null, null)))
    val sample = PointSampler.pointSample(features, 1, 1.0)
    assertResult(2)(sample.length)
    assertResult(1)(sample(0).length)
  }
}
