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
package edu.ucr.cs.bdlab.beast.generator

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParcelGeneratorTest extends AnyFunSuite with ScalaSparkTest {
  test("generate different numbers") {
    for (cardinality <- Seq(7, 15, 16, 1, 123)) {
      val partition = new RandomSpatialPartition(0, cardinality,
        2, 0,
        Seq(ParcelDistribution.SplitRange -> "0.1", ParcelDistribution.Dither -> "0.4"))
      val records = new ParcelGenerator(partition)
      assert(records.length == cardinality)
    }
  }

  test("no empty features") {
    val cardinality = 1000
    val partition = new RandomSpatialPartition(0, cardinality,
      2, 0,
      Seq(ParcelDistribution.SplitRange -> "0.1", ParcelDistribution.Dither -> "0.4"))
    val records = new ParcelGenerator(partition).toArray
    assert(!records.exists(f => f.getGeometry.isEmpty),
      s"Record #${records.indexWhere(f => f.getGeometry.isEmpty)} is empty")
  }
}
