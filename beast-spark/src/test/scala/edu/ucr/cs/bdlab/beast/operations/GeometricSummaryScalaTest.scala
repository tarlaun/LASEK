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
package edu.ucr.cs.bdlab.beast.operations

import java.io.File
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometricSummaryScalaTest extends AnyFunSuite with ScalaSparkTest {

  test("Summary with column names") {
    val inputfile = locateResource("/test.partitions")
    val opts = new BeastOptions().set("iformat", "wkt(Geometry)")
      .set("skipheader", true)
      .set("separator", "\t")
    val summary = GeometricSummary.run(opts, Array(inputfile.getPath), null, sparkContext).asInstanceOf[Summary]
    assert(summary.numFeatures == 44)
  }

  test("Summary with input format and no output format") {
    val inputfile = locateResource("/test.partitions")
    val opts = new BeastOptions().set("iformat", "wkt(Geometry)")
        .set("skipheader", true)
        .set("separator", "\t")
        .set("oformat", "invalid")
    val summary = GeometricSummary.run(opts, Array(inputfile.getPath), null, sparkContext).asInstanceOf[Summary]
    assert(summary.numFeatures == 44)
  }
}
