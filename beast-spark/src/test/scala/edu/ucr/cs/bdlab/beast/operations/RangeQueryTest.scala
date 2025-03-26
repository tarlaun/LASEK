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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Envelope, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner

@RunWith(classOf[JUnitRunner])
class RangeQueryTest extends AnyFunSuite with ScalaSparkTest {

  test("RangeQueryFilterPartitionsInMemory") {
    val testFile = makeFileCopy("/test111.points")
    val data: SpatialRDD = sparkContext.readCSVPoint(testFile.getPath)
    val mbr = data.summary
    val gridPartitioner = new GridPartitioner(mbr, Array(2, 2))
    val partitionedData = data.spatialPartition(gridPartitioner)
  assertResult(4)(partitionedData.getNumPartitions)
    assertResult(true)(partitionedData.partitioner.isDefined)
    val filteredData = partitionedData.rangeQuery(
      new EnvelopeND(new GeometryFactory, 2, -100, 30, -90, 40))
    assertResult(1)(filteredData.getNumPartitions)
  }

  test("RangeQueryWithDifferentCRS") {
    val testFile = makeFileCopy("/test111.points")
    val data: SpatialRDD = sparkContext.readCSVPoint(testFile.getPath)
    val mbr = data.summary
    val gridPartitioner = new GridPartitioner(mbr, Array(2, 2))
    val partitionedData = data.spatialPartition(gridPartitioner)
    assert(partitionedData.getNumPartitions == 4)
    assert(partitionedData.partitioner.isDefined)
    val filteredData = partitionedData.rangeQuery(new GeometryFactory(new PrecisionModel(), 3857)
      .toGeometry(new Envelope(-11131949.07932735607028, -10018754.171394621953368,
        3503549.843504375312477, 4865942.279503175057471)))
    assertResult(1)(filteredData.getNumPartitions)
    assertResult(5)(filteredData.count())
  }

  test("RangeQueryFilterPartitionFromDiskIndex") {
    val testFile = makeDirCopy("/sjoinr.grid")
    val data: RDD[IFeature] = sparkContext.readWKTFile(testFile.getPath, 0)
    assertResult(2)(data.getNumPartitions)
    assertResult(true)(data.partitioner.isDefined)
    val filteredData = data.rangeQuery(new EnvelopeND(new GeometryFactory, 2, 0, 0, 5, 5))
    assertResult(1)(filteredData.getNumPartitions)
  }

  test("RangeQueryFilterPruneAllPartitions") {
    val testFile = makeDirCopy("/sjoinr.grid")
    val data: RDD[IFeature] = sparkContext.readWKTFile(testFile.getPath, 0)
    assertResult(2)(data.getNumPartitions)
    assertResult(true)(data.partitioner.isDefined)
    val filteredData = data.rangeQuery(new EnvelopeND(new GeometryFactory, 2, -1, -1, -.5, -.5))
    assertResult(true)(filteredData.isEmpty())
  }
}
