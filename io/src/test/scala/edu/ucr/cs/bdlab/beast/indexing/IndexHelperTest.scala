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
package edu.ucr.cs.bdlab.beast.indexing

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.JavaPartitionedSpatialRDD
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.generator.{RandomSpatialRDD, UniformDistribution}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.NumPartitions
import edu.ucr.cs.bdlab.beast.io.FeatureReader
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class IndexHelperTest extends AnyFunSuite with ScalaSparkTest {
  test("partition features from Java") {
    val geometryFactor: GeometryFactory = FeatureReader.DefaultGeometryFactory
    val features = sparkContext.parallelize(Seq[IFeature](
      Feature.create(null, new PointND(geometryFactor, 2, 0, 0)),
      Feature.create(null, new PointND(geometryFactor, 2, 1, 1)),
      Feature.create(null, new PointND(geometryFactor, 2, 3, 1)),
      Feature.create(null, new PointND(geometryFactor, 2, 1, 4)),
    ))
    val partitionedFeatures: JavaPartitionedSpatialRDD =
      IndexHelper.partitionFeatures(JavaRDD.fromRDD(features), classOf[RSGrovePartitioner],
      new org.apache.spark.api.java.function.Function[IFeature, Int]() {
        override def call(v1: IFeature): Int = 1
      } , new BeastOptions())
    assert(partitionedFeatures.partitioner.isPresent)
    assert(partitionedFeatures.partitioner.get().isInstanceOf[SpatialPartitioner])
  }

  test("Repartitioning a replicated dataset should not further replicate records") {
    val dataset = new RandomSpatialRDD(sparkContext, UniformDistribution, 10000,
      opts = Seq("maxSize" -> "0.1,0.1", "geometry" -> "box")
    )
    val unitsquare = new EnvelopeNDLite(2, 0, 0, 1, 1)
    val partitioned1 = IndexHelper.partitionFeatures2(dataset, new GridPartitioner(unitsquare, Array(3, 3)))
    val partitioned2 = IndexHelper.partitionFeatures2(partitioned1, new GridPartitioner(unitsquare, Array(5, 5)))
    assert(IndexHelper.runDuplicateAvoidance(partitioned1).count() == 10000)
    assert(IndexHelper.runDuplicateAvoidance(partitioned2).count() == 10000)
  }

  test("Should use the size function when computing the number of partitions") {
    val dataset = new RandomSpatialRDD(sparkContext, UniformDistribution, 16000,
      opts = Seq("maxSize" -> "0.1,0.1", "geometry" -> "box")
    )
    val partitioner = IndexHelper.createPartitioner(dataset, classOf[GridPartitioner],
      NumPartitions(IndexHelper.Size, 1024 * 1024), { f: IFeature => 1024 }, new BeastOptions())
    assert((partitioner.numPartitions - 16).abs < 3)

    val partitioner2 = IndexHelper.createPartitioner(dataset, classOf[RSGrovePartitioner],
      NumPartitions(IndexHelper.Size, 1024 * 1024), { f: IFeature => 1024 }, new BeastOptions())
    assert((partitioner2.numPartitions - 16).abs < 3)
  }

  test("partition features and write index") {
    val geometryFactor: GeometryFactory = FeatureReader.DefaultGeometryFactory
    val features = sparkContext.parallelize(Seq[IFeature](
      Feature.create(null, new PointND(geometryFactor, 2, 0, 0)),
      Feature.create(null, new PointND(geometryFactor, 2, 1, 1)),
      Feature.create(null, new PointND(geometryFactor, 2, 3, 1)),
      Feature.create(null, new PointND(geometryFactor, 2, 1, 4)),
    ))
    val partitionedFeatures: RDD[IFeature] = IndexHelper.partitionFeatures2(features,
      new GridPartitioner(new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0), Array(2, 2)))
    assert(partitionedFeatures.partitioner.isDefined)
    assert(partitionedFeatures.partitioner.get.isInstanceOf[SpatialPartitioner])
    assert(partitionedFeatures.getNumPartitions > 1)
    val outPath = new File(scratchDir, "index")
    IndexHelper.saveIndex2(partitionedFeatures, outPath.getPath, "oformat" -> "wkt(0)")
    val files: Array[File] = outPath.listFiles(file => !file.getName.endsWith(".crc"))
    assertResult(4)(files.length)
  }
}
