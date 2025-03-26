/*
 * Copyright 2018 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{PartitionedSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.NumPartitions

/**
 * Mixin to add IO operations to SpatialRDD
 */
trait IndexMixin {
  implicit class IndexMixinFunctions(rdd: SpatialRDD) {
    /**
     * Partition a set of features according to a created spatial partitioner
     *
     * @param spatialPartitioner the partitioner for the data
     * @return partitioned records
     */
    @deprecated("Use spatialPartition", "0.9.2")
    def partitionBy(spatialPartitioner: SpatialPartitioner): PartitionedSpatialRDD =
      IndexHelper.partitionFeatures(rdd, spatialPartitioner)

    /**
     * Partition a set of features according to a created spatial partitioner
     * @param spatialPartitioner an initialized spatial partitioner
     * @return the same set of input features after partitioned
     */
    def spatialPartition(spatialPartitioner: SpatialPartitioner): SpatialRDD =
      IndexHelper.partitionFeatures2(rdd, spatialPartitioner)

    /**
     * Partitions this RDD using the given partitioner type. If the desired number of partitions is not provided,
     * the output number of partitions will be roughly equal to the number of partitions in the input RDD.
     * @param partitionerKlass the class of the partitioner
     * @param numPartitions the desired number of partitions. If not set, the number of partitions of the input RDD is used.
     * @return a new RDD that is partitioned using the given partitioner class
     */
    @deprecated("Use spatialPartition", "0.9.2")
    def partitionBy(partitionerKlass: Class[_ <: SpatialPartitioner], numPartitions: Int = rdd.getNumPartitions,
                    opts: BeastOptions = new BeastOptions()): PartitionedSpatialRDD = {
      val partitioner = IndexHelper.createPartitioner(rdd, partitionerKlass,
        NumPartitions(IndexHelper.Fixed, numPartitions),
        _ => 1,
        opts
      )
      IndexHelper.partitionFeatures(rdd, partitioner)
    }

    /**
     * Partitions this RDD using the given partitioner type. If the desired number of partitions is not provided,
     * the output number of partitions will be roughly equal to the number of partitions in the input RDD.
     * @param partitionerKlass the class of the partitioner
     * @param numPartitions the desired number of partitions. If not set, the number of partitions of the input RDD is used.
     * @return a new RDD that is partitioned using the given partitioner class
     */
    def spatialPartition(partitionerKlass: Class[_ <: SpatialPartitioner], numPartitions: Int = rdd.getNumPartitions,
                         opts: BeastOptions = new BeastOptions()): SpatialRDD = {
      val partitioner = IndexHelper.createPartitioner(rdd, partitionerKlass,
        NumPartitions(IndexHelper.Fixed, numPartitions),
        _ => 1,
        opts
      )
      IndexHelper.partitionFeatures2(rdd, partitioner)
    }
  }

  implicit class IndexMixinPairFunctions(partitionedRDD: PartitionedSpatialRDD) {
    require(partitionedRDD.partitioner.isDefined && partitionedRDD.partitioner.get.isInstanceOf[SpatialPartitioner],
      "This function requires the RDD to be partitioned by a spatial partitioner")

    /**
     * Writes a spatially partitioned RDD as a set of files, one for each partition and adds a _master file that
     * @param indexPath the path to write the index to
     * @param oformat the output format of the data files
     */
    @deprecated("Use writeSpatialFile", "0.9.2")
    def saveAsIndex(indexPath: String, oformat: String = "rtree"): Unit =
      IndexHelper.saveIndex2(partitionedRDD.mapPartitions(fs => fs.map(_._2), true), indexPath,
        "oformat" -> oformat)

  }
}
