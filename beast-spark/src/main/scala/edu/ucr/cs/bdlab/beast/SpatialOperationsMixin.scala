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
package edu.ucr.cs.bdlab.beast

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.cg.{CGOperationsMixin, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.operations.SpatialJoin
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * Adds spatial operations to RDDs
 */
trait SpatialOperationsMixin extends CGOperationsMixin {

  /**
   * Additional functions for SpatialRDD
   * @param rdd the underlying RDD
   */
  implicit class RDDSpatialFunctions2(rdd: SpatialRDD) {

    /**
     * Performs a spatial join between this RDD and another RDD
     *
     * @param rdd2          another RDD to be join with
     * @param joinPredicate the spatial join predicate
     * @param method        the spatial join algorithm
     * @param mbrCount      an optional counter to keep track of the number of MBR tests
     * @return a new RDD that contains pairs of matching features
     */
    def spatialJoin(rdd2: SpatialRDD, joinPredicate: ESJPredicate = ESJPredicate.Intersects,
                    method: ESJDistributedAlgorithm = null, mbrCount: LongAccumulator = null,
                    opts: BeastOptions = new BeastOptions()): RDD[(IFeature, IFeature)] = {
      SpatialJoin.spatialJoin(rdd, rdd2, joinPredicate, method, mbrCount, opts)
    }
  }

  /**
   * Shortcut functions for SpatialRDDs that are partitioned by any spatial partitioner
   * @param partitionedRDD
   */
  implicit class RDDPartitionedSpatialFunctions2(partitionedRDD: PartitionedSpatialRDD) {
    require(partitionedRDD.partitioner.isDefined && partitionedRDD.partitioner.get.isInstanceOf[SpatialPartitioner],
      "This function requires the RDD to be partitioned by a spatial partitioner")

    /**
     * Performs a spatial join between two spatially partitioned datasets. This method always uses the
     * distributed join algorithm since both input datasets are spatially partitioned.
     * @param partitionedRDD2
     * @param joinPredicate
     * @param mbrCount
     * @return
     */
    def spatialJoin(partitionedRDD2: PartitionedSpatialRDD, joinPredicate: ESJPredicate = ESJPredicate.Intersects,
                    mbrCount: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
      require(partitionedRDD2.partitioner.isDefined && partitionedRDD2.partitioner.get.isInstanceOf[SpatialPartitioner],
        "This function requires the RDD to be partitioned by a spatial partitioner")
      SpatialJoin.spatialJoinDJ(
        partitionedRDD.mapPartitions(_.map(_._2), preservesPartitioning = true),
        partitionedRDD2.mapPartitions(_.map(_._2), preservesPartitioning = true),
        joinPredicate, mbrCount
      )
    }
  }

}
