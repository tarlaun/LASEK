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

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.{SpatialJoinHelper, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.reflect.ClassTag

class IntersectingPartition(
                             override val index: Int,
                             @transient private val rdd1: RDD[_],
                             @transient private val rdd2: RDD[_],
                             s1Index: Int,
                             s2Index: Int,
                             val intersectionMBR: EnvelopeNDLite
                           ) extends Partition {
  val s1: Partition = rdd1.partitions(s1Index)
  val s2: Partition = rdd2.partitions(s2Index)

  override def toString: String = s"IntersectingPartition($s1, $s2)"
}

/**
 * An RDD that combines underlying RDDs based on their spatial location. Both underlying RDDs should be spatially
 * partitioned. It combines every pair of intersecting partitions.
 */
abstract class SpatialIntersectionRDD[T1: ClassTag, T2: ClassTag](var rdd1: RDD[T1], var rdd2: RDD[T2])
  extends RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))](
    rdd1.sparkContext, Nil) {

  val numPartitionsInRdd2: Int = rdd2.partitions.length

  // Ensure that both parent RDDs are spatially partitioned
  Seq(rdd1, rdd2).foreach(parentRDD => {
    require(parentRDD.partitioner.isDefined && parentRDD.partitioner.get.isInstanceOf[SpatialPartitioner],
      s"${this.getClass.getSimpleName} input RDDs should be spatially partitioned")
  })

  // Run a spatial join between partitions to find all pairs of overlapping partitions
  @transient val partitionMBRs: Seq[IndexedSeq[EnvelopeNDLite]] = Seq(rdd1, rdd2).map{rdd => {
    val partitioner: SpatialPartitioner = rdd.partitioner.get.asInstanceOf[SpatialPartitioner]
    0.until(rdd.getNumPartitions).map(i => partitioner.getPartitionMBR(i))
  }}

  var intersectPartitions: Array[Partition] = Array[Partition]()

  SpatialJoinHelper.planeSweepRectangles(partitionMBRs(0), partitionMBRs(1),
    (i1, i2) => {
      val intersectionMBR: EnvelopeNDLite = partitionMBRs(0)(i1).intersectionEnvelope(partitionMBRs(1)(i2))
      intersectPartitions = intersectPartitions :+
        new IntersectingPartition(intersectPartitions.length, rdd1, rdd2, i1, i2, intersectionMBR)
    }
  )

  override protected def getPartitions: Array[Partition] = intersectPartitions

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[IntersectingPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }
}

/**
 * A spatial intersection RDD that works with data loaded from files
 * @param _rdd1 first RDD
 * @param _rdd2 second RDD
 */
class SpatialIntersectionRDD1(@transient val _rdd1: SpatialRDD, @transient val _rdd2: SpatialRDD) extends
  SpatialIntersectionRDD(_rdd1, _rdd2) {

  override def compute(split: Partition, context: TaskContext): Iterator[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] = {
    val partition = split.asInstanceOf[IntersectingPartition]
    val iterator1: Iterator[IFeature] = rdd1.iterator(partition.s1, context)
    val iterator2: Iterator[IFeature] = rdd2.iterator(partition.s2, context)
    val intersectionMBR = partition.intersectionMBR
    Seq((intersectionMBR, (iterator1, iterator2))).iterator
  }
}

/**
 * A spatial intersection RDD that works with in-memory partitioned data
 * @param _rdd1 first RDD
 * @param _rdd2 second RDD
 */
class SpatialIntersectionRDD2(@transient val _rdd1: PartitionedSpatialRDD, @transient val _rdd2: PartitionedSpatialRDD) extends
  SpatialIntersectionRDD(_rdd1, _rdd2) {

  override def compute(split: Partition, context: TaskContext): Iterator[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] = {
    val partition = split.asInstanceOf[IntersectingPartition]
    val iterator1: Iterator[IFeature] = rdd1.iterator(partition.s1, context).map(kv => kv._2)
    val iterator2: Iterator[IFeature] = rdd2.iterator(partition.s2, context).map(kv => kv._2)
    val intersectionMBR = partition.intersectionMBR
    Seq((intersectionMBR, (iterator1, iterator2))).iterator
  }
}

/**
 * A spatial intersection RDD that works with in-memory partitioned data
 * @param _rdd1 first RDD which is spatially partitioned but without a an explicit partitionID assigned to each feature
 * @param _rdd2 second RDD which is spatially partitioned and each record is assigned a partitionID
 */
class SpatialIntersectionRDD3(@transient val _rdd1: SpatialRDD, @transient val _rdd2: PartitionedSpatialRDD) extends
  SpatialIntersectionRDD(_rdd1, _rdd2) {

  override def compute(split: Partition, context: TaskContext): Iterator[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] = {
    val partition = split.asInstanceOf[IntersectingPartition]
    val iterator1: Iterator[IFeature] = rdd1.iterator(partition.s1, context)
    val iterator2: Iterator[IFeature] = rdd2.iterator(partition.s2, context).map(kv => kv._2)
    val intersectionMBR = partition.intersectionMBR
    Seq((intersectionMBR, (iterator1, iterator2))).iterator
  }
}