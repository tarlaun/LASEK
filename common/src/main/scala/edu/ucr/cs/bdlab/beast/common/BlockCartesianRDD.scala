package edu.ucr.cs.bdlab.beast.common

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import java.io.{IOException, ObjectOutputStream}
import scala.reflect.ClassTag

class CartesianPartition(idx: Int,
                         @transient private val rdd1: RDD[_],
                         @transient private val rdd2: RDD[_],
                         s1Index: Int,
                         s2Index: Int
                        ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class BlockCartesianRDD[T: ClassTag, U: ClassTag](
                                                   sc: SparkContext,
                                                   var rdd1 : RDD[T],
                                                   var rdd2 : RDD[U])
  extends RDD[(Iterator[T], Iterator[U])](sc, Nil)
    with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(Iterator[T], Iterator[U])] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    Some(rdd1.iterator(currSplit.s1, context), rdd2.iterator(currSplit.s2, context)).iterator
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
