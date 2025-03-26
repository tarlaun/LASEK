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
package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{PartitionedSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RGrovePartitioner, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import java.io.{IOException, PrintStream}
import java.util

/**
  * Builds an index over a set of features that can be either stored to disk or kept as an RDD.
  */
@OperationMetadata(
  shortName =  "index",
  description = "Builds a distributed spatial index",
  inputArity = "+",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat], classOf[RGrovePartitioner])
)
object Index extends CLIOperation with Logging {

  override def printUsage(out: PrintStream): Unit = {
    val partitioners: Map[String, Class[_ <: SpatialPartitioner]] = IndexHelper.partitioners
    out.println("The available indexes are:")
    partitioners.foreach(kv => {
      val indexerMetadata = kv._2.getAnnotation(classOf[SpatialPartitioner.Metadata])
      out.println(s"- ${kv._1}: ${indexerMetadata.description}")
    })
  }

  override def addDependentClasses(opts: BeastOptions, classes: util.Stack[Class[_]]): Unit = {
    super.addDependentClasses(opts, classes)
    classes.push(IndexHelper.getClass)
  }

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    // Extract index parameters from the command line arguments
    val gIndex = opts.getString(IndexHelper.GlobalIndex, "rsgrove")
    val partitionerClass: Class[_ <: SpatialPartitioner] = IndexHelper.partitioners(gIndex)

    // Start processing the input to build the index
    // Read the input features
    val rdds = inputs.zipWithIndex
      .map(ii => sc.spatialFile(ii._1, opts.retainIndex(ii._2)))
    val features: SpatialRDD = if(rdds.length == 1) rdds.head else sc.union(rdds)
    // Partition the input records using the created partitioner
    val partitionedFeatures: SpatialRDD = IndexHelper.partitionFeatures2(features, partitionerClass,
      new FeatureWriterSize(opts), opts)
    // Save the index to disk
    IndexHelper.saveIndex2(partitionedFeatures, outputs(0), opts)
  }
}
