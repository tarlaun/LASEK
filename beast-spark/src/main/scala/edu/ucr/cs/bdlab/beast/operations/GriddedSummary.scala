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

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Computes the summary of the input features including some geometric attributes.
  */
@OperationMetadata(
  shortName =  "gridsummary",
  description = "Computes data summary over a grid on the data and writes it to a CSV file",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD])
)
object GriddedSummary extends CLIOperation with Logging {
  /** Maximum split size*/
  @OperationParam(description = "Number of grid cells", defaultValue = "100")
  val NumGridCells: String = "numcells"

  /**Overwrite the output file if already exists*/
  @OperationParam(description = "Overwrite the output if it already exists {true, false}.", defaultValue = "false")
  val OverwriteOutput = "overwrite"

  /**
   * Computes a set of summaries for each grid cell in the bounding box of the input
   * @param features the set of features to summarize
   * @param numPartitions either the total number of cells or an array of number of partitions along each dimension
   * @return one global summary for the entire data and a set of local summaries for each grid cell in the
   *         bounding box of the data
   */
  def computeForFeatures(features: SpatialRDD, numPartitions: Int*): (Summary, RDD[(Array[Int], Summary)]) = {
    val globalSummary: Summary = features.summary
    val numDimensions: Int = globalSummary.getCoordinateDimension
    val grid: GridPartitioner = new GridPartitioner(globalSummary,
      if (numPartitions.length == globalSummary.getCoordinateDimension)
        numPartitions.toArray
      else {
        val numPartitionsAlongAxis: Array[Int] = new Array[Int](globalSummary.getCoordinateDimension)
        GridPartitioner.computeNumberOfPartitionsAlongAxes(globalSummary, numPartitions.head, numPartitionsAlongAxis)
        numPartitionsAlongAxis
      }
    )
    val zeroSummary: Summary = new Summary()
    zeroSummary.setCoordinateDimension(numDimensions)
    val localSummaries = features.map(f => {
      // Find the position of the cell that contains the centroid of the bounding box
      val boundingBox = new EnvelopeNDLite().merge(f.getGeometry)
      val cellID = grid.overlapPartition(boundingBox)
      (cellID, (cellID, f))
    }).aggregateByKey(zeroSummary)((summary: Summary, cellIDFeature: (Int, IFeature)) => {
      val geometryMBR = new EnvelopeND(cellIDFeature._2.getGeometry)
      geometryMBR.shrink(grid.getPartitionMBR(cellIDFeature._1))
      summary.expandToGeometryWithSize(geometryMBR, cellIDFeature._2.getStorageSize)
      summary
    }, (summary1: Summary, summary2: Summary) => summary1.expandToSummary(summary2))
      .map(kv => (grid.getGridPosition(kv._1), kv._2))
    (globalSummary, localSummaries)
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options and configuration
    * @param sc the spark context to use
    * @return the summary computed for the input defined in the user options
    */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val inputFeatures = sc.spatialFile(inputs(0), opts)
    val gridSize = opts.getString(NumGridCells, "100").split(",").map(_.toInt)
    val (globalSummary, localSummaries) = computeForFeatures(inputFeatures, gridSize:_*)
    // Write the local summaries to the given input file
    val localSummariesDF = createSummaryDataframe(globalSummary, localSummaries)
    localSummariesDF.write
      .option("delimiter", ",")
      .option("header", true)
      .mode(if (opts.getBoolean(OverwriteOutput, false)) SaveMode.Overwrite else SaveMode.ErrorIfExists)
      .csv(outputs(0))
  }

  def createSummaryDataframe(globalSummary: Summary, localSummaries: RDD[(Array[Int], Summary)]): DataFrame = {
    val sparkSession = SparkSession.active
    val numDimensions = globalSummary.getCoordinateDimension
    var schema: Seq[StructField] = Seq[StructField]()
    for (d <- 0 until numDimensions)
      schema = schema :+ StructField(s"i$d", IntegerType)
    schema = schema ++ Seq(
      StructField("num_features", LongType),
      StructField("size", LongType),
      StructField("num_points", LongType),
      StructField("avg_area", DoubleType)
    )
    for (d <- 0 until numDimensions)
      schema = schema :+ StructField(s"avg_side_length_$d", DoubleType)
    sparkSession.createDataFrame(
      localSummaries.map({case (position: Array[Int], summary: Summary) => {
        var values: Seq[Any] = position
        values = values ++ Seq[Any](summary.numFeatures, summary.size, summary.numPoints, summary.averageArea)
        for (d <- 0 until numDimensions)
          values = values :+ summary.averageSideLength(d)
        Row(values:_*)
      }}), StructType(schema)).coalesce(1)
  }
}
