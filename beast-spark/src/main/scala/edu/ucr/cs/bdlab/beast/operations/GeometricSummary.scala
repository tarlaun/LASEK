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
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.{FeatureWriter, SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses.{BoxCounting, Summary, SummaryAccumulator}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

/**
  * Computes the summary of the input features including some geometric attributes.
  */
@OperationMetadata(
  shortName =  "summary",
  description = "Computes the minimum bounding rectangle (MBR), count, and size of a dataset",
  inputArity = "1",
  outputArity = "0",
  inheritParams = Array(classOf[SpatialFileRDD])
)
object GeometricSummary extends CLIOperation with Logging {

  @OperationParam(description = "Include box counting measures, E0 and E2", defaultValue = "false")
  val IncludeBoxCounting = "boxcounting"

  @OperationParam(description = "Include local summaries in the output", defaultValue = "false")
  val IncludeLocalSummaries = "localsummaries"

  /** Java shortcut */
  def computeForFeaturesWithOutputSize(features: JavaSpatialRDD, opts: BeastOptions) : Summary =
    computeForFeaturesWithOutputSize(features.rdd, opts)

  /**
    * Compute the summary of the given features while computing the size based on the write size of the configured
    * [[FeatureWriter]] (or output format) in the given user options
    * @param features features to compute the summary for
    * @param opts the user options that contains the configured output format
    * @return
    */
  def computeForFeaturesWithOutputSize(features : SpatialRDD, opts : BeastOptions) : Summary =
    edu.ucr.cs.bdlab.beast.synopses.Summary.computeForFeatures(features, new FeatureWriterSize(opts))

  /**
    * Create a summary accumulator that uses the configured output format to measure the size of the output.
    *
    * @param sc the spark context to register the accumulator to
    * @param opts user options that contain information about which FeatureWriter (or output format) to use
    * @return the initialized and registered accumulator
    */
  def createSummaryAccumulatorWithWriteSize(sc: SparkContext, opts : BeastOptions) : SummaryAccumulator =
    Summary.createSummaryAccumulator(sc, new FeatureWriterSize(opts))

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options and configuration
    * @param sc the spark context to use
    * @return the summary computed for the input defined in the user options
    */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val inputFeatures = sc.spatialFile(inputs(0), opts)
    val summary: Summary = {
      try {
        // If we can get some feature writer class successfully, compute a summary with output format
        val featureWriter = SpatialWriter.getConfiguredFeatureWriterClass(opts.loadIntoHadoopConf(null))
        computeForFeaturesWithOutputSize(inputFeatures, opts)
      } catch {
        case e: Exception => Summary.computeForFeatures(inputFeatures)
      }
    }
    // These two arrays represent all values that will be in the summary
    var summarySchema = Seq[StructField]()
    var summaryValues = Seq[Any]()
    // Start with the basic statistics of the dataset
    val summaryRow = summary.asRow
    summarySchema ++= summaryRow.schema
    summaryValues ++= Row.unapplySeq(summaryRow).get

    // Add additional information based on the first feature
    val feature: IFeature = inputFeatures.first()
    // Add SRID that represents the coordinate reference system (CRS) of the dataset
    summarySchema = summarySchema :+ StructField("srid", IntegerType)
    summaryValues = summaryValues :+ feature.getGeometry.getSRID
    // Add information about additional non-spatial attributes
    if (feature.length > 1) {
      val attributeSchema = StructType(Seq(
        StructField("name", StringType),
        StructField("type", StringType),
      ))
      val attributeInfo: Seq[Row] = feature.iNonGeom.toSeq.map(i => {
        var attName: String = feature.getName(i)
        if (attName == null || attName.isEmpty)
          attName = s"attribute#$i"
        val attType: String = Summary.getTypeAsString(feature.schema(i).dataType)
        new GenericRowWithSchema(Array(attName, attType), attributeSchema)
      }).seq
      summarySchema = summarySchema :+ StructField("attributes", new ArrayType(attributeSchema, false))
      summaryValues = summaryValues :+ attributeInfo
    }
    // Add box counting numbers if requested
    if (opts.getBoolean(IncludeBoxCounting, false)) {
      // Compute box counting summaries (E0, E2)
      val bcHistogram = BoxCounting.computeBCHistogram(inputFeatures, 128)
      val e0 = Math.abs(BoxCounting.boxCounting(bcHistogram, 0))
      val e2 = BoxCounting.boxCounting(bcHistogram, 2)
      summarySchema = summarySchema :+ StructField("e0", DoubleType) :+ StructField("e2", DoubleType)
      summaryValues = summaryValues :+ e0 :+ e2
    }
    // Add local summaries if requested
    if (opts.getBoolean(IncludeLocalSummaries, false)) {
      val (globalSummary, localSummaries) = GriddedSummary.computeForFeatures(inputFeatures, 128, 128)
      val localSummarySchema = StructType(Seq(
        StructField("pos", new ArrayType(DoubleType, false)),
        StructField("size", LongType),
        StructField("num_features", LongType),
        StructField("num_non_empty_features", LongType),
        StructField("num_points", LongType),
        StructField("avg_area", DoubleType),
        StructField("avg_sidelength", new ArrayType(DoubleType, false)),
      ))
      summarySchema = summarySchema :+ StructField("local_summaries", new ArrayType(localSummarySchema, false))
      summaryValues = summaryValues :+ localSummaries.collect().map(s => {
        val avg_sidelengths = new Array[Double](s._2.getCoordinateDimension)
        for (d <- avg_sidelengths.indices)
          avg_sidelengths(d) = s._2.averageSideLength(d)
        new GenericRowWithSchema(Array(s._1, s._2.size, s._2.numFeatures, s._2.numNonEmptyGeometries,
          s._2.numPoints, s._2.averageArea, avg_sidelengths), localSummarySchema)
      })
    }
    val finalRow = new GenericRowWithSchema(summaryValues.toArray, StructType(summarySchema))
    println(finalRow.prettyJson)
    summary
  }

}
