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

import java.io.{IOException, PrintStream}

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.synopses.{HistogramOP, UniformHistogram}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import scala.annotation.varargs

/**
  * Computes the histogram of an input file.
  */
@OperationMetadata(
  shortName =  "histogram",
  description = "Computes a uniform histogram for a geometry file",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat])
)
object Histogram extends CLIOperation with Logging {

  /**
    * Number of buckets in the histogram
    */
  @OperationParam(
    description = "Total number of buckets in the histogram",
    defaultValue = "1000"
  )
  val NumBuckets = "numbuckets"

  @OperationParam(
    description = "Include zeros in the output",
    defaultValue = "false"
  )
  val IncludeZeros = "includezeros"

  @OperationParam(
    description = "Type of histogram {simple, euler}",
    defaultValue = "simple"
  )
  val HistogramType = "histogramtype"

  @OperationParam(
    description = "The value to compute in the histogram {count, size, writesize}",
    defaultValue = "count"
  )
  val HistogramValue = "histogramvalue"

  @OperationParam(
    description = "Method to compute the histogram {onepass, onehalfpass, twopasses, sparse}",
    defaultValue = "twopasses"
  )
  val ComputationMethod = "method"

  @varargs def computePointWriteSizeHistogram(features: SpatialRDD, mbb: EnvelopeNDLite, opts: BeastOptions, numBuckets: Int*):
    UniformHistogram = {
    val sizeFunction = new FeatureWriterSize(opts)
    HistogramOP.computeHistogram(features, sizeFunction, HistogramOP.TwoPass, HistogramOP.PointHistogram, numBuckets:_*).asInstanceOf[UniformHistogram]
  }

  @varargs def computePointWriteSizeHistogram(features: JavaSpatialRDD, mbb: EnvelopeNDLite, opts: BeastOptions, numBuckets: Int*):
      UniformHistogram =
    computePointWriteSizeHistogram(features.rdd, mbb, opts, numBuckets:_*)

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    // Extract user parameters
    val numBuckets = opts.getInt(NumBuckets, 1000)
    val features = sc.spatialFile(inputs(0), opts)

    val sizeFunction: IFeature => Int = opts.getString(HistogramValue, "count").toLowerCase match {
      case "count" => _ => 1
      case "size" => _.getStorageSize
      case "writesize" => new FeatureWriterSize(opts)
    }

    val method: HistogramOP.ComputationMethod = opts.getString(ComputationMethod, "twopasses").toLowerCase() match {
      case "twopasses" => HistogramOP.TwoPass
      case "onehalfpass" => HistogramOP.OneHalfPass
      case "onepass" => HistogramOP.OnePass
      case "sparse" => HistogramOP.Sparse
    }

    val htype: HistogramOP.HistogramType = opts.getString(HistogramType, "simple").toLowerCase match {
      case "simple" => HistogramOP.PointHistogram
      case "euler" => HistogramOP.EulerHistogram
    }
    val histogram = HistogramOP.computeHistogram(features, sizeFunction, method, htype, numBuckets)

    // Print the dataset's histogram to the output
    val outStream = new PrintStream(outputs(0))
    histogram.print(outStream, opts.getBoolean(IncludeZeros, false))
    outStream.close()
  }
}
