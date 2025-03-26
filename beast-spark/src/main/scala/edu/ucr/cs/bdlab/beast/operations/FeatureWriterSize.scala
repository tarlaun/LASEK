package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.{FeatureWriter, SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import org.apache.hadoop.io.IOUtils.NullOutputStream

/**
 * A size estimator based on a FeatureWriter that cached the FeatureWriter locally to avoid recreating it
 * with every call but also does not serialize it since FeatureWriter is not necessarily serializable
 *
 * @param opts user options to create the feature writer and initialize it
 */
class FeatureWriterSize(opts: BeastOptions) extends (IFeature => Int)
  with org.apache.spark.api.java.function.Function[IFeature, Int]
  with Serializable {

  require(opts.contains(SpatialWriter.OutputFormat) || opts.contains(SpatialFileRDD.InputFormat),
    s"The output format must be defined by setting the parameter '${SpatialWriter.OutputFormat}'")

  @transient lazy val featureWriter: FeatureWriter = {
    val writerClass = SpatialWriter.getConfiguredFeatureWriterClass(opts.loadIntoHadoopConf(null))
    val featureWriter = writerClass.newInstance
    featureWriter.initialize(new NullOutputStream, opts.loadIntoHadoopConf(null))
    featureWriter
  }

  override def apply(f: IFeature): Int = featureWriter.estimateSize(f)

  /**For Java callers*/
  override def call(f: IFeature): Int = apply(f)
}