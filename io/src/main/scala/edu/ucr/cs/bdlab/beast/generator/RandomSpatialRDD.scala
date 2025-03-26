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
package edu.ucr.cs.bdlab.beast.generator

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Partition, SparkContext, TaskContext}

// A cheap way to create an enumeration in Scala
trait DistributionType extends Serializable

object UniformDistribution extends DistributionType {
  /**
   * The maximum size for boxes and polygons in the range [0.0, 1.0]. Encoded as comma-separated floating-point values.
   * For boxes, this parameter indicates tha maximum size along each dimension.
   * For polygons, this value is a single number that indicates the diameter of the circle that contains the polygon.
   */
  val MaxSize = "maxSize"

  /**The type of geometry to generate, {point, box, polygon}*/
  val GeometryType = "geometry"

  /**
   * Indicates the number of line segments to generate in each polygon. If this is a single integer value (n),
   * the range is assumed to be [3, n]. If it contains two numbers in the form "n1..n2", the range is taken as [n1, n2].
   * The default value is "5".
   */
  val NumSegments = "numSegments"

  override def toString: String = "Uniform"
}

object DiagonalDistribution extends DistributionType {
  /** The percentage of points that are exactly on the diagonal */
  val Percentage = "percentage"
  /**For points not exactly on the diagonal, the buffer in which they are dispersed*/
  val Buffer = "buffer"

  override def toString: String = "Diagonal"
}

object GaussianDistribution extends DistributionType {
  override def toString: String = "Gaussian"
}

object SierpinskiDistribution extends DistributionType {
  override def toString: String = "Sierpinski"
}

object BitDistribution extends DistributionType {
  /** Number of digits in the generated data */
  val Digits = "digits"
  /**The probability of setting a bit*/
  val Probability = "probability"

  override def toString: String = "Bit"
}

object ParcelDistribution extends DistributionType {
  /**
   * The allowed range for splitting boxes. Allowed range [0.0, 0.5]
   * 0.0 means all values are allowed.
   * 0.5 means always split in half.
   */
  val SplitRange: String = "splitRange"
  /**The amount of dithering as a ratio of the side length. Allowed range [0, 1]*/
  val Dither: String = "dither"

  override def toString: String = "Parcel"
}

/**
 * An RDD partition that represents randomly generated data
 * @param index the index of this partition within its parent RDD
 * @param cardinality the number of records to generate
 * @param dimensions number of dimensions
 * @param seed the seed for the random generator
 * @param opts additional options for the generation
 */
case class RandomSpatialPartition(index: Int, cardinality: Long,
                                  dimensions: Int, seed: Long,
                                  opts: BeastOptions) extends Partition
/**
 * A SpatialRDD that contains randomly generated geometries. Each geometry is wrapped in a feature with no
 * additional attributes.
 * @param sc the [[SparkContext]] associated with this RDD
 * @param distribution the distribution of the generated data
 * @param cardinality the number of records to generate
 * @param numPartitions the number of partitions to generate. If set to zero, it will be inferred based on size
 * @param opts additional options for the generated data
 */
class RandomSpatialRDD(sc: SparkContext, distribution: DistributionType, cardinality: Long,
                       numPartitions: Int = 0,
                       opts: BeastOptions = new BeastOptions())
  extends RDD[IFeature](sc, Seq()) {
  val _partitions: Array[Partition] = {
    val numRecordsPerPartition = opts.getLong(SpatialGenerator.RecordsPerPartition, 1000000)
    val seed: Long = opts.getLong(SpatialGenerator.Seed, System.currentTimeMillis())
    val dimensions: Int = opts.getInt(SpatialGenerator.Dimensions, 2)
    val finalNumPartitions: Int = if (numPartitions != 0) numPartitions
                else ((cardinality + numRecordsPerPartition - 1) / numRecordsPerPartition).toInt
    if (distribution != ParcelDistribution) {
      val _partitions = new Array[Partition](finalNumPartitions)
      var recordsRemaining = cardinality
      for (iPartition <- _partitions.indices) {
        val partitionCardinality = recordsRemaining / (finalNumPartitions - iPartition)
        _partitions(iPartition) = RandomSpatialPartition(iPartition, partitionCardinality, dimensions,
          seed + iPartition, opts)
        recordsRemaining -= partitionCardinality
      }
      _partitions
    } else {
      // Parcel distribution requires special partition generation to ensure that the records are non overlapping

      // Generate the partitions using the parcel generator but set dithering to zero since dithering should
      // only be applied on the final records and not the partitions
      val partitionBoxes = new ParcelGenerator(RandomSpatialPartition(0, finalNumPartitions,
        dimensions, seed, new BeastOptions(opts).set("dither", 0)))
      var recordsRemaining = cardinality
      partitionBoxes.zipWithIndex.map(pi => {
        val (partition, iPartition) = pi
        val partitionCardinality = recordsRemaining / (finalNumPartitions - iPartition)
        recordsRemaining -= partitionCardinality
        // Adjust the affine matrix of this partition so that it will generate records within the partition boundaries
        // There is no need to change the other parameters (dither and split range) since they are ratios
        val partitionBox = partition.getGeometry.asInstanceOf[EnvelopeND]
        val partitionOpts = new BeastOptions(opts)
          .set(SpatialGenerator.AffineMatrix,
            Array(partitionBox.getSideLength(0), 0, 0, partitionBox.getSideLength(1),
              partitionBox.getMinCoord(0), partitionBox.getMinCoord(1))
              .map(_.toString).mkString(","))
        RandomSpatialPartition(iPartition, partitionCardinality, dimensions,
          seed + iPartition, partitionOpts)
      }).toArray
    }
  }

  override protected def getPartitions: Array[Partition] = _partitions

  /**
   * Returns an iterator to the generated data in the given partition
   * @param split the partition to return its data. Must be of type [[RandomSpatialPartition]]
   * @param context the Spark task context
   * @return an iterator to the generated data
   */
  override def compute(split: Partition, context: TaskContext): Iterator[IFeature] = {
    distribution match {
      case UniformDistribution => new UniformGenerator(split.asInstanceOf[RandomSpatialPartition])
      case DiagonalDistribution => new DiagonalGenerator(split.asInstanceOf[RandomSpatialPartition])
      case GaussianDistribution => new GaussianGenerator(split.asInstanceOf[RandomSpatialPartition])
      case BitDistribution => new BitGenerator(split.asInstanceOf[RandomSpatialPartition])
      case SierpinskiDistribution => new SierpinskiGenerator(split.asInstanceOf[RandomSpatialPartition])
      case ParcelDistribution => new ParcelGenerator(split.asInstanceOf[RandomSpatialPartition])
    }
  }


  /**
   * Return the descriptor of this dataset as a Row.
   * @return
   */
  def descriptor: Row = {
    var attributes = Array[(String, String)](
      ("distribution", distribution.toString),
      ("cardinality", cardinality.toString),
    )
    val otherAtts = Array(SpatialGenerator.Seed, SpatialGenerator.Dimensions, UniformDistribution.MaxSize,
      UniformDistribution.GeometryType, UniformDistribution.NumSegments, DiagonalDistribution.Percentage,
      DiagonalDistribution.Buffer, BitDistribution.Digits, BitDistribution.Probability,
      ParcelDistribution.SplitRange, ParcelDistribution.Dither, SpatialGenerator.AffineMatrix)
    for (att <- otherAtts; if opts.contains(att))
      attributes = attributes :+ ((att, opts.getString(att)))
    val values: Array[Any] = attributes.map(_._2)
    val schema = StructType(attributes.map(x => StructField(x._1, StringType)))
    new GenericRowWithSchema(values, schema)
  }

}
