/*
 * Copyright 2021 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import org.apache.spark.SparkContext

import java.awt.geom.AffineTransform

/**
 * A factory method for generating spatial data.
 */
class SpatialGeneratorBuilder(sparkContext: SparkContext, opts: BeastOptions = new BeastOptions()) {
  /**Set configuration of the generated data*/
  def config(key: String, value: Any): SpatialGeneratorBuilder = {
    opts.set(key, value.toString)
    this
  }

  /**
   * Merge all configuration with the given options. Any given option that already exists will override the current one.
   * @param opts additional options to use (and possibly override) existing ones
   * @return this object
   */
  def config(opts: BeastOptions): SpatialGeneratorBuilder = {
    this.opts.mergeWith(opts)
    this
  }

  /**
   * Retrieve all the configuration in this generator.
   * @return
   */
  def config: Map[String, String] = opts.toMap

  /**The type of distribution to generate*/
  private var distribution: DistributionType = _

  /**Number of partitions to generate*/
  private var numPartitions: Int = 0

  /**
   * Set the distribution of the generated data
   * @param distribution the distributed of the generated data as one of {[[UniformDistribution]],
   *                     [[DiagonalDistribution]], [[GaussianDistribution]], [[BitDistribution]],
   *                     [[SierpinskiDistribution]], [[ParcelDistribution]]}
   * @return
   */
  def distribution(distribution: DistributionType): SpatialGeneratorBuilder = {
    this.distribution = distribution
    this
  }

  /**
   * Set the number of partitions in the output. If not set or set to zero, one partition will be generated
   * for each one million records
   * @param num the number of partitions in the generated RDD
   * @return
   */
  def numPartitions(num: Int): SpatialGeneratorBuilder = {
    this.numPartitions = num
    this
  }

  /**
   * Sets the affine transformation for the generated data.
   * @param matrix the affine transformation matrix
   * @return
   */
  def affineTransform(matrix: AffineTransform): SpatialGeneratorBuilder = {
    this.config(SpatialGenerator.AffineMatrix, Seq(
      matrix.getScaleX, matrix.getShearY, matrix.getShearX, matrix.getScaleY,
      matrix.getTranslateX, matrix.getTranslateY).mkString(","))
    this
  }

  /**
   * Generates data in the given bounding box
   * @param mbr the bounding box of the generated data
   * @return
   */
  def mbr(mbr: EnvelopeNDLite): SpatialGeneratorBuilder = {
    if (mbr == new EnvelopeNDLite(2, 0, 0, 1, 1))
      this.opts.remove(SpatialGenerator.AffineMatrix)
    else this.config(SpatialGenerator.AffineMatrix, Seq(
      mbr.getSideLength(0), 0, 0, mbr.getSideLength(1), mbr.getMinCoord(0), mbr.getMinCoord(1)
    ).mkString(","))
    this
  }

  /**
   * Generate boxes around each generated point.
   * The size is measured as a fraction [0, 1] to indicate the ratio of the dataset bounding box.
   * Any value above 1.0 is invalid.
   * @param maxSize the maximum size for each side length of the generated box
   * @return this generator builder
   */
  def makeBoxes(maxSize: Double*): SpatialGeneratorBuilder =
    this.config(UniformDistribution.GeometryType, "box")
      .config(UniformDistribution.MaxSize, maxSize.mkString(","))

  /**
   * Generate random polygons of the given size (as in big and small) and number of segments per polygon.
   * The size is a fraction in the range [0, 1] which indicates the ratio of its maximum diameter as compared
   * to the size of the entire dataset bounding box.
   * @param maxSize the maximum size in the range [0, 1]
   * @param numSegments maximum number of line segments per polygon
   * @return this generator builder
   */
  def makePolygons(maxSize: Double, numSegments: Int): SpatialGeneratorBuilder =
    this.config(UniformDistribution.GeometryType, "polygon")
      .config(UniformDistribution.MaxSize, maxSize)
      .config(UniformDistribution.NumSegments, numSegments)

  /**
   * Generate the data as an RDD.
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def generate(cardinality: Long): SpatialRDD = {
    require(distribution != null, "Distribution is not specified for generated data")
    new RandomSpatialRDD(sparkContext, distribution, cardinality, numPartitions, opts)
  }

  /***
   * Generate uniformly distributed data
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def uniform(cardinality: Long): SpatialRDD = this.distribution(UniformDistribution).generate(cardinality)

  /**
   * Generate diagonally distributed data
   * @param cardinality the number of records to generate
   * @param percentage the percentage of records exactly on the diagonal line
   * @param buffer the buffer around the diagonal line in which records can be generated
   * @return the RDD that contains the generated data
   */
  def diagonal(cardinality: Long, percentage: Double = 0.5, buffer: Double = 0.2): SpatialRDD =
    this.distribution(DiagonalDistribution)
      .config(DiagonalDistribution.Percentage, percentage)
      .config(DiagonalDistribution.Buffer, buffer)
      .generate(cardinality)

  /**
   * Generate Gaussian distributed data
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def gaussian(cardinality: Long): SpatialRDD = {
    this.distribution(GaussianDistribution)
      .generate(cardinality)
  }

  /**
   * Generate data from the Sierpinski distribution
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def sierpinski(cardinality: Long): SpatialRDD = {
    this.distribution(SierpinskiDistribution)
      .generate(cardinality)
  }

  /**
   * Generate data from the bit distribution
   * @param cardinality the number of records to generate
   * @param digits the number of digits to set per coordinate
   * @param probability the probability of setting each bit
   * @return the RDD that contains the generated data
   */
  def bit(cardinality: Long, digits: Int = 10, probability: Double = 0.2): SpatialRDD = {
    this.distribution(BitDistribution)
      .config(BitDistribution.Digits, digits)
      .config(BitDistribution.Probability, probability)
      .generate(cardinality)
  }

  /**
   * Generates boxes from the parcel distribution
   * @param cardinality the number of records to generate
   * @param dither the amount of randomization to add to each generated box
   * @param splitRange the range of splitting each box
   * @return the RDD that contains the generated data
   */
  def parcel(cardinality: Long, dither: Double = 0.2, splitRange: Double = 0.2): SpatialRDD = {
    this.distribution(ParcelDistribution)
      .config(ParcelDistribution.Dither, dither)
      .config(ParcelDistribution.SplitRange, splitRange)
      .generate(cardinality)
  }
}
