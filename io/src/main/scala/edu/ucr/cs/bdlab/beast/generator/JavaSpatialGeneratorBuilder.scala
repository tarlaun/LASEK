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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.JavaSpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

/**
 * A factory method for generating spatial data.
 */
class JavaSpatialGeneratorBuilder(sparkContext: SparkContext, opts: BeastOptions = new BeastOptions()) {
  def this(jsc: JavaSparkContext) {
    this(jsc.sc)
  }

  /**Set configuration of the generated data*/
  def config(key: String, value: Any): JavaSpatialGeneratorBuilder = {
    opts.set(key, value.toString)
    this
  }

  def config(opts: BeastOptions): JavaSpatialGeneratorBuilder = {
    this.opts.mergeWith(opts)
    this
  }

  /**The type of distribution to generate*/
  var distribution: DistributionType = _

  /**Number of partitions to generate*/
  var numPartitions: Int = 0

  /**
   * Set the distribution of the generated data
   * @param distribution the distributed of the generated data as one of {[[UniformDistribution]],
   *                     [[DiagonalDistribution]], [[GaussianDistribution]], [[BitDistribution]],
   *                     [[SierpinskiDistribution]], [[ParcelDistribution]]}
   * @return
   */
  def distribution(distribution: DistributionType): JavaSpatialGeneratorBuilder = {
    this.distribution = distribution
    this
  }

  /**
   * Set the number of partitions in the output. If not set or set to zero, one partition will be generated
   * for each one million records
   * @param num the number of partitions in the generated RDD
   * @return
   */
  def numPartitions(num: Int): JavaSpatialGeneratorBuilder = {
    this.numPartitions = num
    this
  }

  /**
   * Generate boxes around each generated point
   * @param maxSize the maximum size for each side length of the generated box
   * @return
   */
  def makeBoxes(maxSize: Int*): JavaSpatialGeneratorBuilder = {
    this.config(UniformDistribution.GeometryType, "box")
      .config(UniformDistribution.MaxSize, maxSize.mkString(","))
    this
  }

  /**
   * Generate the data as an RDD.
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def generate(cardinality: Long): JavaSpatialRDD = {
    require(distribution != null, "Distribution is not specified for generated data")
    JavaRDD.fromRDD(new RandomSpatialRDD(sparkContext, distribution, cardinality, numPartitions, opts))
  }

  /***
   * Generate uniformly distributed data
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def uniform(cardinality: Long): JavaSpatialRDD = this.distribution(UniformDistribution).generate(cardinality)

  /**
   * Generate diagonally distributed data
   * @param cardinality the number of records to generate
   * @param percentage the percentage of records exactly on the diagonal line
   * @param buffer the buffer around the diagonal line in which records can be generated
   * @return the RDD that contains the generated data
   */
  def diagonal(cardinality: Long, percentage: Double = 0.5, buffer: Double = 0.2): JavaSpatialRDD =
    this.distribution(DiagonalDistribution)
      .config(DiagonalDistribution.Percentage, percentage)
      .config(DiagonalDistribution.Buffer, buffer)
      .generate(cardinality)

  /**
   * Generate Gaussian distributed data
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def gaussian(cardinality: Long): JavaSpatialRDD = {
    this.distribution(GaussianDistribution)
      .generate(cardinality)
  }

  /**
   * Generate data from the Sierpinski distribution
   * @param cardinality the number of records to generate
   * @return the RDD that contains the generated data
   */
  def sierpinski(cardinality: Long): JavaSpatialRDD = {
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
  def bit(cardinality: Long, digits: Int = 10, probability: Double = 0.2): JavaSpatialRDD = {
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
  def parcel(cardinality: Long, dither: Double = 0.2, splitRange: Double = 0.2): JavaSpatialRDD = {
    this.distribution(ParcelDistribution)
      .config(ParcelDistribution.Dither, dither)
      .config(ParcelDistribution.SplitRange, splitRange)
      .generate(cardinality)
  }
}
