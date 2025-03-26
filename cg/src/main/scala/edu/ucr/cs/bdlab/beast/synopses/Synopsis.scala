/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.synopses

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.util.LRUCache

/**
 * Synopsis of a dataset
 *
 * @param summary a summary of aggregates of the dataset
 * @param sample sample points for a dataset, first index indicates the dimension, second index indicates the point #
 * @param sampleWeights the weights of the sample points to indicate their size
 * @param e0 box counting value with base 0
 * @param e2 box counting value with base 2
 */
case class Synopsis(summary: Summary, sample: Array[Array[Double]], histogram: AbstractHistogram, e0: Double, e2: Double)

object Synopsis {

  def computeForFeatures(features: SpatialRDD, synopsisSize: Long = 1024 * 1024): Synopsis = {
    val acc = Summary.createSummaryAccumulator(features.sparkContext)
    val numDimensions = 2
    val sampleSize :Int = (synopsisSize / numDimensions / 8).toInt
    var sampleCoordinates = PointSampler.pointSample(features.map(f => {acc.add(f); f}), sampleSize, 0.01)
    val summary = new Summary(acc.value)
    if (sampleCoordinates == null || sampleCoordinates.isEmpty ||
      (sampleCoordinates(0).length < sampleSize && sampleCoordinates(0).length < summary.numFeatures)) {
      // Fall safe for the case where the input size is very small. Mostly used in testing.
      sampleCoordinates = PointSampler.pointSample(features.map(f => {acc.add(f); f}), sampleSize, 1.0)
    }
    // Compute histogram given the summary
    val histogram = BoxCounting.computeBCHistogram(features, 256, summary)
    val e0 = BoxCounting.boxCounting(histogram, 0)
    val e2 = BoxCounting.boxCounting(histogram, 2)
    Synopsis(summary, sampleCoordinates, histogram, e0, e2)
  }

  /**Driver-side cache of synopses for RDDs to avoid recomputing the synopsis multiple times*/
  val cachedSynopses = new LRUCache[Int, Synopsis](16)

  /**
   * Retrieve cached summary or compute and cache if not computed
   * @param rdd the RDD to compute the summary for
   * @return either a cached summary or a newly computed summary
   */
  def getOrCompute(rdd: SpatialRDD): Synopsis = cachedSynopses.getOrElseUpdate(rdd.id, {computeForFeatures(rdd)})
}