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
package edu.ucr.cs.bdlab.beast.synopses

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import org.apache.spark.rdd.RDD

import scala.math.Ordering
import scala.util.Random

/**
 * An object class that can read point samples from SpatialRDD
 */
object PointSampler {
  class CoordinateOrdering extends Ordering[Array[Double]] {
    // No ordering is actually needed.
    def compare(x: Array[Double], y: Array[Double]) = 0
  }

  implicit object CoordinateOrderingObject extends CoordinateOrdering

  /**
   * Reads a point sample from the given spatial RDD. It returns a two dimensional array where the first index
   * is the dimension and the second index is the point. This method runs an action on the given RDD.
   * The method takes both a sample size and sample ratio and it returns whichever is smaller. In other words,
   * it tries to read the given sample ratio and if the result is bigger than the sample size, it truncates it
   * to the sample size. This ensures that the final result will fit in memory regardless of the input size.
   * @param features the input features to sample
   * @param sampleSize number of sample points to read
   * @param sampleRatio the ratio of the points to read
   * @return a two-dimensional array of sample points
   */
  def pointSample(features: SpatialRDD, sampleSize: Int, sampleRatio: Double, seed: Long = System.currentTimeMillis()): Array[Array[Double]] = {
    val sizePerPartition: Int = (sampleSize * 2.0 / features.getNumPartitions).ceil.toInt
    val partialSample: RDD[(Int, Array[Double])] = features.sample(false, sampleRatio).mapPartitionsWithIndex((id, features) => {
      val random = new Random(seed + id)
      val sampleRecords: scala.collection.mutable.PriorityQueue[(Int, Array[Double])] =
        new scala.collection.mutable.PriorityQueue[(Int, Array[Double])]()
      for (feature <- features; if (feature.getGeometry != null && !feature.getGeometry.isEmpty)) {
        val position: Int = random.nextInt(Int.MaxValue)
        if (sampleRecords.size < sizePerPartition || position <= sampleRecords.head._1) {
          val mbr: EnvelopeNDLite = new EnvelopeNDLite().merge(feature.getGeometry)
          val point = new Array[Double](mbr.getCoordinateDimension)
          for (d <- 0 until mbr.getCoordinateDimension)
            point(d) = mbr.getCenter(d)
          if (sampleRecords.size < sizePerPartition) {
            // Add directly since we did not accumulate enough points
            sampleRecords.enqueue((position, point))
          } else if (position <= sampleRecords.head._1) {
            // This point should replace the last point to keep the sample size as desired
            sampleRecords.dequeue()
            sampleRecords.enqueue((position, point))
          }
        }
      }
      sampleRecords.iterator
    })
    val finalPoints = partialSample.collect.sorted.take(sampleSize).map(_._2)
    if (finalPoints.isEmpty)
      Array.empty[Array[Double]]
    else {
      val numDimensions = finalPoints.head.length
      val finalCoordinates: Array[Array[Double]] = Array.ofDim[Double](numDimensions, finalPoints.length)
      var i = 0
      for (point <- finalPoints) {
        assert(point.length == numDimensions)
        for (d <- 0 until numDimensions)
          finalCoordinates(d)(i) = point(d)
        i += 1
      }
      finalCoordinates
    }
  }
}
