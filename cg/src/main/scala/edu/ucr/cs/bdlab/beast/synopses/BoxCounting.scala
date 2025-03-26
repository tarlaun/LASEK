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
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import edu.ucr.cs.bdlab.beast.util.MathUtil

import scala.collection.mutable

object BoxCounting {
  /**
   * Produce the box counting measure of the given dataset with base q as described in the following paper.
   *  - Alberto Belussi, Sara Migliorini, and Ahmed Eldawy.
   *    Skewness-Based Partitioning in SpatialHadoop,
   *    In ISPRS International Journal of Geo-Information (IJGI) 9,
   *    pages 1-19, 2020. (Full text) DOI>10.3390/ijgi9040201
   * This function computes a uniform histogram and uses it to compute the box counting measure
   * for varying grid sizes by considering smaller grids aligned with the original one.
   * @param features the set of records to compute the box counting measure for
   * @param q the base of the box counting measure
   * @return the box counting value
   */
  def boxCounting(features: SpatialRDD, q: Int, opts: BeastOptions = new BeastOptions()): Double = {
    val gridSize = 4096
    val histogram = computeBCHistogram(features, gridSize)
    boxCounting(histogram, q)
  }

  /**
   * Compute a histogram that can be used for box counting. In this histogram, each bin contains the total number
   * of boxes that overlap each histogram.
   * @param features the set of features to compute the histogram for.
   * @param gridSize the number of partitions along each dimension in the histogram.
   * @return the computed histogram
   */
  def computeBCHistogram(features: SpatialRDD, gridSize: Int, mbr: EnvelopeNDLite = null): UniformHistogram = {
    val _mbr = if (mbr == null) Summary.computeForFeatures(features) else mbr
    val numDimensions = _mbr.getCoordinateDimension
    val histogramDimension = Array.fill(numDimensions)(gridSize)
    val histogram: UniformHistogram = features
      .map(f => new EnvelopeNDLite(numDimensions).merge(f.getGeometry))
      .mapPartitions(boxes => {
        val partialHistogram = new UniformHistogram(_mbr, histogramDimension:_*)
        for (box <- boxes) {
          val overlapStart: Array[Int] = new Array[Int](numDimensions)
          val overlapEnd: Array[Int] = new Array[Int](numDimensions)
          for (d <- 0 until numDimensions) {
            overlapStart(d) = ((box.getMinCoord(d) - _mbr.getMinCoord(d)) * gridSize / _mbr.getSideLength(d)).floor.toInt max 0
            overlapEnd(d) = ((box.getMaxCoord(d) - _mbr.getMinCoord(d)) * gridSize / _mbr.getSideLength(d)).ceil.toInt min gridSize
          }
          val overlap: Array[Int] = overlapStart.clone()
          while (overlap.last < overlapEnd.last) {
            partialHistogram.addEntry(overlap, 1)
            overlap(0) += 1
            var d = 0
            while (d < numDimensions - 1 && overlap(d) >= overlapEnd(d)) {
              overlap(d) = overlapStart(d)
              d += 1
              overlap(d) += 1
            }
          }
        }
        Option(partialHistogram).iterator
      })
      .reduce((h1, h2) => h1.mergeAligned(h2))
    histogram
  }

  /**
   * Compute box counting function for the given histogram and base q.
   * @param h the histogram to use
   * @param q the base of the box counting function
   * @return the box counting value
   */
  def boxCounting(h: UniformHistogram, q: Double): Double = {
    var histogram: UniformHistogram = h
    var gridSize = histogram.getNumPartitions(0)
    val numDimensions = histogram.getCoordinateDimension
    val mbr: EnvelopeNDLite = histogram
    var histogramDimension = Array.fill(numDimensions)(gridSize)
    // Create the box counting plot between log(r) and log(BC)
    val numPoints = MathUtil.log2(h.getNumPartitions(0)) + 1
    assert(numPoints >= 3, s"Number of points $numPoints is less than the minimum of three")
    val xs = new Array[Double](numPoints)
    val ys = new Array[Double](numPoints)
    val slopeVariance = new Array[Double](numPoints - 2)
    var prevSlope: Double = Double.NaN
    var iPoint: Int = 0
    while (gridSize >= 1) {
      var sum = 0.0
      for (binId <- 0 until histogram.getNumBins) {
        if (histogram.getBinValue(binId) > 0)
          sum += Math.pow(histogram.getBinValue(binId), q)
      }
      // Compute x, y values
      val r: Double = 1.0 / gridSize
      val bc = sum
      xs(iPoint) = Math.log(r)
      ys(iPoint) = Math.log(bc)
      if (iPoint >= 1) {
        // Starting at the second point, compute the slope
        val slope = (ys(iPoint) - ys(iPoint - 1)) / (xs(iPoint) - xs(iPoint - 1))
        // Starting at the third point, compute the variance in the slopt
        if (iPoint >= 2)
          slopeVariance(iPoint - 2) = Math.atan((slope - prevSlope).abs)
        prevSlope = slope
      }
      iPoint += 1

      // Move up to the next grid
      gridSize /= 2
      if (gridSize >= 1) {
        histogramDimension = Array.fill(numDimensions)(gridSize)
        val smallerHistogram = new UniformHistogram(mbr, histogramDimension: _*)
          .mergeNonAligned(histogram)
        histogram = smallerHistogram
      }
    }
    // Find the split points at which the slope changes significantly
    val numSplitPoints = 3
    // Position of split points
    val splitPoints = new mutable.ArrayBuffer[Int]()
    splitPoints.appendAll(slopeVariance.sorted.takeRight(numSplitPoints)
      .map(v => slopeVariance.indexOf(v)).sorted)
    splitPoints.append(numPoints)
    val minSplitLength = 3
    iPoint = 0
    while (iPoint < splitPoints.length) {
      val splitLength = splitPoints(iPoint) - (if (iPoint == 0) 0 else splitPoints(iPoint - 1))
      if (splitLength < minSplitLength)
        splitPoints.remove(iPoint)
      else
        iPoint += 1
    }
    // Compute the slope of the split with the highest support (number of elements)
    var highestSupport: Int = 0
    var splitWithHighestSupport: Int = -1
    for (iPoint <- 0 until splitPoints.length - 1) {
      val support = splitPoints(iPoint + 1) - splitPoints(iPoint)
      if (support > highestSupport) {
        highestSupport = support
        splitWithHighestSupport = iPoint
      }
    }

    // Compute the slope of the trend line between x=log(r) and x=log(bc) in the chosen split
    var sumx: Double = 0.0
    var sumy: Double = 0.0
    var sumxy: Double = 0.0
    var sumx2: Double = 0.0
    var n: Int = 0
    val range: Range = if (splitWithHighestSupport == -1)
      0 until numPoints // Process all points
    else
      splitPoints(splitWithHighestSupport) until splitPoints(splitWithHighestSupport + 1) // Process longest range

    for (iPoint <- range) {
      // Update linear regression counters
      val x = xs(iPoint)
      val y = ys(iPoint)
      n += 1
      sumx += x
      sumy += y
      sumxy += x * y
      sumx2 += x * x
      // Move up to the next grid
      gridSize /= 2
      if (gridSize >= 1) {
        histogramDimension = Array.fill(numDimensions)(gridSize)
        val smallerHistogram = new UniformHistogram(mbr, histogramDimension: _*)
          .mergeNonAligned(histogram)
        histogram = smallerHistogram
      }
    }
    val b = (n * sumxy - sumx * sumy) / (n * sumx2 - sumx * sumx)
    b
  }

}
