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
package edu.ucr.cs.bdlab.beast.synopses

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.synopses
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.varargs

/**
 * A helper object to calculate histograms for features.
 */
object HistogramOP extends Logging {
  /**A choice of a computation method for histograms {[[OnePass]], [[OneHalfPass]], [[TwoPass]]}*/
  trait ComputationMethod
  case object OnePass extends ComputationMethod
  case object OneHalfPass extends ComputationMethod
  case object TwoPass extends ComputationMethod
  case object Sparse extends ComputationMethod

  /**A choice of a histogram type, {[[PointHistogram]], [[EulerHistogram]]}*/
  trait HistogramType
  case object PointHistogram extends HistogramType
  case object EulerHistogram extends HistogramType

  /**
   * Compute the grid dimension from the given sequence of numbers. If the given sequence contains a single number,
   * it is treated as a total number of partitions and the number of partitions along each dimension is computed
   * using the function [[synopses.UniformHistogram#computeNumPartitions(Envelope, int)]]
   *
   * @param mbr the MBR of the input space. Used to compute square-like cells
   * @param numPartitions the desired number of partitions. Either a single number of a sequence of dimensions
   * @return an array of number of partitions along each axis
   */
  @varargs def computeGridDimensions(mbr: EnvelopeNDLite, numPartitions: Int*): Array[Int] = {
    if (numPartitions.size < mbr.getCoordinateDimension) {
      // Treat it as number of buckets and compute the number of partitions correctly
      val numBuckets: Int = numPartitions.product
      synopses.UniformHistogram.computeNumPartitions(mbr, numBuckets)
    } else {
      numPartitions.toArray
    }
  }

  /**
   * Compute a point histogram for sparse histograms. It maps each record to a bucket and then aggregate by bucket.
   * This method can be helpful for very large histograms to avoid moving the entire histogram during the reduce step.
   * @param features the features to compute their histogram
   * @param sizeFunction the function that evaluates the size of each feature
   * @param mbb the minimum bounding box of the histogram, typically, this is the same as the input MBB
   * @param numBuckets the number of buckets in the histogram
   * @return the computed histogram
   */
  @varargs def computePointHistogramSparse(features: SpatialRDD, sizeFunction: IFeature => Int,
                                          mbb: EnvelopeNDLite, numBuckets: Int*): UniformHistogram = {
    val gridDimensions: Array[Int] = computeGridDimensions(mbb, numBuckets: _*)
    val binSize: RDD[(Int, Long)] = features.map(feature => {
      val center = new PointND(feature.getGeometry)
      val binID = UniformHistogram.getPointBinID(center, mbb, gridDimensions)
      (binID, sizeFunction(feature).toLong)
    }).filter(_._1 >= 0)
    val finalSizes: RDD[(Int, Long)] = binSize.reduceByKey(_+_)
    val finalHistogram: UniformHistogram = new UniformHistogram(mbb, gridDimensions:_*)
    finalSizes.collect.foreach(pt => finalHistogram.values(pt._1) = pt._2)
    finalHistogram
  }

  /**
   * Computes a point histogram which assigns each feature to the cell that contains its centroid
   * @param features the set of features
   * @param sizeFunction a function that maps each feature to a size
   * @param mbb the minimum bounding box of the input data or the region that the histogram should be computed for
   * @param numBuckets the number of partitions per dimension. If only one value is provided, it is treated as
   *                      a hint for the total number of buckets.
   * @return the computed histogram
   */
  @varargs def computePointHistogramTwoPass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                            mbb: EnvelopeNDLite, numBuckets: Int*) : UniformHistogram = {
    val gridDimensions: Array[Int] = computeGridDimensions(mbb, numBuckets: _*)
    var partialHistograms: RDD[UniformHistogram] = features.mapPartitions(features => {
      val partialHistogram: UniformHistogram = new UniformHistogram(mbb, gridDimensions:_*)
      while (features.hasNext) {
        val feature = features.next()
        if (!feature.getGeometry.isEmpty) {
          val point = new PointND(feature.getGeometry)
          partialHistogram.addPoint(point, sizeFunction(feature))
        }
      }
      Option(partialHistogram).iterator
    })
    // Calculate how many histograms we can reduce together
    val size = numBuckets.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf
      .getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms = partialHistograms.coalesce(maxReduce)
    partialHistograms.reduce((h1, h2) => h1.mergeAligned(h2))
  }

  @varargs def computePointHistogramTwoPass(features: JavaSpatialRDD,
                                            sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                            mbb: EnvelopeNDLite, numBuckets: Int*) : UniformHistogram =
    computePointHistogramTwoPass(features.rdd, f => sizeFunction.call(f), mbb, numBuckets:_*)

  /**
   * Computes a point histogram which assigns each feature to the cell that contains its centroid
   * @param features the set of features
   * @param sizeFunction a function that maps each feature to a size
   * @param numBuckets the number of partitions per dimension. If only one value is provided, it is treated as
   *                      a hint for the total number of buckets.
   * @return the computed histogram
   */
  @varargs def computePointHistogramOnePass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                            numBuckets: Int*) : UniformHistogram = {
    val partialHistograms: RDD[UniformHistogram] = computePartialHistograms(features, sizeFunction, numBuckets:_*)
    // Merge partial histograms into one
    partialHistograms.reduce((ph1, ph2) => {
      if (ph1.containsEnvelope(ph2)) {
        ph1.mergeNonAligned(ph2)
      } else {
        val combinedMBR = new EnvelopeNDLite(ph1)
        combinedMBR.merge(ph2)
        val finalHistogram = new synopses.UniformHistogram(combinedMBR, computeGridDimensions(combinedMBR, numBuckets:_*):_*)
        finalHistogram.mergeNonAligned(ph1).mergeNonAligned(ph2)
      }
    })
  }

  /**
   * Computes a point histogram which assigns each feature to the cell that contains its centroid
   * @param features the set of features
   * @param sizeFunction a function that maps each feature to a size
   * @param numBuckets the number of partitions per dimension. If only one value is provided, it is treated as
   *                      a hint for the total number of buckets.
   * @return the computed histogram
   */
  @varargs def computePointHistogramOneHalfPass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                                numBuckets: Int*) : UniformHistogram = {
    val partialHistograms: RDD[UniformHistogram] = computePartialHistograms(features, sizeFunction, numBuckets:_*)
    // Cache all partial histogram
    partialHistograms.persist(StorageLevel.MEMORY_AND_DISK)
    // Compute the MBR of all histogram
    val mbrCombineFunc = (mbr1: EnvelopeNDLite, mbr2: EnvelopeNDLite) => mbr1.merge(mbr2)
    val combinedMBR = partialHistograms.aggregate(new EnvelopeNDLite())(mbrCombineFunc, mbrCombineFunc)
    // Merge all the partial histograms into the final histogram
    val histogramCombineFunc = (hist1: UniformHistogram, hist2: UniformHistogram) => hist1.mergeNonAligned(hist2)
    val finalHistogram = new UniformHistogram(combinedMBR, computeGridDimensions(combinedMBR, numBuckets:_*):_*)
    partialHistograms.aggregate(finalHistogram)(histogramCombineFunc, histogramCombineFunc)
  }

  /**
   * An internal function to compute partial histograms for each partition
   * @param features the set of features
   * @param sizeFunction the function that calculates the size for each features
   * @param numBuckets the number of partitions per histogram
   * @return the computed partial histograms
   */
  private def computePartialHistograms(features: SpatialRDD, sizeFunction: IFeature => Int,
                                       numBuckets: Int*): RDD[UniformHistogram] = {
    val partialHistograms: RDD[UniformHistogram] = features.mapPartitions(features => {
      val pointSizesCached: Array[(PointND, Int)] = features.map(f => (new PointND(f.getGeometry), sizeFunction(f))).toArray
      val mbb = new EnvelopeNDLite(pointSizesCached(0)._1.getCoordinateDimension)
      mbb.setEmpty()
      pointSizesCached.foreach(pointSize => mbb.merge(pointSize._1))
      val partialHistogram = new UniformHistogram(mbb, computeGridDimensions(mbb, numBuckets: _*): _*)
      pointSizesCached.foreach(pointSize => partialHistogram.addPoint(pointSize._1, pointSize._2))
      Some(partialHistogram).iterator
    })
    // Calculate how many histograms we can reduce together
    val size = numBuckets.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf
      .getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms.coalesce(maxReduce)
    else
      partialHistograms
  }

  /**
   * Compute the Euler histogram of the given set of features. The resulting histogram contains four
   * values per cell (in two dimensions). Currently, this method only works for two dimensions
   * @param dataRecords the data points contains pairs of envelopes and values
   * @param inputMBB the MBR of the input space
   * @param numPartitions the number of partitions to create in the histogram
   */
  @varargs def computeEulerHistogram(dataRecords: SpatialRDD, sizeFunction: IFeature => Int,
                                     inputMBB: EnvelopeNDLite, numPartitions : Int*): EulerHistogram2D = {
    val gridDimensions = computeGridDimensions(inputMBB, numPartitions:_*)
    var partialHistograms = dataRecords.mapPartitions(features => {
      val partialHistogram = new EulerHistogram2D(inputMBB, gridDimensions(0), gridDimensions(1))
      while (features.hasNext) {
        val feature = features.next
        // Make sure the MBR fits within the boundaries
        val featureMBB: EnvelopeNDLite = new EnvelopeNDLite().merge(feature.getGeometry)
        featureMBB.shrink(inputMBB)
        if (!featureMBB.isEmpty)
          partialHistogram.addEnvelope(featureMBB.getMinCoord(0), featureMBB.getMinCoord(1),
            featureMBB.getMaxCoord(0), featureMBB.getMaxCoord(1), sizeFunction(feature))
      }
      Some(partialHistogram).iterator
    })
    // Calculate how many histograms we can reduce together
    val size = numPartitions.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf.getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms = partialHistograms.coalesce(maxReduce)
    partialHistograms.reduce((h1, h2) => h1.mergeAligned(h2))
  }

  /**
   * (Java Shortcut to)
   * Compute the Euler histogram of the given set of features. The resulting histogram contains four
   * values per cell (in two dimensions). Currently, this method only works for two dimensions
   * @param dataRecords the data points contains pairs of envelopes and values
   * @param sizeFunction the function that computes the size of a feature
   * @param inputMBB the MBR of the input space
   * @param numPartitions the number of partitions to create in the histogram
   * @return the histogram of the input data
   * Note: We rely on [[org.apache.spark.api.java.function.Function]]
   * rather than [[scala.Function1]] to allow lambda expressions from Java without running into serialization issues
   */
  @varargs def computeEulerHistogram(dataRecords: JavaSpatialRDD,
                                     sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                     inputMBB: EnvelopeNDLite, numPartitions : Int*): EulerHistogram2D =
    computeEulerHistogram(dataRecords.rdd, f => sizeFunction.call(f), inputMBB, numPartitions:_*)

  /**
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param sizeFunction the function that computes the size of each feature
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, sizeFunction: IFeature => Int,
                                method: ComputationMethod, htype: HistogramType, numBuckets: Int*): AbstractHistogram =
    (method, htype) match {
      case (Sparse, PointHistogram) => computePointHistogramSparse(features, sizeFunction, features.summary, numBuckets:_*)
      case (TwoPass, PointHistogram) => computePointHistogramTwoPass(features, sizeFunction, features.summary, numBuckets:_*)
      case (OneHalfPass, PointHistogram) => computePointHistogramOneHalfPass(features, sizeFunction, numBuckets:_*)
      case (OnePass, PointHistogram) => computePointHistogramOnePass(features, sizeFunction, numBuckets:_*)
      case (TwoPass, EulerHistogram) => computeEulerHistogram(features, sizeFunction, features.summary, numBuckets:_*)
      case other => throw new RuntimeException(s"Unsupported histogram computation method '$other'")
    }

  /**
   * (Java Shortcut to)
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param sizeFunction the function that computes the size of each feature
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD,
                                sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                method: ComputationMethod, htype: HistogramType, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, f => sizeFunction.call(f), method, htype, numBuckets:_*)

  /**
   * Compute a count histogram for the given set of features where each bucket contains the number of features
   * @param features the features to compute their histogram
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, method: ComputationMethod,
                                numBuckets: Int*): AbstractHistogram =
    computeHistogram(features, _=>1, method, PointHistogram, numBuckets:_*)

  /**
   * (Java Shortuct to)
   * Compute a count histogram for the given set of features where each bucket contains the number of features
   * @param features the features to compute their histogram
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD, method: ComputationMethod,
                                numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, method, numBuckets:_*)


  /**
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features, TwoPass, numBuckets:_*)

  /**
   * (Java Shortcut to)
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, numBuckets:_*)
}
