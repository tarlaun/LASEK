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

import edu.ucr.cs.bdlab.beast.geolite.{GeometryHelper, IFeature}
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/**
 * An accumulator that computes the MBR of a dataset, the number of features, and its total size.
 * Keep in mind the following points:
 * <ul>
 * <li>It is the responsibility of the caller to use the accumulator correctly. It should be used either in
 * an action, or in a transformation that gets applies.</li>
 * <li>If applied in a transformation, the accumulator might be applied multiple times. This will affect the
 * [[Summary#numFeatures]] and [[Summary#size]] values but the MBR will be accurate if applied many times.
 * From Spark's documentation:
 * <li>For accumulator updates performed inside actions only, Spark guarantees that each task’s update to the
 * accumulator will only be applied once, i.e. restarted tasks will not update the value. In transformations,
 * users should be aware of that each task’s update may be applied more than once if tasks or job stages are
 * re-executed.</li>
 * <li>If an accurate result is vital, use one of the methods [[computeForFeatures()]]
 * or [[computeForFeaturesWithOutputSize()]] instead.</li>
 * </ul>
 */
class SummaryAccumulator(sizeFunction: IFeature => Int = _.getStorageSize) extends AccumulatorV2[IFeature, Summary] {
  /**The internal summary value*/
  val summary = new Summary

  override def isZero: Boolean = summary == null || summary.isEmpty

  override def copy(): AccumulatorV2[IFeature, Summary] = {
    val other = new SummaryAccumulator(sizeFunction)
    other.summary.setCoordinateDimension(this.summary.getCoordinateDimension)
    other.summary.expandToSummary(this.summary)
    other
  }

  override def reset(): Unit = summary.setEmpty()

  override def add(f: IFeature): Unit = {
    if (summary.getCoordinateDimension == 0)
      summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(f.getGeometry))
    val size: Int = sizeFunction(f)
    summary.expandToGeometryWithSize(f.getGeometry, size)
  }

  override def merge(other: AccumulatorV2[IFeature, Summary]): Unit = {
    val otherAcc = other.asInstanceOf[SummaryAccumulator]
    if (this.summary.isEmpty)
      this.summary.setCoordinateDimension(otherAcc.summary.getCoordinateDimension)
    this.summary.expandToSummary(otherAcc.summary)
  }

  override def value: Summary = summary
}
