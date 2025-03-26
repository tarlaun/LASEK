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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, GeometryType}
import edu.ucr.cs.bdlab.beast.synopses.Summary
import org.apache.spark.Partition

/**
 * A wrapper around any partition that adds [[SpatialPartition]] information to it
 */
class WrappedSpatialPartition(val partition: Partition, override val mbr: EnvelopeNDLite,
                              override val numFeatures: Long, override val numNonEmptyGeometries: Long,
                              override val numPoints: Long, override val size: Long,
                              override val sumSideLength: Array[Double], override val geometryType: GeometryType)
  extends SpatialPartition {
  override val index: Int = partition.index

  def this(partition: Partition, summary: Summary) {
    this(partition, new EnvelopeNDLite(summary), summary.numFeatures, summary.numNonEmptyGeometries,
      summary.numPoints, summary.size, summary.sumSideLength, summary.geometryType)
  }
}
