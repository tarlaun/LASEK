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

import edu.ucr.cs.bdlab.beast.geolite.PointND

/**
 * Generates points or boxes that are uniformly distributed in the input space
 * @param partition
 */
class DiagonalGenerator(partition: RandomSpatialPartition)
  extends PointBasedGenerator(partition) {

  val percentage: Double = partition.opts.getDouble(DiagonalDistribution.Percentage, 0.5)

  val buffer: Double = partition.opts.getDouble(DiagonalDistribution.Buffer, 0.2)

  val sqrt2: Double = Math.sqrt(2)

  def generatePoint: PointND = {
    val point = new PointND(geometryFactory, partition.dimensions)
    if (bernoulli(percentage) == 1) {
      val position: Double = uniform(0, 1)
      for (d <- 0 until partition.dimensions)
        point.setCoordinate(d, position)
    } else {
      val c = uniform(0, 1)
      val d = normal(0, buffer / 5)
      // TODO: How to extend these functions to arbitrary dimensions
      point.setCoordinate(0, c + d / sqrt2)
      point.setCoordinate(1, c - d / sqrt2)
    }
    point
  }
}
