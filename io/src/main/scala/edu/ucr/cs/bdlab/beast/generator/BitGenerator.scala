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
 * Generates points or boxes using the bit distribution
 *
 * @param partition
 */
class BitGenerator(partition: RandomSpatialPartition)
  extends PointBasedGenerator(partition) {

  val probability: Double = partition.opts.getDouble(BitDistribution.Probability, 0.2)

  val digits: Int = partition.opts.getInt(BitDistribution.Digits, 10)

  def generatePoint: PointND = {
    val point = new PointND(geometryFactory, partition.dimensions)
    for (d <- 0 until partition.dimensions) {
      point.setCoordinate(d, generateCoordinate)
    }
    point
  }

  private def generateCoordinate: Double = {
    var n: Double = 0.0
    for (i <- 1 to digits) {
      val c = bernoulli(probability)
      n = n + c.toDouble / (1 << i)
    }
    n
  }
}

