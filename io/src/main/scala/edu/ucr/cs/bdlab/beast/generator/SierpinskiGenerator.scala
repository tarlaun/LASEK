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
 * Generates points or boxes that are distributed according to the Sierpinski distribution
 * @param partition
 */
class SierpinskiGenerator(partition: RandomSpatialPartition)
  extends PointBasedGenerator(partition) {

  require(partition.dimensions == 2, "Sierpinski distribution supports only two dimensions")

  val point1 = new PointND(geometryFactory, 0.0, 0.0)
  val point2 = new PointND(geometryFactory, 1.0, 0.0)
  val point3 = new PointND(geometryFactory, 0.5, Math.sqrt(3) / 2)

  var prevPoint: PointND = _

  def generatePoint: PointND = {
    val point = iRecord match {
      case 0 => point1
      case 1 => point2
      case 2 => point3
      case _ => {
        dice(5) match {
          case 1 | 2 => middlePoint(prevPoint, point1)
          case 3 | 4 => middlePoint(prevPoint, point2)
          case 5 => middlePoint(prevPoint, point3)
        }
      }
    }
    iRecord += 1
    prevPoint = point
    point
  }

  private def middlePoint(p1: PointND, p2: PointND): PointND = {
    val mp = new PointND(p1.getFactory, 2)
    mp.setCoordinate(0, (p1.getCoordinate(0) + p2.getCoordinate(0)) / 2)
    mp.setCoordinate(1, (p1.getCoordinate(1) + p2.getCoordinate(1)) / 2)
    mp
  }
}
