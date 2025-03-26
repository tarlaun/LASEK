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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite}
import edu.ucr.cs.bdlab.beast.util.MathUtil
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

/**
 * Generates boxes according to the parcel generator
 * @param partition
 */
class ParcelGenerator(partition: RandomSpatialPartition)
  extends SpatialGenerator(partition) {

  /**The index of the record that will be returned when next is called*/
  var iRecord: Long = 0

  val splitRange: Double = partition.opts.getDouble(ParcelDistribution.SplitRange, 0.2)

  val dither: Double = partition.opts.getDouble(ParcelDistribution.Dither, 0.2)

  /**Unit square is the input domain*/
  val UnitSquare: EnvelopeNDLite = new EnvelopeNDLite(2, 0, 0, 1, 1)

  /**A stack of boxes to split. Each pair represents the level and the box*/
  var boxesToSplit: mutable.ArrayBuffer[(Int, EnvelopeNDLite)] = mutable.ArrayBuffer((0, UnitSquare))

  /**
   * The level of the deepest box to generate is &lceil; log<sub>2</sub>(n)&rceil; =
   * &lfloor;log<sub>2</sub>(n-1)&rfloor; + 1
   */
  val maxDepth: Int = MathUtil.log2(partition.cardinality - 1) + 1

  /**
   * Number of boxes that will be generated at the deepest level (maxDepth).
   * The remaining records will be generated at level maxDepth - 1
   */
  val numBoxesMaxDepth: Long = 2 * partition.cardinality - (1 << maxDepth)

  /**
   * Generates a box by first generating a point and building a box around it
   * @return
   */
  def generateBox: EnvelopeND = {
    assert(!boxesToSplit.isEmpty)
    assert(iRecord <= partition.cardinality)
    var (level, box) = boxesToSplit.remove(boxesToSplit.length - 1)

    while (iRecord <= partition.cardinality) {
      if (level == maxDepth || level == maxDepth - 1 && iRecord > numBoxesMaxDepth) {
        // Box is final. Return it
        ditherBox(box)
        return new EnvelopeND(geometryFactory, box)
      } else {
        // Split the box into two
        val (box1: EnvelopeNDLite, box2: EnvelopeNDLite) = splitBox(box)
        boxesToSplit.append((level + 1, box2))
        // Update the level and box for the next iteration
        level = level + 1
        box = box1
      }
    }
    null
  }

  /**
   * Split the given box into two according to the splitRange value. The given box is reused and returned as the
   * first box while the second box is created in the function.
   * This function always splits the box along the longest side. Let's assume the longest side has a length l,
   * the split will happen at l * uniform(splitRange, 1-splitRange).
   * @param box the box to split. Also the first box to be returned
   * @return the two boxes that result of the split
   */
  private def splitBox(box: EnvelopeNDLite): (EnvelopeNDLite, EnvelopeNDLite) = {
    val secondBox = new EnvelopeNDLite(box)
    var longestDimension: Int = 0
    for (d <- 1 until box.getCoordinateDimension) {
      if (box.getSideLength(d) > box.getSideLength(longestDimension))
        longestDimension = d
    }
    val splitPoint: Double = box.getMinCoord(longestDimension) +
      box.getSideLength(longestDimension) * uniform(splitRange, 1 - splitRange)
    box.setMaxCoord(longestDimension, splitPoint)
    secondBox.setMinCoord(longestDimension, splitPoint)
    (box, secondBox)
  }

  /**
   * Change the size of the given box along all dimensions according to the dither parameter.
   * The amount of change on the side length is a uniformly random variable between [0, dither).
   * This means that if the dither parameter is zero, the box will not be changed.
   * The center of the box remains fixed while dither
   * @param box the box to be dithered, changed in place.
   */
  private def ditherBox(box: EnvelopeNDLite): Unit = {
    for (d <- 0 until box.getCoordinateDimension) {
      val changeAmount: Double = uniform(0, dither) * box.getSideLength(d)
      box.setMinCoord(d, box.getMinCoord(d) + changeAmount / 2)
      box.setMaxCoord(d, box.getMaxCoord(d) - changeAmount / 2)
    }
  }

  /**
   * Generates and returns the next geometry
   *
   * @return the generated geometry
   */
  override def nextGeometry: Geometry = {
    iRecord += 1
    assert(iRecord <= partition.cardinality || boxesToSplit.isEmpty)
    generateBox
  }

  override def hasNext: Boolean = iRecord < partition.cardinality
}