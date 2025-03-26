/*
 * Copyright 2018 University of California, Riverside
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

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import edu.ucr.cs.bdlab.beast.geolite.{GeometryReader, PointND}
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

import scala.math.Ordering

/**
 * A Spark accumulator that reads a sample of a fixed size
 */
class PointSampleAccumulator(var size: Int) extends AccumulatorV2[PointND, Array[PointND]] with Externalizable {
  class PointOrdering extends Ordering[PointND] {
    def compare(x: PointND, y: PointND) = 0
  }
  implicit object PointO extends PointOrdering

  val sampleRecords: scala.collection.mutable.PriorityQueue[(Int, PointND)] =
    new scala.collection.mutable.PriorityQueue[(Int, PointND)]()

  override def isZero: Boolean = sampleRecords.isEmpty

  override def copy(): PointSampleAccumulator = {
    val copy: PointSampleAccumulator = new PointSampleAccumulator(size)
    for (entry <- this.sampleRecords)
      copy.sampleRecords.enqueue((entry._1, entry._2))
    copy
  }

  override def reset(): Unit = sampleRecords.clear()

  override def add(v: PointND): Unit = {
    val position: Int = (Math.random() * Int.MaxValue).toInt
    if (sampleRecords.size < size) {
      // Add directly since we did not accumulate enough points
      sampleRecords.enqueue((position, v))
    } else if (position <= sampleRecords.head._1) {
      // This point should replace the last point to keep the sample size as desired
      sampleRecords.dequeue()
      sampleRecords.enqueue((position, v))
    }
  }

  override def merge(other: AccumulatorV2[PointND, Array[PointND]]): Unit = {
    // TODO use a merge-sort-like algorithm for efficiency (linear rather than this algorithm which is O(n log n)
    if (other.isInstanceOf[PointSampleAccumulator]) {
      for (entry <- other.asInstanceOf[PointSampleAccumulator].sampleRecords)
        this.sampleRecords.enqueue((entry._1, entry._2))
      while (this.sampleRecords.size > size)
        sampleRecords.dequeue()
    } else {
      throw new RuntimeException(s"Not supported type of accumulator $other")
    }
  }

  override def value: Array[PointND] = sampleRecords.map(_._2).toArray

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(size)
    out.writeInt(sampleRecords.size)
    if (sampleRecords.nonEmpty) {
      val numDimensions = sampleRecords.head._2.getCoordinateDimension
      out.writeInt(numDimensions)
      val baos = new ByteArrayOutputStream()
      val dout = new DataOutputStream(new GZIPOutputStream(baos))
      for (i <- sampleRecords) {
        dout.writeInt(i._1)
        for (d <- 0 until numDimensions)
          dout.writeDouble(i._2.getCoordinate(d))
      }
      dout.close()
      // Write the compressed array
      val compressedData: Array[Byte] = baos.toByteArray
      out.writeInt(compressedData.length)
      out.write(compressedData, 0, compressedData.length)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    this.size = in.readInt()
    val numPoints = in.readInt()
    this.sampleRecords.clear()
    if (numPoints > 0) {
      val numDimensions = in.readInt()
      val compressedDataLength = in.readInt()
      val compressedData = new Array[Byte](compressedDataLength)
      in.readFully(compressedData)
      val din = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(compressedData)))

      val geometryFactory = GeometryReader.DefaultInstance.getGeometryFactory
      for (_ <- 0 until numPoints) {
        val position = din.readInt()
        val point = new PointND(geometryFactory, numDimensions)
        for (d <- 0 until numDimensions)
          point.setCoordinate(d, din.readDouble())
        this.sampleRecords.enqueue((position, point))
      }
    }
  }
}

object PointSampleAccumulator {
  def createSampleAccumulator[T](sc: SparkContext, size: Int): PointSampleAccumulator = {
    val acc = new PointSampleAccumulator(size)
    sc.register(acc, s"SampleAccumulator_$size")
    acc
  }
}