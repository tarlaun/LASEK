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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeneratorTest extends AnyFunSuite with ScalaSparkTest {
  test("generate uniform data") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100)
    assert(randomPoints.getNumPartitions == 1)
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Point")
  }

  test("Generate multiple partitions") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = SpatialGenerator.RecordsPerPartition -> "53")
    assert(randomPoints.getNumPartitions == 2)
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Point")
  }

  test("Manually specify number of partitions") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      numPartitions = 2)
    assert(randomPoints.getNumPartitions == 2)
    assert(randomPoints.count() == 100)
    val counts = randomPoints.mapPartitions(features => Some(features.length).iterator).collect()
    assert(counts(0) == 50)
    assert(counts(1) == 50)
  }

  test("Generate boxes") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = Seq(UniformDistribution.GeometryType -> "box", UniformDistribution.MaxSize -> "0.2,0.1"))
    assert(randomPoints.count() == 100)
    assert(randomPoints.first().getGeometry.getGeometryType == "Envelope")
  }

  test("Generate points with transformation") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = SpatialGenerator.AffineMatrix -> "1,0,0,2,1,2")
    assert(randomPoints.count() == 100)
    val mbr = randomPoints.summary
    assert(new EnvelopeNDLite(2, 1.0, 2.0, 2.0, 4.0).containsEnvelope(mbr))
  }

  test("Generate boxes with transformation") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = Seq(UniformDistribution.GeometryType -> "box",
        UniformDistribution.MaxSize -> "0.2,0.2",
        SpatialGenerator.AffineMatrix -> "1,0,0,2,1,2")
    )
    assert(randomPoints.count() == 100)
    val mbr = randomPoints.summary
    assert(new EnvelopeNDLite(2, 1.0, 2.0, 2.0, 4.0).containsEnvelope(mbr))
  }

  test("Generate boxes with mirror transformation") {
    val randomBoxes: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = Seq(UniformDistribution.GeometryType -> "box",
        UniformDistribution.MaxSize -> "0.2,0.2",
        SpatialGenerator.AffineMatrix -> "-1,0,0,-1,0,0")
    )
    assert(randomBoxes.count() == 100)
    val mbr = randomBoxes.summary
    assert(new EnvelopeNDLite(2, -1.0, -1.0, 0.0, 0.0).containsEnvelope(mbr))
  }

  test("Generate parcel one partition") {
    val randomBoxes: SpatialRDD = new RandomSpatialRDD(sparkContext, ParcelDistribution, 100,
      opts = Seq(ParcelDistribution.Dither -> "0.2", ParcelDistribution.SplitRange -> "0.4")
    )
    assert(randomBoxes.count() == 100)
    val mbr = randomBoxes.summary
    assert(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0).containsEnvelope(mbr))
    // Ensure that all boxes are not overlapping
    val boxes = randomBoxes.collect()
    for (i <- boxes.indices; j <- 0 until i)
      assert(!boxes(i).getGeometry.overlaps(boxes(j).getGeometry))
  }

  test("Generate parcel multiple partition") {
    val randomBoxes: SpatialRDD = new RandomSpatialRDD(sparkContext, ParcelDistribution, 100,
      opts = Seq(ParcelDistribution.Dither -> "0.2", ParcelDistribution.SplitRange -> "0.4",
        SpatialGenerator.RecordsPerPartition -> "53")
    )
    assert(randomBoxes.getNumPartitions == 2)
    assert(randomBoxes.count() == 100)
    // Ensure that all boxes are not overlapping
    val boxes = randomBoxes.collect()
    for (i <- boxes.indices; j <- 0 until i)
      assert(!boxes(i).getGeometry.overlaps(boxes(j).getGeometry))
  }

  test("Generate polygons") {
    val randomPolygons: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = Seq(UniformDistribution.GeometryType -> "polygon", UniformDistribution.MaxSize -> "0.2,0.1",
        UniformDistribution.NumSegments -> "10..15"))
    assertResult(100)(randomPolygons.count())
    assertResult("Polygon")(randomPolygons.first().getGeometry.getGeometryType)
    assert((10 to 15).contains(randomPolygons.first().getGeometry.getNumPoints - 1))
  }

  test("Generate polygons with affine transformation") {
    val randomPolygons: SpatialRDD = new RandomSpatialRDD(sparkContext, UniformDistribution, 100,
      opts = Seq(UniformDistribution.GeometryType -> "polygon", UniformDistribution.MaxSize -> "0.2,0.1",
        UniformDistribution.NumSegments -> "10..15", SpatialGenerator.AffineMatrix -> "1,0,0,2,1,2"))
    assertResult(100)(randomPolygons.count())
    assertResult("Polygon")(randomPolygons.first().getGeometry.getGeometryType)
    // Verify that the number of points is in range.
    // Keep in mind that Geometry#getNumPionts returns number of line segments + 1 because of the repeated end point
    assert((10 to 15).contains(randomPolygons.first().getGeometry.getNumPoints - 1))
  }

  test("Generate boxes with MBR") {
    val desiredMBR = new EnvelopeNDLite(2, 2, 3, 9, 8)
    val randomPoints: SpatialRDD = new SpatialGeneratorBuilder(sparkContext).mbr(desiredMBR)
      .config(UniformDistribution.MaxSize, "0.2,0.1")
      .config(UniformDistribution.NumSegments, 10)
      .config(UniformDistribution.GeometryType, "box")
      .config(SpatialGenerator.Seed, 1794)
      .uniform(10)
    val actualMBR = new EnvelopeNDLite(randomPoints.summary)
    assertResult(true)(desiredMBR.containsEnvelope(actualMBR))
  }

  test("Generate polygons with MBR") {
    val desiredMBR = new EnvelopeNDLite(2, 2, 3, 9, 8)
    val randomPoints: SpatialRDD = new SpatialGeneratorBuilder(sparkContext).mbr(desiredMBR)
      .config(UniformDistribution.MaxSize, "0.2,0.1")
      .config(UniformDistribution.NumSegments, 10)
      .config(UniformDistribution.GeometryType, "polygon")
      .config(SpatialGenerator.Seed, 1794)
      .uniform(100)
    val actualMBR = new EnvelopeNDLite(randomPoints.summary)
    assertResult(true)(desiredMBR.containsEnvelope(actualMBR))
  }

  test("Bit distribution stays in the runit square") {
    val randomPoints: SpatialRDD = new RandomSpatialRDD(sparkContext, BitDistribution, 1000,
      opts = Seq(BitDistribution.Probability -> 0.9,
        UniformDistribution.GeometryType -> "polygon",
        UniformDistribution.MaxSize -> "0.2",
        UniformDistribution.NumSegments -> 10))
    val mbr = randomPoints.summary
    assert(mbr.getMinCoord(0) >= 0.0)
    assert(mbr.getMinCoord(1) >= 0.0)
    assert(mbr.getMaxCoord(0) <= 1.0)
    assert(mbr.getMaxCoord(1) <= 1.0)
  }
}
