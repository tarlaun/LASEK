package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.generator._
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, Feature, GeometryReader, IFeature, PointND}
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Envelope, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class SpatialJoinScalaTest extends AnyFunSuite with ScalaSparkTest {
  test("SpatialJoinDupAvoidance") {
    val r = List(
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 10.0, 10.0, 10.1, 10.1)),
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 3.0, 1.0, 5.0, 3.0))
    )
    val s = List(
      Feature.create(null, new EnvelopeND(new GeometryFactory, 2, 2.0, 0.0, 4.0, 2.0))
    )
    val results = SpatialJoin.spatialJoinIntersectsPlaneSweepFeatures(r.toArray, s.toArray,
      new EnvelopeNDLite(2, 2.0, 0.0, 5.0, 3.0), ESJPredicate.Intersects, null)
    assertResult(1)(results.size)
  }

  test("Distributed Join on loaded indexes") {
    val testFile1 = makeDirCopy("/sjoinr.grid")
    val testFile2 = makeDirCopy("/sjoins.grid")
    val dataset1 = sparkContext.readWKTFile(testFile1.getPath, 0)
    val dataset2 = sparkContext.readWKTFile(testFile2.getPath, 0)
    assertResult(2)(dataset1.getNumPartitions)
    assertResult(2)(dataset2.getNumPartitions)
    val joinResults = dataset1.spatialJoin(dataset2, ESJPredicate.Intersects, ESJDistributedAlgorithm.DJ)
    assertResult(2)(joinResults.getNumPartitions)
  }

  test("Distributed Join on memory-partitioned data") {
    val testFile1 = makeFileCopy("/sjoinr.wkt")
    val testFile2 = makeFileCopy("/sjoins.wkt")
    val dataset1 = sparkContext.readWKTFile(testFile1.getPath, 0)
    val dataset2 = sparkContext.readWKTFile(testFile2.getPath, 0)
    val gridPartitioner = new GridPartitioner(new EnvelopeNDLite(2, 0, 0, 11, 5), Array(2,2))
    val partitioned1 = dataset1.spatialPartition(gridPartitioner)
    val partitioned2 = dataset2.spatialPartition(gridPartitioner)
    assertResult(4)(partitioned1.getNumPartitions)
    assertResult(4)(partitioned2.getNumPartitions)
    val joinResults = partitioned1.spatialJoin(partitioned2, ESJPredicate.Intersects)
    assertResult(4)(joinResults.getNumPartitions)
  }

  test("PBSM with points") {
    val geometryFactory = GeometryReader.DefaultGeometryFactory
    val dataset1: SpatialRDD = sparkContext.parallelize(Seq(
      Feature.create(null, new PointND(geometryFactory, 2, 1.0, 1.0))
    ))
    val dataset2: SpatialRDD = sparkContext.parallelize(Seq(
      Feature.create(null, new EnvelopeND(geometryFactory, 2, 0.0, 0.0, 2.0, 2.0))
    ))
    val result = SpatialJoin.spatialJoinPBSM(dataset1, dataset2, ESJPredicate.MBRIntersects)
    assertResult(1)(result.count())
  }

  test("PBSM with disjoint datasets should return an empty RDD without join") {
    val geometryFactory = GeometryReader.DefaultGeometryFactory
    val dataset1: SpatialRDD = sparkContext.parallelize(Seq(
      Feature.create(null, new PointND(geometryFactory, 2, 1.0, 1.0))
    ))
    val dataset2: SpatialRDD = sparkContext.parallelize(Seq(
      Feature.create(null, new EnvelopeND(geometryFactory, 2, 2.0, 2.0, 3.0, 3.0))
    ))
    val result = SpatialJoin.spatialJoinPBSM(dataset1, dataset2, ESJPredicate.MBRIntersects)
    assertResult(0)(result.getNumPartitions)
    assertResult(true)(result.isEmpty())
  }

  test("Close-by but not overlapping records") {
    // Create two sets of rectangles that are very close to each other but not overlapping
    // Ensure that the answer is zero
    val factory = GeometryReader.DefaultGeometryFactory
    val xs = new Array[Double](8)
    xs(0) = 0
    xs(1) = Math.nextUp(xs(0))
    xs(2) = Math.nextUp(xs(1))
    xs(3) = Math.nextUp(xs(2))
    xs(4) = 1E9
    xs(5) = Math.nextUp(xs(4))
    xs(6) = Math.nextUp(xs(5))
    xs(7) = Math.nextUp(xs(6))
    val s1 = Array(
      Feature.create(null, factory.toGeometry(new Envelope(xs(0), xs(1), 0, 1))),
      Feature.create(null, factory.toGeometry(new Envelope(xs(4), xs(5), 0, 1)))
    )
    val s2 = Array(
      Feature.create(null, factory.toGeometry(new Envelope(xs(2), xs(3), 0, 1))),
      Feature.create(null, factory.toGeometry(new Envelope(xs(6), xs(7), 0, 1)))
    )
    val r1: SpatialRDD = sparkContext.parallelize(s1)
    val r2: SpatialRDD = sparkContext.parallelize(s2)
    val results = SpatialJoin.spatialJoinBNLJ(r1, r2, joinPredicate = ESJPredicate.MBRIntersects)
    assertResult(0)(results.count())
  }

  test("should not apply duplicate avoidance on loaded disjoint index before join") {
    val factory = GeometryReader.DefaultGeometryFactory
    val r1 = Array(
      Feature.create(null, new EnvelopeND(factory, 2, 2.0, 1.0, 6.0, 3.0))
    )
    val r2 = Array(
      Feature.create(null, new EnvelopeND(factory, 2, 5.0, 1.0, 7.0, 3.0))
    )
    val grid = new GridPartitioner(new EnvelopeNDLite(2, 0, 0, 8, 4), Array(2, 1))
    val index1Path = new File(scratchDir, "dataset1_index").getPath
    sparkContext.parallelize(r1).asInstanceOf[SpatialRDD].spatialPartition(grid).writeSpatialFile(index1Path, "envelope")
    val index2Path = new File(scratchDir, "dataset2_index").getPath
    sparkContext.parallelize(r2).asInstanceOf[SpatialRDD].spatialPartition(grid).writeSpatialFile(index2Path, "envelope")
    // Now join both after reading from disk
    val r1Disk: SpatialRDD = sparkContext.spatialFile(index1Path, "envelope")
    val r2Disk: SpatialRDD = sparkContext.spatialFile(index2Path, "envelope")
    val resultSize = r1Disk.spatialJoin(r2Disk).count()
    assertResult(1)(resultSize)
  }

  test("Stress test spatial join algorithms with random cases") {
    if (shouldRunStressTest()) {
      // Run random join cases and check that all algorithms produce the same result size
      // This is meant to make sure that corner cases are handled correctly
      // We hope that randomly generated data will produce many corner cases that should be handled
      val snapRecords: IFeature => IFeature = f => {
        val envelope = f.getGeometry.asInstanceOf[EnvelopeND]
        for (d <- 0 until envelope.getCoordinateDimension) {
          envelope.setMinCoord(d, (envelope.getMinCoord(d) / 0.05).floor * 0.05)
          envelope.setMinCoord(d, (envelope.getMinCoord(d) / 0.05).ceil * 0.05)
        }
        Feature.create(row = null, geometry = envelope)
      }
      val addRecordID: ((IFeature, Long)) => IFeature = fid => Feature.append(fid._1, fid._2, "ID")

      val distributions = Seq(UniformDistribution)
      for (i <- 0 until 10) {
        for (distribution <- distributions) {
          val firstDataset: SpatialRDD = sparkContext.generateSpatialData(distribution, 100,
            opts = Seq("seed" -> i, UniformDistribution.MaxSize -> "0.1,0.1", "geometry" -> "box"))
            .map(snapRecords)
            .zipWithUniqueId()
            .map(addRecordID)

          val secondDataset: SpatialRDD = sparkContext.generateSpatialData(UniformDistribution, 100,
            opts = Seq("seed" -> (i + 1), UniformDistribution.MaxSize -> "0.1,0.1", "geometry" -> "box"))
            .map(snapRecords)
            .zipWithUniqueId()
            .map(addRecordID)
          val bnljResults = firstDataset.spatialJoin(secondDataset, method = ESJDistributedAlgorithm.BNLJ)
          // We use the BNLJ as the baseline (groundtruth)
          val bnljResultSize = bnljResults.count()
          // Check PBSM result
          val pbsmResults = firstDataset.spatialJoin(secondDataset, method = ESJDistributedAlgorithm.PBSM,
            opts = SpatialJoin.JoinWorkloadUnit -> "2k")
          assertResult(true, s"Error in PBSM in iteration #$i")(resultsEqual(bnljResults, bnljResultSize, pbsmResults))

          // Check DJ result with indexed datasets
          val unitSquare = new EnvelopeNDLite(2, 0, 0, 1, 1)
          val firstIndex = firstDataset.spatialPartition(new GridPartitioner(unitSquare, Array(3, 3)))
          val secondIndex = secondDataset.spatialPartition(new GridPartitioner(unitSquare, Array(3, 3)))
          val djResults = SpatialJoin.spatialJoinDJ(firstIndex, secondIndex, ESJPredicate.MBRIntersects)
          assertResult(true, s"Error in DJ in iteration #$i")(resultsEqual(bnljResults, bnljResultSize, djResults))

          // Check REPJ results
          val repjResults = SpatialJoin.spatialJoinRepJ(firstIndex, secondIndex, ESJPredicate.MBRIntersects)
          assertResult(true, s"Error in REPJ in iteration #$i")(resultsEqual(bnljResults, bnljResultSize, repjResults))
        }
      }
    }
  }

  test("Stress test spatial join PBSM with different partitioners") {
    if (shouldRunStressTest()) {
      // Run random join cases and check that all algorithms produce the same result size
      // This is meant to make sure that corner cases are handled correctly
      // We hope that randomly generated data will produce many corner cases that should be handled
      val snapRecords: (IFeature => IFeature) = f => {
        val envelope = f.getGeometry.asInstanceOf[EnvelopeND]
        for (d <- 0 until envelope.getCoordinateDimension) {
          envelope.setMinCoord(d, (envelope.getMinCoord(d) / 0.05).floor * 0.05)
          envelope.setMinCoord(d, (envelope.getMinCoord(d) / 0.05).ceil * 0.05)
        }
        Feature.create(row = null, geometry = envelope)
      }
      val addRecordID: ((IFeature, Long)) => IFeature = fid => Feature.append(fid._1, fid._2, "ID")

      val distributions = Seq(UniformDistribution)
      for (i <- 0 until 10) {
        for (distribution <- distributions) {
          val firstDataset: SpatialRDD = sparkContext.generateSpatialData(distribution, 100,
            opts = Seq("seed" -> i, UniformDistribution.MaxSize -> "0.1,0.1", "geometry" -> "box"))
            .map(snapRecords)
            .zipWithUniqueId()
            .map(addRecordID)

          val secondDataset: SpatialRDD = sparkContext.generateSpatialData(UniformDistribution, 100,
            opts = Seq("seed" -> (i + 1), UniformDistribution.MaxSize -> "0.1,0.1", "geometry" -> "box"))
            .map(snapRecords)
            .zipWithUniqueId()
            .map(addRecordID)
          val bnljResults = firstDataset.spatialJoin(secondDataset, method = ESJDistributedAlgorithm.BNLJ)
          // We use the BNLJ as the baseline (groundtruth)
          val bnljResultSize = bnljResults.count()
          // Check PBSM result with various partitioners
          val partitioners = Array("grid", "rsgrove")
          for (partitioner <- partitioners) {
            val pbsmResults = firstDataset.spatialJoin(secondDataset, method = ESJDistributedAlgorithm.PBSM,
              opts = Seq(SpatialJoin.JoinWorkloadUnit -> "2k", SpatialJoin.PBSMPartitioner -> partitioner))
            assertResult(true, s"Error in PBSM in iteration #$i")(resultsEqual(bnljResults, bnljResultSize, pbsmResults))
          }
        }
      }
    }
  }

  test("Stress test with for point in polygon queries") {
    if (shouldRunStressTest()) {
      // Run random join cases and check that all algorithms produce the same result size
      // This is meant to make sure that corner cases are handled correctly
      // We hope that randomly generated data will produce many corner cases that should be handled
      val addRecordID: ((IFeature, Long)) => IFeature = fid => Feature.append(fid._1, fid._2, "ID")

      for (i <- 0 until 10) {
        val firstDataset: SpatialRDD = sparkContext.generateSpatialData(ParcelDistribution, 100,
          opts = Seq("seed" -> i, ParcelDistribution.Dither -> 0))
          .zipWithUniqueId()
          .map(addRecordID)

        val secondDataset: SpatialRDD = sparkContext.generateSpatialData(UniformDistribution, 100,
          opts = Seq("seed" -> (i + 1), ParcelDistribution.Dither -> 0))
          .zipWithUniqueId()
          .map(addRecordID)
        val bnljResults = firstDataset.spatialJoin(secondDataset, method = ESJDistributedAlgorithm.BNLJ,
          joinPredicate = ESJPredicate.MBRIntersects)
        // We use the BNLJ as the baseline (groundtruth)
        val bnljResultSize = bnljResults.count()
        // Check PBSM result with non-indexed data
        val pbsmResults = SpatialJoin.spatialJoinPBSM(firstDataset, secondDataset, ESJPredicate.MBRIntersects,
          opts = SpatialJoin.JoinWorkloadUnit -> "16m")
        assertResult(true, s"Error in iteration #$i")(resultsEqual(bnljResults, bnljResultSize, pbsmResults))
      }
    }
  }

  def resultsEqual(expected: RDD[(IFeature, IFeature)], expectedCount: Long,
                   actual: RDD[(IFeature, IFeature)]): Boolean = {
    val actualCount: Long = actual.count()
    // For speed, we only check the size
    if (actualCount == expectedCount)
      return true
    // If the sizes do not match, we check every record to report the error
    System.err.println(s"Expected $expectedCount results but got $actualCount")
    val expectedResult = expected.map(f1f2 => (f1f2._1.getAs[Long]("ID"), f1f2._2.getAs[Long]("ID")))
      .collect().sorted
    val actualResult = actual.map(f1f2 => (f1f2._1.getAs[Long]("ID"), f1f2._2.getAs[Long]("ID")))
      .collect().sorted

    var i: Int = 0
    var j: Int = 0
    while (i < expectedResult.length && j < actualResult.length) {
      if (j > 0  && actualResult(j) == actualResult(j - 1)) {
        System.err.println(s"Repeated entry in the answer ${expectedResult(j)}")
        return false
      }
      if (expectedResult(i) != actualResult(j)) {
        if (expectedResult(i)._1 < actualResult(j)._1 ||
          (expectedResult(i)._1 == actualResult(j)._1 && expectedResult(i)._2 <= actualResult(j)._2)) {
          System.err.println(s"Expected result missing ${expectedResult(i)}")
          return false
        } else {
          System.err.println(s"Additional result that was not expected ${actualResult(j)}")
          return false
        }
      }
      i += 1
      j += 1
    }
    if (i < expectedResult.length) {
      System.err.println(s"Expected result not found ${expectedResult(i)}")
      return false
    }
    if (j < actualResult.length) {
      System.err.println(s"Too many results. Unexpected result ${actualResult(j)}")
      return false
    }
    false
  }
}
