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
package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.cg.{GeometryQuadSplitter, PlaneSweepSelfJoinIterator, PlaneSweepSpatialJoinIterator, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{CellPartitioner, GridPartitioner, IndexHelper, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses.{Summary, Synopsis, UniformHistogram}
import edu.ucr.cs.bdlab.beast.util.{MathUtil, OperationMetadata, OperationParam}
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{HashPartitioner, SparkContext}

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

@OperationMetadata(shortName = "sj",
  description = "Computes spatial join that finds all overlapping features from two files.",
  inputArity = "2",
  outputArity = "?",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object SpatialJoin extends CLIOperation with Logging {

  @OperationParam(description = "The spatial join algorithm to use {'bnlj', 'dj', 'pbsm' = 'sjmr'}.", defaultValue = "auto")
  val SpatialJoinMethod: String = "method"

  @OperationParam(description = "The spatial predicate to use in the join. Supported predicates are {intersects, contains}", defaultValue = "intersects")
  val SpatialJoinPredicate: String = "predicate"

  @OperationParam(description = "Overwrite the output file if it exists", defaultValue = "true")
  val OverwriteOutput: String = "overwrite"

  @OperationParam(description = "Write the output to a file", defaultValue = "true", showInUsage = false)
  val WriteOutput: String = "output"

  @OperationParam(description = "Desired workload (in bytes) per task for spatial join algorithms",
    defaultValue = "32m", showInUsage = false)
  val JoinWorkloadUnit: String = "sjworkload"

  @OperationParam(description = "The maximum ratio of replication in PBSM",
    defaultValue = "0.02", showInUsage = false)
  val ReplicationBound: String = "pbsm.replication"

  @OperationParam(description = "A multiplier that calculates the size of the PBSM grid",
    defaultValue = "200", showInUsage = false)
  val PBSMMultiplier = "pbsmmultiplier"

  @OperationParam(description = "The partitioner to use with PBSM",
    defaultValue = "grid", showInUsage = false)
  val PBSMPartitioner = "pbsmpartitioner"

  @OperationParam(description =
"""
The average number of points per geometry after which geometries will be broken down using
the quad split algorithm to speedup the spatial join
""",
    defaultValue = "1000", showInUsage = false)
  val QuadSplitThreshold: String = "quadsplitthreshold"

  /**Instructs spatial join to not keep the geometry of the left dataset (for efficiency)*/
  val RemoveGeometry1: String = "SpatialJoin.RemoveGeometry1"

  /**Instructs spatial join to not keep the geometry of the right dataset (for efficiency)*/
  val RemoveGeometry2: String = "SpatialJoin.RemoveGeometry2"

  /** The name of the accumulator that records the total number of MBRTests */
  val MBRTestsAccumulatorName = "MBRTests"

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    // Set the split size to 16MB so that the code will not run out of memory
    sc.hadoopConfiguration.setLong("mapred.max.split.size", opts.getSizeAsBytes(JoinWorkloadUnit, "4m"))
    val mbrTests = sc.longAccumulator(MBRTestsAccumulatorName)
    val joinPredicate = opts.getEnumIgnoreCase(SpatialJoinPredicate, ESJPredicate.Intersects)
    val f1rdd = sc.spatialFile(inputs(0), opts.retainIndex(0))
    val f2rdd = sc.spatialFile(inputs(1), opts.retainIndex(1))
    val joinResults = spatialJoin(f1rdd, f2rdd, joinPredicate, null, mbrTests, opts)
    if (opts.getBoolean(WriteOutput, true)) {
      val outPath = new Path(outputs(0))
      // Delete output file if exists and overwrite flag is on
      val fs = outPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(outPath) && opts.getBoolean(SpatialWriter.OverwriteOutput, false))
        fs.delete(outPath, true)
      val resultSize = sc.longAccumulator("resultsize")
      joinResults.map(f1f2 => {
        resultSize.add(1)
        f1f2._1.toString + f1f2._2.toString
      }).saveAsTextFile(outputs(0))
      logInfo(s"Join result size is ${resultSize.value}")
    } else {
      // Skip writing the output. Join count
      val resultSize = joinResults.count()
      logInfo(s"Join result size is ${resultSize}")
    }
    logInfo(s"Total number of MBR tests is ${mbrTests.value}")
  }

  /**
   * The main entry point for spatial join operations.
   * Performs a spatial join between the given two inputs and returns an RDD of pairs of matching features.
   * This method is a transformation. However, if the [[ESJDistributedAlgorithm.PBSM]] is used, the MBR of the two
   * inputs has to be calculated first which runs a reduce action on each dataset even if the output of the spatial
   * join is not used.
   *
   * You can specify a specific spatial join method through the [[joinMethod]] parameter. If not specified, an
   * algorithm will be picked automatically based on the following rules.
   *  - If both datasets are spatially partitioned, the distributed join [[ESJDistributedAlgorithm.DJ]] algorithm is used.
   *  - If the product of the number of partitions of both datasets is less than [[SparkContext.defaultParallelism]],
   *    then the block nested loop join is used [[ESJDistributedAlgorithm.BNLJ]]
   *  - If at least one dataset is partition, then the repartition join is used [[ESJDistributedAlgorithm.REPJ]]
   *  - If none of the above, then the partition based spatial merge join is used [[ESJDistributedAlgorithm.PBSM]]
   *
   * @param r1 the first (left) dataset
   * @param r2 the second (right) dataset
   * @param joinPredicate the join predicate. The default is [[ESJPredicate.Intersects]] which finds all non-disjoint
   *                      features
   * @param joinMethod the join algorithm. If not specified the algorithm automatically chooses an algorithm based
   *                   on the heuristic described above.
   * @param mbrCount an (optional) accumulator to count the number of MBR tests during the algorithm.
   * @return an RDD that contains pairs of matching features.
   */
  def spatialJoin(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate = ESJPredicate.Intersects,
                  joinMethod: ESJDistributedAlgorithm = null, mbrCount: LongAccumulator = null,
                  opts: BeastOptions = new BeastOptions()): RDD[(IFeature, IFeature)] = {
    val removeGeometry1 = opts.getBoolean(RemoveGeometry1, false)
    val removeGeometry2 = opts.getBoolean(RemoveGeometry2, false)
    val joinWorkloadUnit: Long = opts.getSizeAsBytes(JoinWorkloadUnit, "32m")
    val quadSplitThreshold: Int = opts.getInt(QuadSplitThreshold, 1000)
    // If the joinMethod is not set, choose an algorithm automatically according to the following rules
    val joinAlgorithm = if (joinMethod != null) {
      logDebug(s"Use SJ algorithm $joinMethod as provided by user")
      joinMethod
    } else if (r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm DJ because both inputs are partitioned")
      ESJDistributedAlgorithm.DJ
    } else if (r1.getNumPartitions * r2.getNumPartitions < r1.sparkContext.defaultParallelism) {
      logDebug("Use SJ algorithm BNLJ because the total number of partitions is less than parallelism " +
        s"${r1.getNumPartitions * r2.getNumPartitions} < ${r1.sparkContext.defaultParallelism}")
      ESJDistributedAlgorithm.BNLJ
    } else if (r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm RepJ because one input is partitioned")
      ESJDistributedAlgorithm.REPJ
    } else {
      logDebug("Use SJ algorithm PBSM because inputs are not partitioned")
      ESJDistributedAlgorithm.PBSM
    }

    val inputsPartitioned: Boolean = r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned

    // Run the spatial join algorithm
    var joinResult: RDD[(IFeature, IFeature)] = joinAlgorithm match {
      case ESJDistributedAlgorithm.BNLJ =>
        spatialJoinBNLJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.PBSM | ESJDistributedAlgorithm.SJMR =>
        spatialJoinPBSM(r1, r2, joinPredicate, mbrCount, opts)
      case ESJDistributedAlgorithm.DJ if inputsPartitioned =>
        spatialJoinDJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.DJ =>
        logWarning("Cannot run DJ with non-partitioned input. Falling back to BNLJ")
        spatialJoinBNLJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.REPJ =>
        spatialJoinRepJ(r1, r2, joinPredicate, mbrCount)
      case ESJDistributedAlgorithm.SJ1D =>
        spatialJoin1DPartitioning(r1, r2, joinPredicate, opts, mbrCount)
      case _other => throw new RuntimeException(s"Unrecognized spatial join method ${_other}. " +
        s"Please specify one of {'pbsm'='sjmr', 'dj', 'repj', 'bnlj'}")
    }
    if (removeGeometry1) {
      // Remove the smaller geometry from the first dataset
      joinResult = joinResult.map(f1f2 => {
        val f = f1f2._1
        var values: Seq[Any] = Row.unapplySeq(f).get
        values = values.slice(1, values.length)
        var schema: Seq[StructField] = f.schema
        schema = schema.slice(1, schema.length)
        (new Feature(values.toArray, StructType(schema)), f1f2._2)
      })
    }
    if (removeGeometry2) {
      // Remove the smaller geometry from the second dataset
      joinResult = joinResult.map(f1f2 => {
        val f = f1f2._2
        var values: Seq[Any] = Row.unapplySeq(f).get
        values = values.slice(1, values.length)
        var schema: Seq[StructField] = f.schema
        schema = schema.slice(1, schema.length)
        (f1f2._1, new Feature(values.toArray, StructType(schema)))
      })
    }
    joinResult
  }

  /**
   * Spatial join for two JavaRDDs. A Java shortcut for [[spatialJoin()]]
   * @param r1 first spatial RDD (left)
   * @param r2 second spatial RDD (right)
   * @param joinPredicate the spatial predicate for the join
   * @param joinMethod the algorithm to use in the join. If null, an algorithm is auto-selected based on the inputs.
   * @param mbrCount an optional accumulator to keep track of the number of MBR tests
   * @param opts additional options to further customize the join algorithm
   * @return
   */
  def spatialJoin(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: ESJPredicate,
                  joinMethod: ESJDistributedAlgorithm, mbrCount: LongAccumulator,
                  opts: BeastOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoin(r1.rdd, r2.rdd, joinPredicate, joinMethod, mbrCount, opts))

  /**
   * Runs a plane-sweep algorithm between the given two arrays of input features and returns an iterator of
   * pairs of features.
   * @param r the first set of features
   * @param s the second set of features
   * @param dupAvoidanceMBR the duplicate avoidance MBR to run the reference point technique.
   * @param joinPredicate the join predicate to match features
   * @param numMBRTests an (optional) accumulator to count the number of MBR tests
   * @tparam T1 the type of the first dataset
   * @tparam T2 the type of the second dataset
   * @return an iterator over pairs of features
   */
  private[beast] def spatialJoinIntersectsPlaneSweepFeatures[T1 <: IFeature, T2 <: IFeature]
      (r: Array[T1], s: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, joinPredicate: ESJPredicate,
       numMBRTests: LongAccumulator): TraversableOnce[(IFeature, IFeature)] = {
    if (r.isEmpty || s.isEmpty)
      return Seq()
    logDebug(s"Joining ${r.length} x ${s.length} records using planesweep")
    val refine: ((_ <: IFeature, _ <: IFeature)) => Boolean = refiner(joinPredicate)
    new PlaneSweepSpatialJoinIterator(r, s, dupAvoidanceMBR, numMBRTests)
      .filter(refine)
  }

  /**
   * Returns the correct refinement function for the given predicate
   * @param joinPredicate
   * @return
   */
  private def refiner(joinPredicate: ESJPredicate): ((_ <: IFeature, _ <: IFeature)) => Boolean = joinPredicate match {
    case ESJPredicate.Contains => p =>
      try {
        p._1.getGeometry.contains(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException => logWarning(s"Error comparing records", e); false
      }
    case ESJPredicate.Intersects => p =>
      try {
        p._1.getGeometry.intersects(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException => logWarning(s"Error comparing records", e); false
      }
    // For MBR intersects, we still need to check the envelopes since the planesweep algorithm is approximate
    // The planesweep algorithm works only with rectangles
    case ESJPredicate.MBRIntersects => p => {
      val env1: EnvelopeNDLite = new EnvelopeNDLite().merge(p._1.getGeometry)
      val env2: EnvelopeNDLite = new EnvelopeNDLite().merge(p._2.getGeometry)
      env1.intersectsEnvelope(env2)
    }
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @param opts          Additional options for the PBSM algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate, numMBRTests: LongAccumulator = null,
                      opts: BeastOptions = new BeastOptions()): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r1 and r2 for PBSM")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]
    // Merge the two samples into one
    val sample1 = synopsis1.sample
    val sample2 = synopsis2.sample
    val sample = new Array[Array[Double]](sample1.length)
    for (d <- sample1.indices) {
      sample(d) = if (!sample1.isEmpty && !sample2.isEmpty)
        sample1(d) ++ sample2(d)
      else if (!sample1.isEmpty)
        sample1(d)
      else if (!sample2.isEmpty)
        sample2(d)
      else
        Array[Double]()
    }
    // Remove samples that are not in the intersection MBR
    var sampleSize = 0
    for (i <- sample(0).indices) {
      var pointInside: Boolean = true
      var d = 0
      while (d < sample.length && pointInside) {
        pointInside = sample(d)(i) >= intersectionMBR.getMinCoord(d) && sample(d)(i) < intersectionMBR.getMaxCoord(d)
       d += 1
      }
      if (pointInside && sampleSize < i) {
        for (d <- sample.indices)
          sample(d)(sampleSize) = sample(d)(i)
        sampleSize += 1
      }
    }
    // Shrink the array by removing all filtered out elements
    for (d <- sample.indices)
      sample(d) = sample(d).slice(0, sampleSize)

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numCells: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    logDebug(s"PBSM is using $numCells cells")
    val pbsmPartitioner: String = opts.getString(PBSMPartitioner, "grid")
    val partitionerClass: Class[_<: SpatialPartitioner] = IndexHelper.partitioners(pbsmPartitioner)
    val partitioner = partitionerClass.newInstance()
    partitioner.setup(opts, true)
    val intersectionSummary = new Summary()
    intersectionSummary.set(intersectionMBR)
    val intersectionHistogram = new UniformHistogram(intersectionMBR, 128, 128)
    intersectionHistogram.mergeNonAligned(synopsis1.histogram.asInstanceOf[UniformHistogram])
    intersectionHistogram.mergeNonAligned(synopsis2.histogram.asInstanceOf[UniformHistogram])
    partitioner.construct(intersectionSummary, sample, intersectionHistogram, numCells)
    logInfo(s"PBSM is using partitioner $partitioner with $numCells cells")
    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("SpatialJoin", s"PBSM is using ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: ESJPredicate,
                      numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinPBSM(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: ESJPredicate)
  : JavaPairRDD[IFeature, IFeature] = spatialJoinPBSM(r1, r2, joinPredicate, null)

  /**
   * Runs a spatial join between the two given RDDs using the block-nested-loop join algorithm.
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @return
   */
  def spatialJoinBNLJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate, numMBRTests: LongAccumulator = null)
        : RDD[(IFeature, IFeature)] = {
    // Convert the two RDD to arrays
    val f1: RDD[Array[IFeature]] = r1.glom()
    val f2: RDD[Array[IFeature]] = r2.glom()

    // Combine them using the Cartesian product (as in block nested loop)
    val f1f2 = f1.cartesian(f2)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Block-nested loop join with ${f1f2.getNumPartitions} partitions")

    // For each pair of blocks, run the spatial join algorithm
    f1f2.flatMap(p1p2 => {
      // Extract the two arrays of features
      val p1: Array[IFeature] = p1p2._1
      val p2: Array[IFeature] = p1p2._2

      // Duplicate avoidance MBR is set to infinity (include all space) in the BNLJ algorithm
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      dupAvoidanceMBR.setInfinite()
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /** Java shortcut */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: ESJPredicate, numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /** Java shortcut without MBR count */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: ESJPredicate): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, null))

  /**
   * Distributed join algorithm between spatially partitioned RDDs
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @param numMBRTests   a counter that will contain the number of MBR tests
   * @return a pair RDD for joined features
   */
  def spatialJoinDJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate, numMBRTests: LongAccumulator = null):
      RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned, "r1 should be spatially partitioned")
    require(r2.isSpatiallyPartitioned, "r2 should be spatially partitioned")
    // Skip duplicate avoidance from both input RDDs since this algorithm relies on the replication
    SpatialFileRDD.skipDuplicateAvoidance(r1)
    SpatialFileRDD.skipDuplicateAvoidance(r2)
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")
    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /***
   * Repartition join algorithm between two datasets: r1 is spatially disjoint partitioned and r2 is not
   * @param r1 the first dataset
   * @param r2 the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests an optional accumulator that counts the number of MBR tests
   * @return an RDD of pairs of matching features
   */
  def spatialJoinRepJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate, numMBRTests: LongAccumulator = null):
      RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned,
      "Repartition join requires at least one of the two datasets to be spatially partitioned")
    // Choose which dataset to repartition, 1 for r1, and 2 for r2
    // If only one dataset is partitioned, always repartition the other one
    // If both are partitioned, repartition the smaller one
    val whichDatasetToPartition: Int = if (!r1.isSpatiallyPartitioned)
      1
    else if (!r2.isSpatiallyPartitioned)
      2
    else {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      // Choose the smaller dataset
      r1.sparkContext.setJobGroup("Analyzing", "Estimating the size of r1 and r2 for REPJ")
      val mbr1Async = Future { r1.summary }
      val mbr2Async = Future { r2.summary }
      val size1: Long = concurrent.Await.result(mbr1Async, Duration.Inf).size
      val size2: Long = concurrent.Await.result(mbr2Async, Duration.Inf).size
      if (size1 < size2) 1 else 2
    }
    val (r1Partitioned: SpatialRDD, r2Partitioned: SpatialRDD) = if (whichDatasetToPartition == 1) {
      // Repartition r1 according to r2
      val referencePartitioner = new CellPartitioner(r2.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r2)
      val r1Partitioned = r1.spatialPartition(referencePartitioner)
      (r1Partitioned, r2)
    } else {
      // Repartition r2 according to r1
      val referencePartitioner = new CellPartitioner(r1.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r1)
      val r2Partitioned = r2.spatialPartition(referencePartitioner)
      (r1, r2Partitioned)
    }
    val joined: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1Partitioned, r2Partitioned)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Repartition Join with ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val dupAvoidanceMBR: EnvelopeNDLite = r._1
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  def spatialJoin1DPartitioning(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: ESJPredicate,
                                opts: BeastOptions = new BeastOptions(),
                                numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r1 and r2 for PBSM")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val partitioner = new GridPartitioner(intersectionMBR, Array(1, numStrips))
    logInfo(s"SJ1D is using partitioner $partitioner")
    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("SpatialJoin", s"SJ1D is using ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /**
   * Runs a self-join on the given set of data.
   * @param r the set of features to self-join
   * @param joinPredicate
   * @param numMBRTests
   * @return
   */
  def selfJoinDJ(r: SpatialRDD, joinPredicate: ESJPredicate, numMBRTests: LongAccumulator = null):
    RDD[(IFeature, IFeature)] = {
    val rPartitioned = if (r.isSpatiallyPartitioned && r.getSpatialPartitioner.isDisjoint) r
        else r.spatialPartition(classOf[RSGrovePartitioner], r.getNumPartitions, "disjoint" -> true)
    val partitioner = rPartitioned.getSpatialPartitioner
    rPartitioned.mapPartitionsWithIndex((id, features) => {
      val refPointMBR: EnvelopeNDLite = partitioner.getPartitionMBR(id)
      new PlaneSweepSelfJoinIterator(features.toArray, refPointMBR, numMBRTests)
        .filter(refiner(joinPredicate))
    })
  }
}
