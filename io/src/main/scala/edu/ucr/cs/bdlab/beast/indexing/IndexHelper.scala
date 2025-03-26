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
package edu.ucr.cs.bdlab.beast.indexing

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaPartitionedSpatialRDD, JavaSpatialRDD, PartitionedSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, GeometryHelper, IFeature}
import edu.ucr.cs.bdlab.beast.io.{FeatureWriter, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses._
import edu.ucr.cs.bdlab.beast.util.{IntArray, OperationHelper, OperationParam}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskFailureListener

import java.io.PrintStream

/**
 * A helper object for creating indexes and partitioning [[SpatialRDD]]s
 */
object IndexHelper extends Logging {
  /**The different ways for specifying the number of partitions*/
  trait PartitionCriterion
  /**The number of partitions is explicitly specified*/
  case object Fixed extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a number of features*/
  case object FeatureCount extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a specified size*/
  case object Size extends PartitionCriterion

  /**Information that is used to calculated the number of partitions*/
  case class NumPartitions(pc: PartitionCriterion, value: Long)

  /**The type of the global index (partitioner)*/
  @OperationParam(
    description = "The type of the global index",
    required = false,
    defaultValue = "rsgrove"
  )
  val GlobalIndex = "gindex"

  /**Whether to build a disjoint index (with no overlapping partitions)*/
  @OperationParam(
    description = "Build a disjoint index with no overlaps between partitions",
    defaultValue = "false"
  )
  val DisjointIndex = "disjoint"

  /**The size of the synopsis used to summarize the input before building the index*/
  @OperationParam(
    description = "The size of the synopsis used to summarize the input, e.g., 1024, 10m, 1g",
    defaultValue = "10m"
  )
  val SynopsisSize = "synopsissize";

  /**A flag to increase the load balancing by using the histogram with the sample, if possible*/
  @OperationParam(
    description = "Set this option to combine the sample with a histogram for accurate load balancing",
    defaultValue = "true"
  )
  val BalancedPartitioning = "balanced";

  /**The criterion used to calculate the number of partitions*/
  @OperationParam(
    description =
      """The criterion used to compute the number of partitions. It can be one of:
- Fixed(n): Create a fixed number of partitions (n partitions)
- Size(s): Create n partitions such that each partition contains around s bytes
- Count(c): Create n partitions such that each partition contains around c records""",
    defaultValue = "Size(128m)"
  )
  val PartitionCriterionThreshold = "pcriterion";

  // ---- The following set of functions help in creating a partitioner from a SpatialRDD and a partitioner class

  /**
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param numPartitions the desired number of partitions
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(numPartitions: NumPartitions, summary: Summary): Int = numPartitions.pc match {
    case Fixed => numPartitions.value.toInt
    case FeatureCount => Math.ceil(summary.numFeatures.toDouble / numPartitions.value).toInt
    case Size => Math.ceil(summary.size.toDouble / numPartitions.value).toInt
  }

  /**
   * (Java shortcut to)
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param pcriterion the criterion used to define the number of partitions
   * @param value the value associated with the criterion
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(pcriterion: String, value: Long, summary: Summary): Int = {
    val pc: PartitionCriterion = pcriterion.toLowerCase match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    computeNumberOfPartitions(NumPartitions(pc, value), summary)
  }

  /**
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param numPartitions the desired number of partitions (this is just a loose hint not a strict number)
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: SpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        numPartitions: NumPartitions,
                        sizeFunction: IFeature=>Int,
                        opts: BeastOptions
                       ): SpatialPartitioner = {
    // The size of the synopsis (summary) that will be created
    val synopsisSize = opts.getSizeAsBytes(SynopsisSize, "10m")
    // Whether to generate a disjoint index (if supported)
    val disjoint = opts.getBoolean(DisjointIndex, false)
    // Whether to generate a highly-balanced partitioning using a histogram (if supported)
    val balanced = opts.getBoolean(BalancedPartitioning, true)

    // Calculate the summary
    val t1 = System.nanoTime()
    val (histogram, sampleCoordinates, summary) =
        summarizeDataset(features.filter(f => f.getGeometry != null && !f.getGeometry.isEmpty),
          partitionerClass, synopsisSize, sizeFunction, balanced)

    val t2 = System.nanoTime

    // Now that the input set has been summarized, we can create the partitioner
    val numCells: Int = computeNumberOfPartitions(numPartitions, summary)
    if (numCells == 1) {
      logInfo("Input too small. Creating a cell partitioner with one cell")
      // Create a cell partitioner that contains one cell that represents the entire input
      val universe = new EnvelopeNDLite(summary)
      universe.setInfinite()
      new CellPartitioner(universe)
      // Notice that it might be possible to avoid computing the histogram and sample. However, it is not worth it
      // since this case happens only for small datasets
    } else {
      val spatialPartitioner: SpatialPartitioner = partitionerClass.newInstance
      spatialPartitioner.setup(opts, disjoint)
      val pMetadata = spatialPartitioner.getMetadata
      if (disjoint && !pMetadata.disjointSupported)
        throw new RuntimeException("Partitioner " + partitionerClass + " does not support disjoint partitioning")

      // Construct the partitioner
      val nump: Int = computeNumberOfPartitions(numPartitions, summary)
      spatialPartitioner.construct(summary, sampleCoordinates, histogram, nump)
      val t3 = System.nanoTime
      logInfo(f"Synopses created in ${(t2 - t1) * 1E-9}%f seconds and partitioner '${partitionerClass.getSimpleName}' " +
        f" constructed in ${(t3 - t2) * 1E-9}%f seconds")
      spatialPartitioner
    }
  }

  /**
   * (Java shortcut to)
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param pcriterion the partition criterion {fixed, count, size}
   * @param pvalue the value of partition criterion
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: JavaSpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        pcriterion: String,
                        pvalue: Long,
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int] = {_.getStorageSize},
                        opts: BeastOptions
                       ): SpatialPartitioner = {
    require(sizeFunction != null, "Size function cannot be null. You can use {IFeature::getStorageSize} as a default.")
    val pc = pcriterion match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    createPartitioner(features.rdd, partitionerClass, NumPartitions(pc, pvalue), f => sizeFunction.call(f), opts)
  }

  /**
   * Compute up-to three summaries as supported by the partitioner.
   * [[HistogramOP]].Sparse method since the histogram size is usually large.
   * @param features the features to summarize
   * @param partitionerClass the partitioner class to compute the summaries for
   * @param summarySize the total summary size (combined size for sample and histogram)
   * @param sizeFunction the function the calculates the size of each feature (if size is needed)
   * @param balancedPartitioning set to true if balanced partitioning is desired
   * @return the three computed summaries with nulls for non-computed ones
   */
  private[beast] def summarizeDataset(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                                      summarySize: Long, sizeFunction: IFeature=>Int, balancedPartitioning: Boolean)
      : (UniformHistogram, Array[Array[Double]], Summary) = {
    // The summary is always computed
    var summary: Summary = null
    var sampleCoordinates: Array[Array[Double]] = null
    var histogram: UniformHistogram = null

    // Retrieve the construct method to determine the required parameters
    val constructMethod = partitionerClass.getMethod("construct", classOf[Summary],
      classOf[Array[Array[Double]]], classOf[AbstractHistogram], classOf[Int])
    val parameterAnnotations = constructMethod.getParameterAnnotations
    // Determine whether the sample or the histogram (or both) are needed to construct the partitioner
    val sampleNeeded = parameterAnnotations(1).exists(p => p.isInstanceOf[SpatialPartitioner.Required] ||
      p.isInstanceOf[SpatialPartitioner.Preferred])
    val histogramNeeded = parameterAnnotations(2).exists(p => p.isInstanceOf[SpatialPartitioner.Required]) ||
      (balancedPartitioning && parameterAnnotations(2).exists(p => p.isInstanceOf[SpatialPartitioner.Preferred]))

    // If both sample and histogram are required, reduce the size of the synopsis size to accommodate both
    val synopsisSize = if (sampleNeeded && histogramNeeded) summarySize / 2 else summarySize
    if (!sampleNeeded && !histogramNeeded) {
      // Compute the summary directly
      summary = Summary.computeForFeatures(features, sizeFunction)
    } else if (sampleNeeded) {
      // Read sample and compute summary using accumulator
      // We use a number of dimensions of two since we did not calculate the summary yet
      val numDimensions = 2
      val sampleSize = (synopsisSize / (8 * numDimensions)).toInt
      val acc = Summary.createSummaryAccumulator(features.sparkContext, sizeFunction)
      sampleCoordinates = PointSampler.pointSample(features.map(f => {acc.add(f); f}), sampleSize, 0.01)
      summary = new Summary(acc.value)
      if (sampleCoordinates == null || sampleCoordinates.isEmpty ||
        (sampleCoordinates(0).length < sampleSize && sampleCoordinates(0).length < summary.numFeatures)) {
        // Fall safe for the case where the input size is very small. Mostly used in testing.
        sampleCoordinates = PointSampler.pointSample(features.map(f => {acc.add(f); f}), sampleSize, 1.0)
      }
      if (histogramNeeded) {
        // Compute histogram given the summary
        val numBuckets = (synopsisSize / 8).toInt
        histogram = HistogramOP.computePointHistogramSparse(features, sizeFunction, summary, numBuckets)
      }
    }
    (histogram, sampleCoordinates, summary)
  }

  /**
   * Parse the partition criterion and value in the form "method(value)"
   * @param criterionValue a user-given string in the form "method(value)"
   * @return the parsed partition criterion and value
   */
  def parsePartitionCriterion(criterionValue: String): NumPartitions = {
    val pCriterionRegexp = raw"(fixed|count|size)+\((\w+)\)".r
    criterionValue.toLowerCase match {
      case pCriterionRegexp(method, value) => {
        val pc = method match {
          case "fixed" => Fixed
          case "count" => FeatureCount
          case "size" => Size
        }
        val pvalue: Long = StringUtils.TraditionalBinaryPrefix.string2long(value)
        NumPartitions(pc, pvalue)
      }
    }
  }

  // ---- The following set of functions partition and RDD to generate a partitioned RDD using a partitioner instance

  /**
   * :: DeveloperApi :: Assigns each record to one or more partitions based on the given partitioner.
   * NOTE: This method does NOT partition the records; it just assigns each record to the overlapping partitions ID(s).
   * Each record stays in its own RDD partition.
   * @param features the set of features to assign to partitions
   * @param spatialPartitioner the partitioner to use to assign features to partitions
   * @return a new RDD where each feature is assigned to all overlapping partitions
   */
  @DeveloperApi def _assignFeaturesToPartitions(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): RDD[(Int, IFeature)] = {
    val featuresToPartitions: SpatialRDD = runDuplicateAvoidance(features)
    val mbr: EnvelopeNDLite = new EnvelopeNDLite(spatialPartitioner.getCoordinateDimension)
    if (!spatialPartitioner.isDisjoint) {
      // Non disjoint partitioners are easy as each feature is assigned to exactly one partition
      featuresToPartitions.map(f => {
        mbr.setEmpty()
        (spatialPartitioner.overlapPartition(mbr.merge(f.getGeometry)), f)
      })
    } else {
      // Disjoint partitioners need us to create a list of partition IDs for each record
      featuresToPartitions.flatMap(f => {
        val matchedPartitions = new IntArray
        mbr.setEmpty()
        mbr.merge(f.getGeometry)
        spatialPartitioner.overlapPartitions(mbr, matchedPartitions)
        val resultingPairs = Array.ofDim[(Int, IFeature)](matchedPartitions.size())
        for (i <- 0 until matchedPartitions.size())
          resultingPairs(i) = (matchedPartitions.get(i), f)
        resultingPairs
      })
    }
  }

  /**
    * Partitions the given features using an already initialized [[SpatialPartitioner]].
    *
    * @param features the features to partition
    * @param spatialPartitioner the spatial partitioner to partition the features with
    * @return an RDD of (partition number, IFeature)
    * @deprecated Use [[partitionFeatures2]] instead
   */
  def partitionFeatures(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): PartitionedSpatialRDD = {
    val partitionIDFeaturePairs = _assignFeaturesToPartitions(features, spatialPartitioner)
    // Enforce the partitioner to shuffle records by partition ID
    partitionIDFeaturePairs.partitionBy(spatialPartitioner)
  }

  /**
   * Partitions the given features using an already initialized [[SpatialPartitioner]]
   * @param features the features to partition
   * @param spatialPartitioner the spatial partition to use.
   * @return a [[SpatialRDD]] that is partitioned
   */
  def partitionFeatures2(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): SpatialRDD = {
    _assignFeaturesToPartitions(features, spatialPartitioner)
      .partitionBy(spatialPartitioner)
      .mapPartitions(_.map(_._2), preservesPartitioning = true)
  }

  /**
   * Run the duplicate avoidance technique on the given set of features if it is spatially partitioned
   * using a disjoint partitioner. Otherwise, the input set is returned as-is.
   * @param features the set of features to remove the duplicates from.
   * @return a set of features with all duplicates removed.
   */
  private[beast] def runDuplicateAvoidance(features: SpatialRDD): SpatialRDD = {
    val partitioner = features.partitioner
    if (partitioner.isEmpty || !partitioner.get.isInstanceOf[SpatialPartitioner])
      return features
    val spatialPartitioner = partitioner.get.asInstanceOf[SpatialPartitioner]
    if (!spatialPartitioner.isDisjoint)
      return features
    features.mapPartitionsWithIndex((partitionID, features) => {
      val referenceMBR = spatialPartitioner.getPartitionMBR(partitionID)
      val geometryMBR: EnvelopeNDLite = new EnvelopeNDLite(referenceMBR.getCoordinateDimension)
      val referencePoint: Array[Double] = new Array[Double](referenceMBR.getCoordinateDimension)
      features.filter(f => {
        geometryMBR.setEmpty()
        geometryMBR.merge(f.getGeometry)
        for (d <- 0 until geometryMBR.getCoordinateDimension)
          referencePoint(d) = geometryMBR.getMinCoord(d)
        referenceMBR.containsPoint(referencePoint)
      })
    }, preservesPartitioning = true)
  }

  /**
   * Partition features using an already initialized [[SpatialPartitioner]] from Java
   *
   * @param features the set of features to partition
   * @param partitioner an already initialized partitioner
   * @return a JavaPairRDD where the key represents the partition number and the value is the feature.
   * @deprecated use [[partitionFeatures2(JavaRDD[IFeature], SpatialPartitioner)]]
   */
  @deprecated("Use partitionFeatures2", "0.9.2")
  def partitionFeatures(features: JavaSpatialRDD, partitioner: SpatialPartitioner): JavaPairRDD[Integer, IFeature] = {
    val pairs: RDD[(Integer, IFeature)] = IndexHelper
      ._assignFeaturesToPartitions(features.rdd, partitioner)
      .map(kv => (kv._1, kv._2))
    JavaPairRDD.fromRDD(pairs.partitionBy(partitioner))
  }

  /**
   * Partitions a JavaSpatialRDD using the given spatial partitioner and returns a new partitioned RDD.
   * If the given partitioner is configured to be disjoint, the returned RDD might contain some replication.
   * @param features the set of features to partition
   * @param partitioner the partitioner to use to partition the features
   * @return the partitioned RDD
   */
  def partitionFeatures2(features: JavaSpatialRDD, partitioner: SpatialPartitioner): JavaSpatialRDD =
    JavaRDD.fromRDD(partitionFeatures2(features.rdd, partitioner))

  // ---- The following set of functions partition a SpatialRDD given a partitioner class

  /**
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   *
   * @param features         the RDD of features to partition
   * @param partitionerClass the partitioner class to use for partitioning
   * @param opts             any user options to use while creating the partitioner
   * @deprecated use [[partitionFeatures2]]
   */
  @deprecated("Use partitionFeatures2", "0.9.2")
  def partitionFeatures(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: IFeature=>Int, opts: BeastOptions): PartitionedSpatialRDD = {
    val pInfo = parsePartitionCriterion(opts.getString(IndexHelper.PartitionCriterionThreshold, "Size(128m)"))
    val spatialPartitioner = createPartitioner(features, partitionerClass, pInfo, sizeFunction, opts)
    partitionFeatures(features, spatialPartitioner)
  }

  /**
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   * @param features the set of features to spatially partition
   * @param partitionerClass the type of the spatial partition
   * @param sizeFunction the function used to computed the size
   * @param opts additional options
   * @return the same set of input features after they are partitioned.
   */
  def partitionFeatures2(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: IFeature=>Int, opts: BeastOptions): SpatialRDD = {
    val pInfo = parsePartitionCriterion(opts.getString(IndexHelper.PartitionCriterionThreshold, "Size(128m)"))
    val spatialPartitioner = createPartitioner(features, partitionerClass, pInfo, sizeFunction, opts)
    partitionFeatures2(features, spatialPartitioner)
  }


  /**
   * (Java shortcut to)
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   *
   * @param features         the RDD of features to partition
   * @param partitionerClass the partitioner class to use for partitioning
   * @param opts             any user options to use while creating the partitioner
   * @deprecated use [[partitionFeatures2]]
   */
  @deprecated("Use partitionFeatures2", "0.9.2")
  def partitionFeatures(features: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int], opts: BeastOptions)
      : JavaPartitionedSpatialRDD = {
    val pInfo = parsePartitionCriterion(opts.getString(IndexHelper.PartitionCriterionThreshold, "Size(128m)"))
    val spatialPartitioner = createPartitioner(features.rdd, partitionerClass, pInfo, f => sizeFunction.call(f), opts)
    partitionFeatures(features, spatialPartitioner)
  }

  /**
   * (Java shortcut) Partition features based on the given partitioner class.
   * @param features a set of features
   * @param partitionerClass the class of the partitioner
   * @param sizeFunction the function that estimates the size of each record
   * @param opts additional options
   * @return a new RDD where the features are partitioned based on the given partitioner class
   */
  def partitionFeatures2(features: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                         opts: BeastOptions) : JavaSpatialRDD =
    JavaRDD.fromRDD(partitionFeatures2(features.rdd, partitionerClass, { f => sizeFunction.call(f) }, opts))


  // ---- The following functions provides access to the set of configured partitioners
  /** A table of all the partitioners available */
  lazy val partitioners: Map[String, Class[_ <: SpatialPartitioner]] = {
    val ps: scala.collection.mutable.TreeMap[String, Class[_ <: SpatialPartitioner]] =
      new scala.collection.mutable.TreeMap[String, Class[_ <: SpatialPartitioner]]()

    val partitionerClasses: java.util.List[String] = OperationHelper.readConfigurationXML("beast.xml").get("SpatialPartitioners")
    val partitionerClassesIterator = partitionerClasses.iterator()
    while (partitionerClassesIterator.hasNext) {
      val partitionerClassName = partitionerClassesIterator.next()
      try {
        val partitionerClass = Class.forName(partitionerClassName).asSubclass(classOf[SpatialPartitioner])
        val metadata = partitionerClass.getAnnotation(classOf[SpatialPartitioner.Metadata])
        if (metadata == null)
          logWarning(s"Skipping partitioner '${partitionerClass.getName}' without a valid Metadata annotation")
        else
          ps.put(metadata.extension, partitionerClass)
      } catch {
        case e: ClassNotFoundException =>
          e.printStackTrace()
      }
    }
    ps.toMap
  }

  import scala.collection.convert.ImplicitConversionsToJava._
  /**
   * (Java shortcut to) Return the set of partitioners defined in the configuration files.
   */
  def getPartitioners: java.util.Map[String, Class[_ <: SpatialPartitioner]] = partitioners

  /**
   * (Java shortcut to) Save a partitioner dataset as a global index file to disk
   *
   * @param partitionedFeatures features that are already partitioned using a spatial partitioner
   * @param path path to the output file to be written
   * @param opts any additional user options
   * @deprecated Use [[saveIndex2(RDD[IFeature], String, BeastOptions)]]
   */
  @deprecated("Use saveIndex2", "0.9.2")
  def saveIndex(partitionedFeatures: JavaPartitionedSpatialRDD, path: String, opts: BeastOptions): Unit =
    saveIndex2(partitionedFeatures.rdd.mapPartitions(_.map(_._2), true), path, opts)

  /**
    * Save a partitioner dataset as a global index file to disk
    *
    * @param partitionedFeatures features that are already partitioned using a spatial partitioner
    * @param path path to the output file to be written
    * @param opts any additional user options
    * @deprecated Use [[saveIndex2(RDD[IFeature], String, BeastOptions)]]
    */
  @deprecated("Use saveIndex2", "0.9.2")
  def saveIndex(partitionedFeatures: PartitionedSpatialRDD, path: String, opts: BeastOptions): Unit =
    saveIndex2(partitionedFeatures.mapPartitions(_.map(_._2), true), path, opts)

  /**
   * Java shortcut to save partitioned data as index. See [[saveIndex2]]
   * @param partitionFeatures a set of spatially partitioned features
   * @param path the path to write the index to
   * @param opts additional options for writing the output such as file output format for each partition
   */
  def saveIndex2J(partitionFeatures: JavaSpatialRDD, path: String, opts: BeastOptions): Unit =
    saveIndex2(partitionFeatures.rdd, path, opts)

  /**
   * Saves a partitioned RDD to disk. Each partition is stored to a separate file and an additional master file
   * that stores the partition information.
   * @param partitionFeatures a set of partitioned features
   * @param path the path to store the files
   * @param opts additional options for storing the data
   */
  def saveIndex2(partitionFeatures: SpatialRDD, path: String, opts: BeastOptions): Unit = {
    require(partitionFeatures.partitioner.isDefined, "Input should be partitioned")
    require(partitionFeatures.partitioner.get.isInstanceOf[SpatialPartitioner],
      "Input should be spatially partitioned")
    val out: Path = new Path(path)
    val filesystem: FileSystem = out.getFileSystem(opts.loadIntoHadoopConf())
    if (opts.getBoolean(SpatialWriter.OverwriteOutput, false)) {
      if (filesystem.exists(out))
        filesystem.delete(out, true)
    }
    val partitioner = partitionFeatures.partitioner.get.asInstanceOf[SpatialPartitioner]
    val writerClass: Class[_ <: FeatureWriter] =
      SpatialWriter.getConfiguredFeatureWriterClass(opts.loadIntoHadoopConf())
    // Run a job that writes each partition to a separate file and returns its metadata
    val partitionInfo: Array[(Int, String, Summary)] = partitionFeatures.sparkContext.runJob(partitionFeatures,
      (context, features: Iterator[IFeature]) => {
        if (features.hasNext) {
          // Get writer metadata to determine the extension of the output file
          val metadata: FeatureWriter.Metadata = writerClass.getAnnotation(classOf[FeatureWriter.Metadata])
          // Create a temporary directory for this task output
          val tempDir: Path = new Path(new Path(path), f"temp-${context.taskAttemptId()}")
          val fileSystem = tempDir.getFileSystem(opts.loadIntoHadoopConf())
          context.addTaskFailureListener(new TaskFailureListener() {
            override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
              if (fileSystem.exists(tempDir))
                fileSystem.delete(tempDir, true)
            }
          })
          fileSystem.mkdirs(tempDir)
          val partitionId: Int = context.partitionId()
          // The minimum bounding box of the partition based on the partitioner
          val partitionMBB: EnvelopeNDLite = partitioner.getPartitionMBR(partitionId)
          // The minimum bounding box of all the data stored in this partition
          // Initialize the feature writer
          val partitionFileName: String = f"part-${partitionId}%05d${metadata.extension()}"
          val partitionFile: Path = new Path(tempDir, partitionFileName)
          val featureWriter = writerClass.newInstance()
          featureWriter.initialize(partitionFile, opts.loadIntoHadoopConf())
          val summary = new Summary
          for (feature <- features) {
            summary.incrementNumFeatures(1)
            summary.merge(feature.getGeometry)
            featureWriter.write(feature)
          }
          featureWriter.close()
          if (partitioner.isDisjoint)
            summary.shrink(partitionMBB)
          // Get file size
          summary.size = fileSystem.getFileStatus(partitionFile).getLen
          (partitionId, partitionFile.toString, summary)
        } else {
          null
        }
      }
    )
    // Move all files to the output directory and write the master file
    val masterFilePath = new Path(out, "_master."+partitioner.getMetadata.extension())
    val masterFileOut = new PrintStream(filesystem.create(masterFilePath))
    printMasterFileHeader(partitioner.getCoordinateDimension, masterFileOut)
    for ((partitionId, filename, summary) <- partitionInfo.filter(_ != null)) {
      masterFileOut.println
      val partitionPath = new Path(filename)
      // Move the file to the output directory
      filesystem.rename(partitionPath, new Path(out, partitionPath.getName))
      masterFileOut.print(getPartitionAsText(partitionId, partitionPath.getName, summary))
    }
    masterFileOut.close()
    // Clean up temporary output directory
    filesystem.listStatus(out, new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith("temp-")
    }).foreach(f => filesystem.delete(f.getPath, true))
  }

  /**
   * Convert a partition to text in a format that will appear in the master file
   *
   * @param id        the ID of the partition
   * @param filename  the name of the file
   * @param partition other partition information
   * @return the created text
   */
  def getPartitionAsText(id: Int, filename: String, partition: Summary): java.lang.StringBuilder = {
    val partitionText = new java.lang.StringBuilder
    partitionText.append(id)
    partitionText.append('\t')
    partitionText.append(filename)
    partitionText.append('\t')
    partitionText.append(partition.numFeatures)
    partitionText.append('\t')
    partitionText.append(partition.numNonEmptyGeometries)
    partitionText.append('\t')
    partitionText.append(partition.numPoints)
    partitionText.append('\t')
    partitionText.append(partition.size)
    for (d <- 0 until partition.getCoordinateDimension) {
      partitionText.append('\t')
      partitionText.append(partition.sumSideLength(d))
    }
    partitionText.append('\t')
    if (partition.getCoordinateDimension == 2) partition.toWKT(partitionText)
    partitionText.append('\t')
    for (d <- 0 until partition.getCoordinateDimension) {
      partitionText.append(partition.getMinCoord(d))
      partitionText.append('\t')
    }
    for (d <- 0 until partition.getCoordinateDimension) { // Avoid appending a tab separator after the last coordinate
      if (d != 0) partitionText.append('\t')
      partitionText.append(partition.getMaxCoord(d))
    }
    partitionText
  }

  /**
   * Writes the header of the master file
   *
   * @param numDimensions number of dimensions
   * @param out           the print stream to write to
   */
  def printMasterFileHeader(numDimensions: Int, out: PrintStream): Unit = {
    out.print("ID")
    out.print('\t')
    out.print("File Name")
    out.print('\t')
    out.print("Record Count")
    out.print('\t')
    out.print("NonEmpty Count")
    out.print('\t')
    out.print("NumPoints")
    out.print('\t')
    out.print("Data Size")
    out.print('\t')
    val numLetters = GeometryHelper.DimensionNames.length
    for (d <- 0 until numDimensions) {
      out.print("Sum_")
      if (d < numLetters) out.print(GeometryHelper.DimensionNames(d))
      else out.print(GeometryHelper.DimensionNames(d / numLetters - 1) + "" + GeometryHelper.DimensionNames(d % numLetters))
      out.print('\t')
    }
    out.print("Geometry")
    for (d <- 0 until numDimensions) {
      out.print('\t')
      if (d < numLetters) out.print(GeometryHelper.DimensionNames(d))
      else out.print(GeometryHelper.DimensionNames(d / numLetters - 1) + "" + GeometryHelper.DimensionNames(d % numLetters))
      out.print("min")
    }
    for (d <- 0 until numDimensions) {
      out.print('\t')
      if (d < numLetters) out.print(GeometryHelper.DimensionNames(d))
      else out.print(GeometryHelper.DimensionNames(d / numLetters - 1) + "" + GeometryHelper.DimensionNames(d % numLetters))
      out.print("max")
    }
  }

}
