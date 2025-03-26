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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SpatialPartition
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, GeometryHelper, GeometryType, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.CellPartitioner
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.SpatialFilePartition
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.FilePartition
import edu.ucr.cs.bdlab.beast.util.{IConfigurable, OperationParam}
import org.apache.commons.io.IOCase
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.LineReader
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A SpatialRDD that is backed by a file. Each partition points to part of a file and contains spatial information
 * about that file if a master file exists.
 *
 * Note: If the options conflict, they are used in this order:
 *  - The passed BeastOptions (opts)
 *  - The Spark configuration in the passed SparkContext
 *  - The Hadoop Configuration in the passed SparkContext
 */
class SpatialFileRDD(@transient sc: SparkContext, path: String, var opts: BeastOptions = new BeastOptions())
  extends SpatialRDD(sc, Seq()) with Logging {

  opts = new BeastOptions(sc.hadoopConfiguration).mergeWith(sc.getConf).mergeWith(opts)

  /**The feature reader class that will be used to read this file*/
  lazy val featureReaderClass: Class[_ <: FeatureReader] = SpatialFileRDD.getFeatureReaderClass(path, opts)

  /**Metadata of the feature reader*/
  lazy val readerMetadata: SpatialReaderMetadata =
    featureReaderClass.getAnnotation[SpatialReaderMetadata](classOf[SpatialReaderMetadata])

  /**
   * Whether to apply duplicate avoidance or not. By default, duplicate avoidance is applied to avoid reading
   * duplicate records. However, some specialized operations, e.g., spatial join, might skip this step since
   * the spatial join algorithm uses this replication and performs its own duplicate avoidance.
   */
  private var applyDuplicateAvoidance: Boolean = true

  /**
   * This method is called only once. It uses the input file path to enumerate all partitions.
   * @return
   */
  override def getPartitions: Array[Partition] =
    SpatialFileRDD.createPartitions(path, opts, sc.hadoopConfiguration).asInstanceOf[Array[Partition]]

  /**
   * Define the partitioner of this RDD. If this RDD is defined based on a master file that defines the spatial extents
   * of each partition, this partitioner will be defined accordingly. Otherwise, no partitioner is defined.
   */
  @transient override val partitioner: Option[Partitioner] = {
    val allPartitioned = partitions.forall(p => p.isInstanceOf[SpatialFilePartition])
    if (allPartitioned && partitions.nonEmpty)
      Some(new CellPartitioner(partitions.map(_.asInstanceOf[SpatialFilePartition].mbr):_*))
    else None
  }

  override def compute(split: Partition, context: TaskContext): Iterator[IFeature] =
    SpatialFileRDD.readPartition(split.asInstanceOf[FilePartition], featureReaderClass,
      applyDuplicateAvoidance, opts)

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[FilePartition].locations
}

object SpatialFileRDD extends IConfigurable with Logging {

  /**
   * A partition that points to a file
   * @param index the index of the partition
   * @param path the path to the partition
   * @param offset the start offset of the partition within the file
   * @param length the length of the partition in bytes
   * @param locations the hosts that contain the partition
   */
  class FilePartition(override val index: Int, val path: String, val offset: Long, val length: Long,
                      val locations: Array[String])
    extends Partition {

    override def equals(other: Any): Boolean = {
      other match {
        case p: FilePartition =>
          this.path.equals(p.path) && this.offset == p.offset && this.length == p.length
        case _ => false
      }
    }
  }

  /**
   * A spatial partition points to part of a file and might contain spatial information about it
   * @param index the index of the partition within its parent RDD
   * @param path the path to the file
   * @param offset the start offset within the file
   * @param length number of bytes in the file
   * @param locations the machines that contain this part of the file
   * @param partitionInfo [[IFeature]] that contains the partition information where the geometry is the MBR
   */
  class SpatialFilePartition(override val index: Int, path: String, offset: Long, length: Long,
                             locations: Array[String], @transient partitionInfo: IFeature)
    extends FilePartition(index, path, offset, length, locations) with SpatialPartition {
    override val mbr: EnvelopeNDLite = new EnvelopeNDLite().merge(partitionInfo.getGeometry)
    override def size: Long = length
    override val numFeatures: Long = rowGetOrElse(partitionInfo, "Record Count", "-1").toLong
    override val numNonEmptyGeometries: Long = rowGetOrElse(partitionInfo, "NonEmpty Count", "-1").toLong
    override val numPoints: Long = rowGetOrElse(partitionInfo, "NumPoints", "-1").toLong
    override val sumSideLength: Array[Double] = new Array[Double](mbr.getCoordinateDimension)
    for (i <- sumSideLength.indices)
      sumSideLength(i) = rowGetOrElse(partitionInfo, "sum"+GeometryHelper.DimensionNames(i), "NaN").toDouble
    override val geometryType: GeometryType =
      rowGetOrElse(partitionInfo, "GeometryType", GeometryType.GeometryCollectionName) match {
      case GeometryType.EmptyName => GeometryType.EMPTY
      case GeometryType.PointName => GeometryType.POINT
      case GeometryType.LineStringName => GeometryType.LINESTRING
      case GeometryType.PolygonName => GeometryType.POLYGON
      case GeometryType.MultiPointName => GeometryType.MULTIPOINT
      case GeometryType.MultiLineStringName => GeometryType.MULTILINESTRING
      case GeometryType.MultiPolygonName => GeometryType.MULTIPOLYGON
      case GeometryType.GeometryCollectionName => GeometryType.GEOMETRYCOLLECTION
    }

    override def toString: String = s"SpatialFilePartition #$index: $path [$offset+$length]"
  }

  def rowGetOrElse[T](row: Row, attName: String, defaultValue: T): T =
    rowGetOrElse(row, attName, () => defaultValue)

  def rowGetOrElse[T](row: Row, attName: String, getter: () => T): T = {
    if (row.schema.fieldNames == null || !row.schema.fieldNames.contains(attName))
      return getter.apply()
    row.getAs[T](attName)
  }

  /** A user-friendly input format string */
  @OperationParam(description =
  """The format of the input file {point(xcol,ycol),envelope(x1col,y1col,x2col,y2col),wkt(gcol)}
    |	point(xcol,ycol) indicates a CSV input where xcol and ycol indicate the indexes of the columns that contain the x and y coordinates
    |	envelope(x1col,y1col,x2col,y2col) indicate an input that contains rectangles stored in (x1,y1,x2,y2) format
    |	wkt(gcol) indicate a CSV file with the field (gcol) containing a WKT-encoded geometry.
    |	shapefile: Esri shapefile. Accepts both .shp+.shx+.dbf files or a compressed .zip file with these three files
    |	rtree: An optimized R-tree index
    |	geojson: GeoJSON file containing features with geometries and properties (attributes)"""
    , required = false) val InputFormat = "iformat"

  /** Early filter the input based on this MBR */
  @OperationParam(description = "An optional MBR to filter the input. Format: x1,y1,x2,y2") val FilterMBR = "filtermbr"

  /** Process the input recursively */
  @OperationParam(description = "Process the input recursively", defaultValue = "false") val Recursive = "recursive"

  /** Whether to split input files or not */
  @OperationParam(description = "Split big files", defaultValue = "true", showInUsage = false)
  val SplitFiles = "SpatialInputFormat.SplitFiles"

  /** Minimum split size */
  @OperationParam(description = "Minimum split size", showInUsage = false) val MinSplitSize: String =
  FileInputFormat.SPLIT_MINSIZE

  /** Maximum split size*/
  @OperationParam(description = "Maximum split size", showInUsage = false) val MaxSplitSize: String =
  FileInputFormat.SPLIT_MAXSIZE

  /** A filter that prunes hidden files (files that start with _ or .) */
  val HiddenFileFilter: PathFilter = (p: Path) => p.getName.charAt(0) != '_' && p.getName.charAt(0) != '.'

  /** A filter that finds master files only */
  val MasterFileFilter: PathFilter = (p: Path) => p.getName.startsWith("_master")

  /**
   * Add the feature reader class that might have additional parameters
   * @param opts the user-provided options for this class.
   * @param parameterClasses
   */
  override def addDependentClasses(opts: BeastOptions, parameterClasses: util.Stack[Class[_]]): Unit = {
    super.addDependentClasses(opts, parameterClasses)
    if (opts == null) return
    val featureReaderClass = getFeatureReaderClass(null, opts)
    if (featureReaderClass != null)
      parameterClasses.push(featureReaderClass)
  }

  /**
   * Tries to auto-detect the input format from the given path. If an input could be detected, the corresponding
   * parameters for that input are set in BeastOptions and returned. If the input format could not be detected,
   * a `null` is returned. The given set of options are assumed to be correct (given by user) and this function
   * does not try to override them. Any options that is not present in the given options can be overriden by the
   * auto-detect function.
   * @param path the path to either a file or a directory
   * @param opts the options set by user.
   * @return the detected feature reader class and the options that can be used to set that reader or `null` if
   *         the input could not be detected
   */
  def autodetectInputFormat(path: String, opts: BeastOptions): (Class[_ <: FeatureReader], BeastOptions) = {
    // Iterate over all input formats and run the auto-detect function for each of them
    val t1: Long = System.nanoTime()
    val allReaders: Iterator[Class[_ <: FeatureReader]] = FeatureReader.getFeatureReaders().values().iterator().asScala
    var detectedOpts: BeastOptions = null
    var readerClass: Class[_ <: FeatureReader] = null
    while (detectedOpts == null && allReaders.hasNext) {
      readerClass = allReaders.next()
      val reader: FeatureReader = readerClass.getConstructor().newInstance()
      detectedOpts = reader.autoDetect(opts, path)
    }
    val t2: Long = System.nanoTime()
    logWarning(s"Detection took ${(t2-t1)*1E-9} seconds")
    if (detectedOpts != null) {
      // Print out the added parameters
      logWarning("Input format detected. In the future, please add these parameters to your command line to avoid " +
        "running this costly step. " + detectedOpts.toString)
    }
    if (detectedOpts == null)
      null
    else
      (readerClass, detectedOpts)
  }

  /**
   * Returns the path of the master file within the given index if exists.
   * If more than one master file exists, the most recent one is returned.
   *
   * @param fileSystem the file system that contains the index
   * @param indexPath  the path to the index
   * @return the path to the master file if exists. {@code null} otherwise.
   * @throws IOException if an error happens while reading the master file
   */
  def getMasterFilePath(fileSystem: FileSystem, indexPath: Path): Path = {
    val masterFiles = fileSystem.listStatus(indexPath, MasterFileFilter)
    if (masterFiles.isEmpty) return null
    var iMostRecent = 0
    for ($i <- 1 until masterFiles.length) {
      if (masterFiles($i).getModificationTime > masterFiles(iMostRecent).getModificationTime)
        iMostRecent = $i
    }
    masterFiles(iMostRecent).getPath
  }


  /**
   * Adds a path to the list of splits by creating the appropriate file splits. If the file is splittable,
   * multiple splits might be added.
   *
   * @param fileSystem  the file system that contains the input
   * @param fileStatus  the status of the input file
   * @param start       the start offset in the file
   * @param length      the length of the part of the file to add
   * @param noSplit     a boolean flag that is set to avoid splitting files
   * @param partitionInfo information about the spatial attributes of the partition or `null` if unknown
   * @param _partitions (output) the created partitions are added to this list
   */
  private def addPartitions(opts: BeastOptions, fileSystem: FileSystem, fileStatus: FileStatus,
                            start: Long, length: Long, noSplit: Boolean,
                            partitionInfo: IFeature,
                            _partitions: mutable.ArrayBuffer[FilePartition]): Unit = {
    val codec: CompressionCodec = new CompressionCodecFactory(fileSystem.getConf).getCodec(fileStatus.getPath)
    val splitSize = if (noSplit || (codec != null && !codec.isInstanceOf[SplittableCompressionCodec])) length else {
      val minSize = opts.getLong(MinSplitSize, 1)
      val maxSize = opts.getLong(MaxSplitSize, Long.MaxValue)
      Math.max(minSize, Math.min(maxSize, fileStatus.getBlockSize))
    }
    val blkLocations = fileSystem.getFileBlockLocations(fileStatus, start, length)
    var partitionStart = start
    val fileEnd = start + length
    val SPLIT_SLOP = 1.1 // 10% slop
    while (partitionStart < fileEnd) {
      val blkIndex = blkLocations.find(bl => partitionStart >= bl.getOffset && partitionStart < bl.getOffset + bl.getLength)
      assert(blkIndex.isDefined, s"No locations found for block at offset $partitionStart")
      val partitionEnd = if ((fileEnd - partitionStart).toDouble / splitSize > SPLIT_SLOP)
        partitionStart + splitSize
      else
        fileEnd
      _partitions.append(
        if (partitionInfo != null)
          new SpatialFilePartition(_partitions.length, fileStatus.getPath.toString, partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts, partitionInfo)
        else
          new FilePartition(_partitions.length, fileStatus.getPath.toString, partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts)
      )
      partitionStart = partitionEnd
    }
  }

  /**
   * Create all partitions in this RDD for the given input file
   * @return
   */
  def createPartitions(path: String, opts: BeastOptions, conf: Configuration): Array[FilePartition] = {
    val inputPaths: Array[String] = path.split(",")
    val pathsToInspect = mutable.ArrayBuffer[String](inputPaths:_*)
    val recursive: Boolean = opts.getBoolean(Recursive, false)
    val hadoopConf: Configuration = opts.loadIntoHadoopConf(new Configuration(conf))
    val featureReaderClass: Class[_ <: FeatureReader] = getFeatureReaderClass(inputPaths(0), opts)
    val readerMetadata: SpatialReaderMetadata = featureReaderClass.getAnnotation(classOf[SpatialReaderMetadata])
    val splitFiles: Boolean = opts.getBoolean(SpatialFileRDD.SplitFiles, true) &&
      !readerMetadata.noSplit()
    var fileFilters: Seq[PathFilter] = Seq(SpatialFileRDD.HiddenFileFilter)
    val wildcard = readerMetadata.filter()
    if (wildcard != null && wildcard.nonEmpty) {
      val wildcardFilter = new WildcardFileFilter(wildcard.split("\n"), IOCase.SYSTEM)
      val wildcardPathFilter: PathFilter = p => wildcardFilter.accept(new File(p.getName))
      fileFilters = fileFilters :+ wildcardPathFilter
    }
    // Merge all filters together
    val fileFilter: PathFilter = if (fileFilters.length == 1)
      fileFilters.head
    else // More than one filter, combine them with &&
      p => fileFilters.forall(filter => filter.accept(p))

    val _partitions = mutable.ArrayBuffer[FilePartition]()

    while (pathsToInspect.nonEmpty) {
      val pathToInspect: Path = new Path(pathsToInspect.remove(pathsToInspect.length - 1))
      val fs: FileSystem = pathToInspect.getFileSystem(hadoopConf)
      val fileStatus: FileStatus = fs.getFileStatus(pathToInspect)
      // Check if the file matches the filter. Special case, the given input path should be added without testing
      val pathGivenByUser = inputPaths.exists(p => new Path(p).equals(pathToInspect))
      if (fileStatus.isFile && (fileFilter.accept(fileStatus.getPath) || pathGivenByUser)) {
        // A file that matches the filter, process it
        SpatialFileRDD.addPartitions(opts, fs, fileStatus, 0, fileStatus.getLen, !splitFiles, null, _partitions)
      } else if (fileStatus.isDirectory && (recursive ||  pathGivenByUser)) {
        // A directory and the recursive option is enabled, add all its contents
        val masterFilePath = getMasterFilePath(fs, fileStatus.getPath)
        if (masterFilePath != null) {
          // If a master file exists, use the master file to list the files
          // 1 - To determine the number of dimensions, we read the first line
          val lineReader = new LineReader(fs.open(masterFilePath))
          val headerLine = new Text()
          lineReader.readLine(headerLine)
          lineReader.close()
          val headerParts: Array[String] = headerLine.toString.split("\t")
          val numDimensions: Int = (headerParts.length - headerParts.indexOf("xmin")) / 2
          // Reader the master file
          val masterFileReader = new CSVFeatureReader
          val masterFileOpts = new BeastOptions(opts)
            .set(CSVFeatureReader.SkipHeader, true)
            .set(SpatialFileRDD.InputFormat, s"envelopek($numDimensions,xmin)")
            .set(CSVFeatureReader.FieldSeparator, "\t")
            .set(CSVFeatureReader.QuoteCharacters, "\'\'\"\"")
          masterFileReader.initialize(masterFilePath, masterFileOpts)
          try {
            for (spatialPartition <- masterFileReader.iterator().asScala) {
              val partitionPath = new Path(fileStatus.getPath, spatialPartition.getAs("File Name").toString)
              if (fileFilter.accept(partitionPath)) {
                val start: Long = 0
                val length: Long = spatialPartition.getAs("Data Size").toString.toLong
                SpatialFileRDD.addPartitions(opts, fs, fs.getFileStatus(partitionPath), start, length, !splitFiles,
                  spatialPartition, _partitions)
              }
            }
          } finally {
            masterFileReader.close()
          }
        } else {
          // No master files, list all directory contents
          for (subfile <- fs.listStatus(fileStatus.getPath))
            pathsToInspect.append(subfile.getPath.toString)
        }
      }
    }
    _partitions.toArray
  }


  /**
   * The class of the feature reader to use with this RDD. All partitions use the same feature reader.
   */
  def getFeatureReaderClass(path: String, opts: BeastOptions): Class[_ <: FeatureReader] = {
    val iformat: String = opts.getString(SpatialFileRDD.InputFormat)
    var ifClass: Class[_ <: FeatureReader] = null
    if (iformat == null || iformat.equals("*auto*")) {
      logDebug(s"Auto detecting input format for '$path'")
      if (path != null) {
        // Auto detect the input
        val detected = SpatialFileRDD.autodetectInputFormat(path, opts)
        require(detected != null, s"Input format could not be detected for input '$path'")
        ifClass = detected._1
        opts.mergeWith(detected._2)
      }
    } else {
      // Iterate over all input formats and test if the given user-friendly format is supported
      var allReaders: Iterator[Class[_ <: FeatureReader]] = FeatureReader.getFeatureReaders.values().iterator().asScala
      while (ifClass == null && allReaders.hasNext) {
        val readerClass = allReaders.next()
        if (readerClass.getConstructor().newInstance().isRecognized(iformat))
          ifClass = readerClass
      }
      if (ifClass == null) {
        // Input format not recognized. Try to suggest some corrections
        allReaders = FeatureReader.getFeatureReaders.values().iterator().asScala
        var corrections: Array[String] = Array()
        for (readerClass <- allReaders) {
          val c: Array[String] = readerClass.newInstance().iformatCorrections(iformat)
          if (c != null)
            corrections = corrections ++ c
        }
        throw new RuntimeException(s"Input format '$iformat' cannot be recognized. " +
          s"Perhaps you mean one of {${corrections.mkString(",")}}")
      }
    }
    ifClass
  }

  /**
   * Reads the given partition
   * @param partition the partition to read
   * @param featureReaderClass the class of the feature reader
   * @param opts the user options
   * @return an iterator to the features
   */
  def readPartition(partition: FilePartition, featureReaderClass: Class[_ <: FeatureReader],
                    applyDuplicateAvoidance: Boolean, opts: BeastOptions): Iterator[IFeature] = {
    val featureReader = featureReaderClass.getConstructor().newInstance()
    featureReader.initialize(partition, opts)
    logInfo(s"Processing partition ${partition.path} [${partition.offset},${partition.offset+partition.length})")
    val features = featureReader.iterator().asScala
    if (!partition.isInstanceOf[SpatialFilePartition] ||
      partition.asInstanceOf[SpatialFilePartition].mbr == null ||
      !applyDuplicateAvoidance) {
      // No duplicate avoidance is needed
      features
    } else {
      // Apply duplicate avoidance
      val mbr = partition.asInstanceOf[SpatialFilePartition].mbr
      features.filter(f => {
        val recordMBR: EnvelopeND = new EnvelopeND(f.getGeometry.getFactory).merge(f.getGeometry)
        var refPointInMBR = true
        var d = 0
        while (d < recordMBR.getCoordinateDimension && refPointInMBR) {
          refPointInMBR = recordMBR.getMinCoord(d) >= mbr.getMinCoord(d) &&
            recordMBR.getMinCoord(d) < mbr.getMaxCoord(d)
          if (!refPointInMBR && recordMBR.getSideLength(d) == 0) {
            // Check a special case for points that are on the higher edge of the partition
            refPointInMBR = recordMBR.getMinCoord(d) == mbr.getMaxCoord(d)
          }
          d += 1
        }
        refPointInMBR
      })
    }
  }

  /**
   * Java shortcut to read the features as a Java iterator
   * @param partition
   * @param featureReaderClass
   * @param opts
   * @return
   */
  def readPartitionJ(partition: FilePartition, featureReaderClass: Class[_ <: FeatureReader],
                     opts: BeastOptions): java.util.Iterator[IFeature] =
    readPartition(partition, featureReaderClass, true, opts).asJava

  /**
   * Reads the given path locally without creating any RDDs. Useful for reading a small file
   * when SparkContext is not accessible, e.g., inside a mapPartition function.
   * @param path path to a single file or a directory
   * @param iformat the format of the data
   * @param opts additional options for reading the file
   * @return an iterator to features in the given path
   */
  def readLocal(path: String, iformat: String, opts: BeastOptions, conf: Configuration): Iterator[IFeature] = {
    val beastOpts =
      if (opts != null) new BeastOptions(opts).set(SpatialFileRDD.InputFormat, iformat)
      else new BeastOptions(SpatialFileRDD.InputFormat -> iformat)
    val readerClass = getFeatureReaderClass(path, beastOpts)
    val partitions = createPartitions(path, beastOpts, conf)
    new Iterator[IFeature] {
      var currentPartition: Int = 0

      var currentIterator: Iterator[IFeature] = SpatialFileRDD.readPartition(partitions(currentPartition), readerClass,
        true, beastOpts)

      override def hasNext: Boolean = currentIterator.hasNext || currentPartition < partitions.length - 1

      override def next(): IFeature = {
        if (!currentIterator.hasNext) {
          if (currentPartition < partitions.length - 1) {
            currentPartition += 1
            currentIterator = SpatialFileRDD.readPartition(partitions(currentPartition), readerClass, true, beastOpts)
          }
        }
        assert(currentIterator.hasNext)
        currentIterator.next()
      }
    }
  }

  /**
   * (Java version) Reads the given path locally without creating any RDDs. Useful for reading a small file
   * when SparkContext is not accessible, e.g., inside a mapPartition function.
   * @param path path to a single file or a directory
   * @param format the format of the data
   * @param opts additional options for reading the file
   * @param conf configuration used to create the file system to read the input
   * @return an iterator to features in the given path
   */
  def readLocalJ(path: String, format: String, opts: BeastOptions, conf: Configuration): java.util.Iterator[IFeature] =
    readLocal(path, format, opts, conf).asJava

  /**
   * If the given RDD is based on a SpatialFileRDD, it causes it to skip duplicate avoidance.
   * @param rdd the rdd to skip duplicate avoidance for
   */
  private[beast] def skipDuplicateAvoidance(rdd: RDD[_]): Unit = {
    val rddsToCheck = new mutable.ArrayBuffer[RDD[_]]()
    rddsToCheck.append(rdd)
    while (rddsToCheck.nonEmpty) {
      val rddToCheck: RDD[_] = rddsToCheck.remove(rddsToCheck.length - 1)
      if (rddToCheck.isInstanceOf[SpatialFileRDD]) {
        rddToCheck.asInstanceOf[SpatialFileRDD].applyDuplicateAvoidance = false
      } else {
        // Check all its dependencies
        for (dependency <- rddToCheck.dependencies) {
          rddsToCheck.append(dependency.rdd)
        }
      }
    }
  }

}