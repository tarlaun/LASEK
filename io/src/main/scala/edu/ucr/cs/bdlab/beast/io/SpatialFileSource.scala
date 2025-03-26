package edu.ucr.cs.bdlab.beast.io

import com.fasterxml.jackson.databind.ObjectMapper
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import org.apache.commons.io.IOCase
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.io.File
import java.util
import scala.reflect.ClassTag

/**
 * A generic SparkSQL data source for spatial files. That is, any class that extends [[FeatureReader]].
 * The class can still be extended to provide any additional tweaks, such as, projection push down, if supported.
 */
abstract class SpatialFileSource[FR <: FeatureReader](implicit t: ClassTag[FR])
  extends TableProvider with DataSourceRegister {

  // TODO we need to implement a fall back to V1 source with FileFormat due to a problem with Spark

  /**
   * Returns the schema of the file by opening the first split and reading one feature from it.
   * Subclasses might be able to provide a better implementation that does not require parsing the first record.
   * @param options  an immutable case-insensitive string-to-string map that can identify a table,
   *                                  e.g. file path, Kafka topic name, etc
   * @return the schema of the input file
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val inputPaths = SpatialFileSourceHelper.getSpatialFilePaths[FR](options).map(new Path(_))
    lazy val reader = t.runtimeClass.getConstructor().newInstance().asInstanceOf[FeatureReader]
    for (inputPath <- inputPaths) {
      try {
        val fileSystem = inputPath.getFileSystem(SpatialFileSourceHelper.hadoopConf)
        val length = fileSystem.getFileStatus(inputPath).getLen
        val filesplit = new FileSplit(inputPath, 0, length, Array())
        reader.initialize(filesplit, BeastOptions.fromMap(options))
        if (reader.hasNext) {
          return reader.next().schema
        }
      } finally {
        reader.close()
      }
    }
    StructType(Seq())
  }

  override def getTable(schema: StructType, transforms: Array[Transform], options: util.Map[String, String]): Table =
    new SpatialFileTable[FR](schema, options)

  override def supportsExternalMetadata(): Boolean = true
}

/**
 * A generic SparkSQL data source and sink for spatial files. It works on a pair of classes that extend
 * [[FeatureReader]] and [[FeatureWriter]]
 * The class can still be extended to provide any additional tweaks, such as, projection push down, if supported.
 */
abstract class SpatialFileSourceSink[FR <: FeatureReader, FW <: FeatureWriter]
(implicit t: ClassTag[FR], t2: ClassTag[FW]) extends SpatialFileSource[FR] {

  override def getTable(schema: StructType, transforms: Array[Transform], options: util.Map[String, String]): Table = {
    new SpatialFileTableRW[FR, FW](schema, options)
  }

}

/**
 * A class that represents a table of spatial data.
 * @param schema the schema of the table including both spatial and non-spatial attributes
 * @param options additional user options
 * @param t the type of the feature reader
 * @tparam FR the class of the feature reader
 */
class SpatialFileTable[FR <: FeatureReader](override val schema: StructType, options: util.Map[String, String])
                                           (implicit t: ClassTag[FR]) extends SupportsRead {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SpatialFileScanBuilder(schema, options)

  override def name(): String = {
    val shortname: String = t.runtimeClass.getAnnotation(classOf[SpatialReaderMetadata]).shortName()
    val filename: String = SpatialFileSourceHelper.getPaths(options).head
    s"${shortname}:${filename}"
  }

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = new util.HashSet[TableCapability]()
    capabilities.add(TableCapability.BATCH_READ)
    capabilities
  }
}

class SpatialFileTableRW[FR <: FeatureReader, FW <: FeatureWriter]
    (override val schema: StructType, options: util.Map[String, String])(implicit t: ClassTag[FR], t2: ClassTag[FW])
  extends SpatialFileTable[FR](schema, options) with SupportsWrite {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new SpatialWriteBuilder(info)

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = super.capabilities()
    capabilities.add(TableCapability.BATCH_WRITE)
    capabilities.add(TableCapability.TRUNCATE)
    capabilities
  }
}

object SpatialFileSourceHelper {

  /**The minimum size of a split*/
  val MinSplitSize: String = FileInputFormat.SPLIT_MINSIZE

  /**The maximum size of a split*/
  val MaxSplitSize: String = FileInputFormat.SPLIT_MAXSIZE

  /**A path filter that skips hidden files which begin with "." or "_"*/
  val HiddenFileFilter: PathFilter = (p: Path) => p.getName.charAt(0) != '_' && p.getName.charAt(0) != '.'

  /**A path filter that only selects master files*/
  val MasterFileFilter: PathFilter = (p: Path) => p.getName.startsWith("_master")

  /**The active Spark session*/
  lazy val sparkSession = SparkSession.active

  /**The Hadoop configuration is used to initialize [[org.apache.hadoop.fs.FileSystem]] instances for HDFS */
  lazy val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  /**
   * Get a sequence of all input paths specified by the user.
   * @param options the user-provided options
   * @return a sequence of all input paths as strings
   */
  def getPaths(options: util.Map[String, String]): Seq[String] = {
    val objectMapper = new ObjectMapper()
    val paths = Option(options.get("paths")).map { pathStr =>
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    }.getOrElse(Seq.empty)
    paths ++ Option(options.get("path")).toSeq
  }

  /**
   * Get a list of all files that can be read in the input. This includes all .shp and all .zip files.
   * @param options the options that the user specified including the input
   */
  def getSpatialFilePaths[FR <: FeatureReader](options: util.Map[String, String])(implicit t: ClassTag[FR]): Seq[String] = {
    val paths = getPaths(options)
    val metadata: SpatialReaderMetadata = t.runtimeClass.getAnnotation(classOf[SpatialReaderMetadata])
    def filematch: PathFilter = if (metadata.filter() == null || metadata.filter().isEmpty) {
      _: Path => true
    } else {
      val wildcard = metadata.filter()
      val wildcardFilter = new WildcardFileFilter(wildcard.split("\n"), IOCase.SYSTEM)
      path: Path => wildcardFilter.accept(new File(path.getName))
    }
    paths.flatMap(in => {
      val path = new Path(in)
      val filesystem = path.getFileSystem(hadoopConf)
      if (filesystem.isFile(path) && filematch.accept(path))
        Option(in).iterator
      else if (filesystem.isDirectory(path))
        filesystem.listStatus(path, filematch).map(_.getPath.toString)
      else
        Option.empty[String].iterator
    })
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
  def addPartitions(fileSystem: FileSystem, fileStatus: FileStatus,
                            start: Long, length: Long, noSplit: Boolean,
                            partitionInfo: IFeature, options: util.Map[String, String],
                           _partitions: collection.mutable.ArrayBuffer[InputPartition]): Unit = {
    val splitSize = if (noSplit) length else {
      val minSize = options.getOrDefault(MinSplitSize, "1").toLong
      val maxSize = options.getOrDefault(MaxSplitSize, Long.MaxValue.toString).toLong
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
          new SpatialDataframePartition(_partitions.length, Array(PartitionedFile(null, SparkPath.fromFileStatus(fileStatus), partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts)), partitionInfo)
        else
          new FilePartition(_partitions.length, Array(PartitionedFile(null, SparkPath.fromFileStatus(fileStatus), partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts)))
      )
      partitionStart = partitionEnd
    }
  }

}