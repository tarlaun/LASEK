package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * A builder for scanning spatial files
 *
 * @param schema the schema of the file
 * @param options additional user options
 * @param t
 * @tparam FR
 */
class SpatialFileScanBuilder[FR <: FeatureReader](val schema: StructType, val options: util.Map[String, String])
                                                 (implicit t: ClassTag[FR])
  extends ScanBuilder /*with SupportsPushDownFilters with SupportsPushDownRequiredColumns*/ {
  override def build(): Scan = {
    new SpatialFileScan[FR](schema, options)
  }
}

/**
 * A class that reads the schema and the data of a spatial file
 * @param schema
 * @param options
 * @param t
 * @tparam FR
 */
class SpatialFileScan[FR <: FeatureReader](val schema: StructType, val options: util.Map[String, String])
                                          (implicit t: ClassTag[FR])extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SpatialFileBatch[FR](schema, options)
}

/**
 * A class for batch reading a spatial file
 * @param schema
 * @param options
 * @param t
 * @tparam FR
 */
class SpatialFileBatch[FR <: FeatureReader](val schema: StructType, val options: util.Map[String, String])
                                           (implicit t: ClassTag[FR]) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    val SpatialFiles: Seq[String] = SpatialFileSourceHelper.getSpatialFilePaths[FR](options)
    val noSplit = t.runtimeClass.getAnnotation(classOf[SpatialReaderMetadata]).noSplit()
    val partitions = new ArrayBuffer[InputPartition]()
    SpatialFiles.foreach(SpatialFile => {
      val path = new Path(SpatialFile)
      val filesystem = path.getFileSystem(SpatialFileSourceHelper.hadoopConf)
      val filestatus = filesystem.getFileStatus(path)
      SpatialFileSourceHelper.addPartitions(filesystem, filestatus, 0, filestatus.getLen, noSplit,
        null, options, partitions)
    })
    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new SpatialFilePartitionReaderFactory[FR](schema, options)
}

/**
 * A factory method for creating readers for each partition in the input file.
 * Note: This class must be serializable because it is created at the head node and serialized to worker nodes.
 * @param schema the schema of the input file
 * @param options additional user options.
 */
class SpatialFilePartitionReaderFactory[FR <: FeatureReader](val schema: StructType,
                                                             @transient val options: util.Map[String, String])
                                                            (implicit t: ClassTag[FR]) extends PartitionReaderFactory {

  /**
   * A serializable copy of the options because [[CaseInsensitiveStringMap]] is not serializable
   */
  val _options = new util.HashMap[String, String]()
  _options.putAll(options)

  import scala.collection.JavaConverters._
  // Retrieve Hadoop configuration and store it in a serializable form to use it at the worker nodes while writing
  val hadoopConf: BeastOptions =
    new BeastOptions(SparkSession.active.sessionState.newHadoopConfWithOptions(options.asScala.toMap))

  override def createReader(p: InputPartition): PartitionReader[InternalRow] = {
    new SpatialFilePartitionReader[FR](p.asInstanceOf[FilePartition], _options, hadoopConf)
  }
}

/**
 * A reader class that takes a partition and reads all features in that partition.
 * @param partition the partition to read
 * @param options additional options given by the user
 * @tparam FR the type of the feature reader
 */
class SpatialFilePartitionReader[FR <: FeatureReader](partition: FilePartition, options: util.Map[String, String],
                                                      hadoopConf: BeastOptions)
                                                     (implicit t: ClassTag[FR]) extends PartitionReader[InternalRow] {
  require(partition.files.length == 1, "Can only work with one file at a time")
  val SpatialFileReader = t.runtimeClass.getConstructor().newInstance().asInstanceOf[FeatureReader]
  val filesplit = new SpatialFileSplit(partition.files(0).filePath.toPath, partition.files(0).start,
    partition.files(0).length, partition.files(0).locations)
  SpatialFileReader.initialize(filesplit, hadoopConf)

  override def next(): Boolean = SpatialFileReader.nextKeyValue()

  override def get(): InternalRow = SpatialFileReader.getCurrentValue.asInstanceOf[Feature].toInternalRow

  override def close(): Unit = SpatialFileReader.close()
}

/**
 * A spatial file partition is a regular file partition annotated with some additional information about the partition.
 * @param index the index of that partition (0-based)
 * @param files the set of files in that partition
 * @param partitionInfo additional information about the partition including its geometry area and size
 */
class SpatialDataframePartition(override val index: Int, override val files: Array[PartitionedFile], partitionInfo: IFeature)
  extends FilePartition(index, files)
