package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.Feature
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper
import edu.ucr.cs.bdlab.beast.synopses.Summary
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.io.PrintStream
import java.util
import scala.reflect.ClassTag


/**
 * A builder for file writers that write spatial data
 *
 * @param info
 * @tparam FW
 */
class SpatialWriteBuilder[FW <: FeatureWriter](info: LogicalWriteInfo)(implicit t: ClassTag[FW]) extends WriteBuilder with SupportsTruncate {
  var isAppend : Boolean = true

  override def buildForBatch(): BatchWrite = new SpatialBatchWrite[FW](info, isAppend)
  override def truncate(): WriteBuilder = {
    isAppend = false
    this
  };
}

class SpatialBatchWrite[FW <: FeatureWriter](linfo: LogicalWriteInfo, isAppend : Boolean)
                                            (implicit t: ClassTag[FW])  extends BatchWrite {


  lazy val hadoopConf: Configuration = SparkSession.active.sessionState.newHadoopConf()

  val originalPath : Path = new Path(linfo.options().get("path"))
  val outPath: Path = {
    new Path(linfo.options().get("path") + "/tmp_output")
  }

  override def createBatchWriterFactory(pinfo: PhysicalWriteInfo): DataWriterFactory = {
    val options : java.util.HashMap[String, String] = new java.util.HashMap[String,String](linfo.options().size())
    linfo.options().forEach{case (k,v) => options.put(k,v)}
    new SpatialDataWriteFactory[FW](outPath.toString, pinfo.numPartitions(), linfo.schema(), options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val partitionsInfo: Array[SpatialWriteMessage] = messages.map(_.asInstanceOf[SpatialWriteMessage])
    val fileSystem = outPath.getFileSystem(hadoopConf)
    // Move all the files to the output directory
    for(i <- partitionsInfo.indices) {
      val partitionInfo = partitionsInfo(i)
      val tempPath = new Path(partitionInfo.tempPath)
      // Note: For some file formats, e.g., Shapefile, multiple files could be written and we move all of them.
      for (tempFile <- fileSystem.listStatus(tempPath).map(_.getPath)) {
        var newFilename = new Path(originalPath, tempFile.getName)
        var pathExists = fileSystem.exists(newFilename)
        val extension = tempFile.getName.substring(tempFile.getName.lastIndexOf(".")+1)
        if(isAppend) {
          var j = i
          while(pathExists) {
            newFilename = new Path(originalPath, f"part-$j%05d.$extension")
            partitionsInfo(i) = new SpatialWriteMessage(j, partitionInfo.tempPath, f"part-$j%05d.$extension")
            pathExists = fileSystem.exists(newFilename)
            j+=1
          }
        } else if(pathExists) { // truncate
          fileSystem.delete(newFilename, true)
        }
        fileSystem.rename(tempFile, newFilename)
      }
      // Delete the temp directory. It should be empty
      assert(fileSystem.listStatus(tempPath).isEmpty, s"Directory '${tempPath}' should be empty but is not")
      fileSystem.delete(outPath, true)
    }
    // Merge the new partition info with an existing _master file or write a new _master file
    // If there is an existing master file, append to it.
    // If this is the first master file to write, write a new one with the appropriate header.
    val currentMasterFile = SpatialFileRDD.getMasterFilePath(fileSystem, originalPath)
    val newMasterFileName = s"_master-${System.currentTimeMillis()}"
    val masterFileOutput = new PrintStream(fileSystem.create(new Path(originalPath, newMasterFileName)))
    if (currentMasterFile == null || !isAppend) {
      // First master file, write a new one with a header
      IndexHelper.printMasterFileHeader(partitionsInfo.head.getCoordinateDimension, masterFileOutput)
    } else {
      // Copy data from the most recent master file
      val inMasterFile = fileSystem.open(currentMasterFile)
      try {
        IOUtils.copyLarge(inMasterFile, masterFileOutput)
      } finally {
        inMasterFile.close()
      }
    }
    // Append all the information of the new partitions
    for (partitionInfo <- partitionsInfo) {
      masterFileOutput.println
      masterFileOutput.print(getPartitionAsText(partitionInfo))
    }
    masterFileOutput.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Delete all the files and the temporary directories
  }

  protected def getPartitionAsText(partition: SpatialWriteMessage): String =
    IndexHelper.getPartitionAsText(partition.partitionId, partition.fileName, partition).toString
}

class SpatialDataWriteFactory[FW <: FeatureWriter]
  (outPath: String, numPartitions: Int, schema: StructType, options: java.util.HashMap[String, String])
  (implicit t: ClassTag[FW]) extends DataWriterFactory {

  import scala.collection.JavaConverters._

  val hadoopConf: BeastOptions = BeastOptions.fromMap(options)

  val featureWriterClass: Class[_<:FeatureWriter] = t.runtimeClass.asSubclass(classOf[FeatureWriter])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val tempPath = new Path(outPath, s"temp-$partitionId-$taskId")
    val fileSystem = tempPath.getFileSystem(hadoopConf.loadIntoHadoopConf(null))
    fileSystem.mkdirs(tempPath)
    val extension: String = featureWriterClass.getConstructor().newInstance().getFileExtension(outPath, hadoopConf)
    val filePath = new Path(tempPath, f"part-$partitionId%05d$extension")
    val featureWriter = featureWriterClass.getConstructor().newInstance()
    // call initialize TODO
    new SpatialDataWriter(featureWriter, partitionId, filePath, hadoopConf.loadIntoHadoopConf(null), schema, options)
  }
}

class SpatialDataWriter(featureWriter: FeatureWriter, partitionId: Int, path: Path, hadoopConf: Configuration,
                        schema: StructType,
                        options: util.Map[String, String]) extends DataWriter[InternalRow] {
  // Initialize the writer
  featureWriter.initialize(path, hadoopConf)

  val fileSummary = new SpatialWriteMessage(partitionId, path.getParent.toString, path.getName)

  override def write(record: InternalRow): Unit = {
    val values = record.toSeq(schema).toArray
    val feature = new Feature(values, schema)
    fileSummary.expandToFeature(feature)
    featureWriter.write(feature)
  }

  override def commit(): WriterCommitMessage = {
    fileSummary
  }

  override def abort(): Unit = featureWriter.close()

  override def close(): Unit = {
    featureWriter.close()
  }
}

/**
 * A write message that stores information about a partition that has been written to the output
 * @param tempPath the temporary path where the output file is written
 */
class SpatialWriteMessage(val partitionId: Int, val tempPath: String, val fileName: String) extends Summary with WriterCommitMessage
