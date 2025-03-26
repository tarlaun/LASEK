/*
 * Copyright 2022 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.{MaxSplitSize, MinSplitSize, SpatialFilePartition}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * A class that iterates over all files that match a set of extensions.
 * This class lazily returns the matching paths which helps when only one file is needed and the input path
 * contains thousands of matching files.
 * @param fileSystem the file system that contains all the given files
 * @param files the list of files or directories to start the search on
 * @param extensions (optional) list of extensions to limit the search. Each one contains the dot, e.g., ".zip"
 * @param recursive recursively go into subdirectories in the search. Default: false
 * @param skipHidden skip hidden files that begin with "." or "_". Default: true
 * @param useMaster in directories with a master file, use it to list the files instead of using the file system
 * @param splitFiles a function that tells whether a file should be split or not
 */
class SpatialFilePartitioner(fileSystem: FileSystem, files: Iterator[Path],
                             extensions: Array[String] = null,
                             recursive: Boolean = false,
                             skipHidden: Boolean = true,
                             useMaster: Boolean = true,
                             splitFiles: FileStatus => Boolean = SpatialFilePartitioner.splitCompressed)
  extends Iterator[SpatialFilePartition2] with Logging {

  /** The list of paths that should be searched next */
  private val pathsToSearch = new ArrayBuffer[FileStatus]()
  for (file <- files)
    pathsToSearch.append(fileSystem.getFileStatus(file))

  /** Partitions that have been created but not returned yet */
  private val partitions = new ArrayBuffer[SpatialFilePartition2]()

  /** A filter that is used to determine which paths to consider based on user preferences */
  private lazy val filter: FileStatus => Boolean = {
    var subFilters = Array[FileStatus => Boolean]()
    if (extensions != null) {
      val lowerCaseExtensions = extensions.map(_.toLowerCase)
      val extensionFilter: FileStatus => Boolean = fileStatus => {
        if (fileStatus.isDirectory)
          true // Extensions are only applied to files not directories
        else {
          val name = fileStatus.getPath.getName.toLowerCase
          lowerCaseExtensions.indexWhere(ext => name.endsWith(ext)) != -1
        }
      }
      subFilters = subFilters :+ extensionFilter
    }
    if (skipHidden) {
      val hiddenFilter: FileStatus => Boolean = fileStatus => {
        val name = fileStatus.getPath.getName
        // Hidden filter applies to directories and files
        !(name.startsWith("_") || name.startsWith("."))
      }
      subFilters = subFilters :+ hiddenFilter
    }
    val combinedFilter: FileStatus => Boolean = path => subFilters.forall(filter => filter(path))
    combinedFilter
  }

  /** Number of partitions that have been created so far */
  private var numPartitionsCreated: Int = 0

  /** The record that will be returned next */
  private var nextRecord: SpatialFilePartition2 = prefetchNext

  private def prefetchNext: SpatialFilePartition2 = {
    while (partitions.nonEmpty || pathsToSearch.nonEmpty) {
      if (partitions.nonEmpty)
        return partitions.remove(partitions.length - 1)
      val pathToSearch = pathsToSearch.remove(pathsToSearch.size - 1)
      if (pathToSearch.isFile) {
        // A file, apply any user-provided filters before returning
        if (filter(pathToSearch)) {
          val locations = fileSystem.getFileBlockLocations(pathToSearch, 0, pathToSearch.getLen)
            .flatMap(_.getHosts).distinct
          val filePartition = SpatialFilePartition2(0, pathToSearch.getPath.toString,
            0, pathToSearch.getLen, locations, -1, -1, null, null)
          // Split the file if needed
          addFilePartitions(filePartition, partitions)
        }
      } else if (pathToSearch.isDirectory) {
        // A directory, return all files inside it while applying the filter
        var contents = fileSystem.listStatus(pathToSearch.getPath)
        val iMaster = if (!useMaster) -1 else contents.indexWhere(f => f.getPath.getName.startsWith("_master"))
        if (iMaster != -1) {
          // Override the contents based on the master file and ignore the user-provided filter
          val masterReader = new CSVReaderLite(fileSystem.open(contents(iMaster).getPath), '\t')
          try {
            val masterFilePartitions = masterReader.map(row => {
              val path = new Path(pathToSearch.getPath, row.getAs[String]("File Name"))
              // Use the file length from the master file to override the split length
              val length = row.getAs[String]("Data Size").trim.toLong
              val firstDimension = row.fieldIndex("Geometry") + 1
              val lastDimension = row.length - 1
              val numDimensions = (lastDimension - firstDimension + 1) / 2
              val minCoord = new Array[Double](numDimensions)
              val maxCoord = new Array[Double](numDimensions)
              for (d <- 0 until numDimensions) {
                minCoord(d) = row.getAs[String](firstDimension + d).trim.toDouble
                maxCoord(d) = row.getAs[String](firstDimension + numDimensions + d).trim.toDouble
              }
              val mbr = new EnvelopeNDLite(minCoord, maxCoord)
              val numRecords = row.getAs[String]("Record Count").trim.toLong
              val locations = fileSystem.getFileBlockLocations(path, 0, length)
                .flatMap(_.getHosts).distinct
              SpatialFilePartition2(0, path.toString, 0, length, locations, numRecords, -1, null, mbr)
            })
            for (masterFilePartition <- masterFilePartitions)
              addFilePartitions(masterFilePartition, partitions)
          } finally {
            masterReader.close()
          }
        } else {
          contents = contents.filter(status => filter(status))
          for (content <- contents) {
            if (recursive || !content.isDirectory)
              pathsToSearch.append(content)
          }
        }
      } else {
        logWarning(s"Do not know how to handle $pathToSearch")
      }

    }
    // No more paths to search. Return null to mark end of iterator
    null
  }

  override def hasNext: Boolean = nextRecord != null

  override def next(): SpatialFilePartition2 = {
    val currentRecord = nextRecord
    nextRecord = prefetchNext
    currentRecord
  }

  def addFilePartitions(file: SpatialFilePartition2, partitions: ArrayBuffer[SpatialFilePartition2]): Unit = {
    val fileStatus = fileSystem.getFileStatus(new Path(file.filePath))
    if (!splitFiles(fileStatus)) {
      file.index = numPartitionsCreated
      numPartitionsCreated += 1
      partitions.append(file)
    } else {
      // Split files
      val splitSize = {
        val minSize = fileSystem.getConf.getLong(MinSplitSize, 1)
        val maxSize = fileSystem.getConf.getLong(MaxSplitSize, Long.MaxValue)
        Math.max(minSize, Math.min(maxSize, fileStatus.getBlockSize))
      }
      val blkLocations = fileSystem.getFileBlockLocations(fileStatus, file.offset, file.length)
      var partitionStart = file.offset
      val fileEnd = file.end
      val SPLIT_SLOP = 1.1 // 10% slop
      while (partitionStart < fileEnd) {
        val blkIndex = blkLocations.find(bl => partitionStart >= bl.getOffset && partitionStart < bl.getOffset + bl.getLength)
        assert(blkIndex.isDefined, s"No locations found for block at offset $partitionStart")
        val partitionEnd = if ((fileEnd - partitionStart).toDouble / splitSize > SPLIT_SLOP)
          partitionStart + splitSize
        else
          fileEnd
        val partitionID = numPartitionsCreated
        val spatialPartition = SpatialFilePartition2(partitionID, file.filePath, partitionStart,
          partitionEnd - partitionStart, blkIndex.get.getHosts, file.numFeatures, file.numPoints, file.avgSideLength, file.mbr)
        numPartitionsCreated += 1
        partitions.append(spatialPartition)
        partitionStart = partitionEnd
      }
    }
  }
}

object SpatialFilePartitioner {
  /** The configuration entry for the minimum split size */
  val MinSplitSize: String = FileInputFormat.SPLIT_MINSIZE

  /** The configuration entry for the maximum split size */
  val MaxSplitSize: String = FileInputFormat.SPLIT_MAXSIZE

  lazy val codecFactory: CompressionCodecFactory =  new CompressionCodecFactory(new Configuration())

  /**
   * A splitter that splits non-compressed files and block-compressed files.
   * @return
   */
  def splitCompressed: FileStatus => Boolean = fileStatus => codecFactory.getCodec(fileStatus.getPath) match {
    case null | _: SplittableCompressionCodec => true
    case _ => false
  }
}