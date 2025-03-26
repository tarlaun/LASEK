/*
 * Copyright 2021 University of California, Riverside
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
package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.ITile
import edu.ucr.cs.bdlab.beast.util.{FileListRDD, IConfigurable, OperationParam, Parallel2}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}

import java.io.PrintStream
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * An RDD partition for a RasterFileRDD. It represents part of a raster file.
 * @param index the ID of the partition
 * @param path the path to the file
 * @param offset the start offset to read from the file
 * @param length the length of the partition in bytes
 * @param locations list of hosts that has this partition
 */
case class RasterFilePartition(override val index: Int, path: String,
                               offset: Long, length: Long, locations: Array[String]) extends Partition {
  override def toString: String = s"Raster Partition #$index ($path: $offset+$length)"
}

/**
 * A raster RDD that reads tiles from a file
 */
class RasterFileRDD[T](@transient sc: SparkContext, path: String, @transient _opts: BeastOptions) extends RDD[ITile[T]](sc, Seq()) {
  import RasterFileRDD._

  lazy val minSplitSize: Long = opts.getSizeAsBytes(SPLIT_MINSIZE, 4L * 1024 * 1024)

  lazy val maxSplitSize: Long = opts.getSizeAsBytes(SPLIT_MAXSIZE, Long.MaxValue)

  /**Hadoop configuration loaded into BeastOptions to make it serializable*/
  val opts: BeastOptions = new BeastOptions(_opts).mergeWith(sc.getConf).mergeWith(new BeastOptions(sc.hadoopConfiguration))

  override protected def getPartitions: Array[Partition] = {
    val p = new Path(path)
    val fileSystem = p.getFileSystem(sc.hadoopConfiguration)
    val partitions = new collection.mutable.ArrayBuffer[RasterFilePartition]
    val rasterFiles: Array[FileStatus] = getRasterFiles(fileSystem, p)
    val totalLength: Long = rasterFiles.map(_.getLen).sum
    val preferredSplitSize: Long = java.lang.Long.highestOneBit(totalLength / sc.defaultParallelism)
    Parallel2.forEach(rasterFiles.length, (i1: Int, i2: Int) => {
      for (i <- i1 until i2) {
        val rasterFile = rasterFiles(i)
        val rasterPath: Path = rasterFile.getPath
        val length: Long = rasterFile.getLen
        val splitSize: Long = (preferredSplitSize min rasterFile.getBlockSize) max minSplitSize min maxSplitSize
        // If file not splittable.
        if (!isSplittable(rasterFile.getPath.getName) || length == 0) {
          val locations = fileSystem.getFileBlockLocations(rasterFile, 0, length)
            .flatMap(l => l.getHosts.iterator).distinct
          partitions.synchronized {
            val partitionID = partitions.size
            partitions.append(RasterFilePartition(partitionID, rasterFile.getPath.toString, 0, length, locations))
          }
        } else {
          var bytesRemaining: Long = length
          while (bytesRemaining / splitSize > SPLIT_SLOP) {
            val locations = fileSystem.getFileBlockLocations(rasterFile, length - bytesRemaining, splitSize)
              .flatMap(l => l.getHosts.iterator)
            partitions.synchronized {
              val partitionID = partitions.size
              partitions.append(RasterFilePartition(partitionID, rasterFile.getPath.toString, length - bytesRemaining, splitSize, locations))
            }
            bytesRemaining -= splitSize
          }
          if (bytesRemaining != 0) {
            val locations = fileSystem.getFileBlockLocations(rasterFile, length - bytesRemaining, bytesRemaining)
              .flatMap(l => l.getHosts.iterator)
            partitions.synchronized {
              val partitionID = partitions.size
              partitions.append(RasterFilePartition(partitionID, rasterFile.getPath.toString, length - bytesRemaining, bytesRemaining, locations))
            }
          }
        }
      }
    })
    logInfo(s"Generated ${partitions.size} partitions from raster input path '$path'")
    partitions.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[RasterFilePartition].locations

  private def isSplittable(rasterFilePath:String): Boolean = true

  override def compute(split: Partition, context: TaskContext): Iterator[ITile[T]] = {
    logDebug(s"Processing $split")
    val rasterPartition: RasterFilePartition = split.asInstanceOf[RasterFilePartition]
    val rasterPath = new Path(rasterPartition.path)
    val fileSystem = rasterPath.getFileSystem(opts.loadIntoHadoopConf())
    val rasterReader: IRasterReader[T] = RasterHelper.createRasterReader(fileSystem, rasterPath, opts)
    context.addTaskCompletionListener[Unit] { context =>
      // TODO: Update the bytes read before closing is to make sure lingering bytesRead statistics in
      // this thread get correctly added.
      // Close the reader of free up resources
      rasterReader.close()
    }

    // Sort by tileOffset, avoid random seek position
    val tileIds = rasterReader.getTileOffsets.zipWithIndex.filter(idpos => {
      rasterReader.isValidTile(idpos._2) &&
        idpos._1 >= rasterPartition.offset && idpos._1 < rasterPartition.offset + rasterPartition.length
    }).sortBy(x => x._1).map(_._2)
    tileIds.iterator.map(tid => rasterReader.readTile(tid))
  }
}

object RasterFileRDD extends IConfigurable {
  @OperationParam(description = "Maximum split size", showInUsage = false, defaultValue = "Long.MAX_VALUE")
  val SPLIT_MAXSIZE: String = "mapreduce.input.fileinputformat.split.maxsize"

  @OperationParam(description = "Minimum split size", showInUsage = false, defaultValue = "1")
  val SPLIT_MINSIZE: String = "mapreduce.input.fileinputformat.split.minsize"

  @OperationParam(description = "Whether to scan input path recursively", showInUsage = false, defaultValue = "false")
  val INPUT_DIR_RECURSIVE: String = "mapreduce.input.fileinputformat.input.dir.recursive"

  @OperationParam(description = "Number of threads to list input files", showInUsage = false, defaultValue = "1")
  val LIST_STATUS_NUM_THREADS: String = "mapreduce.input.fileinputformat.list-status.num-threads"

  val DEFAULT_LIST_STATUS_NUM_THREADS: Int = 1

  @OperationParam(description = "Input format for raster files",
    showInUsage = true)
  val RasterInputFormat: String = "rformat"

  private val SPLIT_SLOP: Double = 1.1 // 10% slop

  /**
   * Get raster files as a list of string
   * @param rasterFileSystem the file system that contains the raster files
   * @param rasterPath the path to a single file or a directory of raster files
   * @return list of raster files
   */
  private def getRasterFiles(rasterFileSystem: FileSystem, rasterPath: Path): Array[FileStatus] = {
    if (rasterFileSystem.getFileStatus(rasterPath).isDirectory) {
      rasterFileSystem.listStatus(rasterPath, new PathFilter() {
        override def accept(path: Path): Boolean =
          path.getName.toLowerCase().endsWith(".tif") || path.getName.toLowerCase().endsWith(".hdf")
      })
    } else {
      Array(rasterFileSystem.getFileStatus(rasterPath))
    }
  }

  /**
   * Read a raster file as a JavaRDD
   * @param javaSparkContext the Java Spark context
   * @param path path to a file or a directory with files
   * @param opts additional options for loading the file
   * @return the created JavaRDD
   */
  def readRaster[T](javaSparkContext: JavaSparkContext, path: String, opts: BeastOptions): JavaRDD[ITile[T]] =
    JavaRDD.fromRDD(new RasterFileRDD(javaSparkContext.sc, path, opts))

  /**
   * Build a raster index on all GeoTIFF files in a directory.
   * @param sparkContext spark context to parallelize index creation
   * @param dir the directory that contains raster files
   * @param indexFile the path of the index file to write
   */
  def buildIndex(sparkContext: SparkContext, dir: String, indexFile: String): Unit = {
    val hadoopConf = sparkContext.hadoopConfiguration
    val opts = new BeastOptions(hadoopConf)
    val files = FileListRDD.listFiles(sparkContext, dir, "*.tif")
    val indexInfo = files.map(filename => {
        val rasterPath = new Path(dir, filename)
        val infs = rasterPath.getFileSystem(opts.loadIntoHadoopConf(null))
        val filesize = infs.getFileStatus(rasterPath).getLen
        val metadata = GeoTiffReader.getRasterMetadata(infs, rasterPath)
        val mbr: Envelope = metadata.envelope
        val srid: Int = metadata.srid
        val mbrCorners: Array[Float] = Array[Double](mbr.getMinX, mbr.getMinY, mbr.getMaxX, mbr.getMinY,
          mbr.getMaxX, mbr.getMaxY, mbr.getMinX, mbr.getMaxY, mbr.getMinX, mbr.getMinY).map(_.toFloat)
        val t = Reprojector.findTransformationInfo(srid, 4326)
        t.mathTransform.transform(mbrCorners, 0, mbrCorners, 0, 4)
        (filename, filesize, mbr, srid, mbrCorners)
      }).collect().sortBy(_._1).zipWithIndex
    // Write index information to a CSV file
    val indexPath: Path = new Path(indexFile)
    val indexFS = indexPath.getFileSystem(hadoopConf)
    val out = new PrintStream(indexFS.create(indexPath))
    try {
      out.println(Seq("ID","FileName","FileSize","x1","y1","x2","y2","SRID","Geometry4326").mkString(";"))
      val geomFactory = new GeometryFactory()
      indexInfo.foreach(f => {
        val geometry = geomFactory.createPolygon(f._1._5.grouped(2).map(c => new Coordinate(c(0), c(1))).toArray)
        out.println(Seq(f._2, f._1._1, f._1._2,
          f._1._3.getMinX, f._1._3.getMinY, f._1._3.getMaxX, f._1._3.getMaxY,
          f._1._4, geometry.toText).mkString(";"))
      })
    } finally {
      out.close()
    }
  }

  /**
   * Selects all the raster files that could potentially overlap the given query range from a directory of files.
   * If that directory contains an index file, i.e., "_index.csv", then this index is used to prune files that
   * are not relevant. If no index file is there, then all files are returned.
   * @param fileSystem the file system at which the raster files exist
   * @param dir the directory that contains the raster files
   * @param range the query range to limit the files
   * @return the list of files that potentially overlap the given query range
   */
  def selectFiles(fileSystem: FileSystem, dir: String, range: Geometry): Array[String] = {
    if (fileSystem.getFileStatus(new Path(dir)).isFile) {
      // The input points to a single file, just return it
      return Array(dir)
    }
    val indexFile = new Path(dir, "_index.csv")
    if (!fileSystem.exists(indexFile)) {
      // No index file, return all directory contents
      return getRasterFiles(fileSystem, new Path(dir)).map(_.getPath.toString)
    }
    // Use the index file to return only matching files
    val matchingFiles = new ArrayBuffer[String]()
    val inputStream = fileSystem.open(indexFile)
    try {
      val lines: Iterator[String] = Source.fromInputStream(inputStream).getLines()
      val header: Array[String] = lines.next.split(";")
      val mbrAtts = header.indexOf("x1")
      val filenameAttr = header.indexOf("FileName")
      val sridAttr = header.indexOf("SRID")
      for (line <- lines) {
        val data: Array[String] = line.split(";").map(_.trim)
        val mbr = data.slice(mbrAtts, mbrAtts + 4).map(_.toDouble)
        val srid = data(sridAttr).toInt
        if (srid != range.getSRID)
          Reprojector.reprojectEnvelopeInPlace(mbr, srid, range.getSRID)
        if (range.overlaps(range.getFactory.toGeometry(new Envelope(mbr(0), mbr(2), mbr(1), mbr(3))))) {
          matchingFiles.append(new Path(dir, data(filenameAttr)).toString)
        }
      }
    } finally {
      inputStream.close()
    }
    matchingFiles.toArray
  }
}