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

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.{BigIFDEntry, IFDEntry, TiffConstants}
import edu.ucr.cs.bdlab.beast.util._
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, TaskContext}
import org.geotools.coverage.grid.io.imageio.geotiff.CRS2GeoTiffMetadataAdapter
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.io._
import java.util.zip.DeflaterOutputStream
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A writer for GeoTiff files. It takes [[edu.ucr.cs.bdlab.beast.geolite.RasterMetadata]] at construction
 * and a sequence of [[edu.ucr.cs.bdlab.beast.geolite.ITile]]s to write.
 * The ways this class works is that it writes all tile data to a temporary file. When closed, it will write the
 * GeoTiff header file and appends tile data to it. Therefore, the given file will not be populated with data
 * until the writer is closed.
 * @param outPath the path of the output file that will contain the GeoTIFF after the writer is closed
 * @param rasterMetadata metadata of the raster file that contains CRS and other information
 * @param bare when set to true, the writer will only write tile data without GeoTiff headers to the given output
 * @param opts additional options for the writer
 */
class GeoTiffWriter[T](val outPath: Path, rasterMetadata: RasterMetadata, bare: Boolean = false,
                    opts: BeastOptions = new BeastOptions())
  extends AutoCloseable with Logging {
  import edu.ucr.cs.bdlab.raptor.GeoTiffWriter._

  /**The file system of all output files*/
  private val fileSystem: FileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf())

  // Initialize a temporary file for writing the tile data
  /**A temporary file where all tile data will be written to until the file is closed*/
  private val tileDataPath: Path = if (bare) outPath else new Path(outPath.toString+"_tiledata")

  /**An output stream that writes to the temporary file*/
  private val tileDataOut: FSDataOutputStream = fileSystem.create(tileDataPath)

  /**Keeps track of tile offsets and lengths to write the header at the end*/
  protected val tileOffsetsLengthPath = new Path(outPath.toString+"_tilemetadata")

  /** The output stream to write tile metadata to (offset and length for each one). */
  protected val tileOffsetsLengthsOut: FSDataOutputStream = fileSystem.create(tileOffsetsLengthPath)

  /**Number of bits for each sample*/
  private var bitsPerSample: Array[Int] = _
  if (opts.contains(BitsPerSample))
    bitsPerSample = opts.getString(BitsPerSample, "8").split(",").map(_.toInt)

  /**Data types of all samples (1=Unsigned Int, 2= Signed Int, 3=Float)*/
  private var sampleFormats: Array[Int] = _
  if (opts.contains(SampleFormats))
    sampleFormats = opts.getString(SampleFormats, "1").split(",").map(_.toInt)
  // If bits per sample is set, then it is assumed to be integers
  else if (bitsPerSample != null)
    sampleFormats = bitsPerSample.map(_ => 1)

  /**The value that marks invalid pixels*/
  private var fillValue: Int = opts.getInt(GeoTiffWriter.FillValue, Int.MaxValue)

  /**Set the value that will be used to fill empty pixels*/
  def setFillValue(value: Int): Unit = this.fillValue = value

  /**Type of compression for tiles*/
  val compression: Int = opts.getInt(Compression, TiffConstants.COMPRESSION_LZW)

  /**The predictor determines whether to apply difference to */
  val predictor: Int = if (compression == TiffConstants.COMPRESSION_LZW)
    opts.getInt(Predictor, if (compression == TiffConstants.COMPRESSION_LZW) 1 else 0)
  else 0

  /**Number of bands in the file*/
  var numBands: Int = 1

  private def inferSampleFormats(tile: ITile[T]): Unit = {
    tile.pixelType match {
      case ByteType => bitsPerSample = Array(8); sampleFormats = Array(1)
      case ShortType => bitsPerSample = Array(16); sampleFormats = Array(1)
      case IntegerType => bitsPerSample = Array(32); sampleFormats = Array(1)
      case FloatType => bitsPerSample = Array(32); sampleFormats = Array(3)
      case ArrayType(ByteType, _) =>
        bitsPerSample = Array.fill(numBands)(8)
        sampleFormats = Array.fill(numBands)(1)
      case ArrayType(ShortType, _) =>
        bitsPerSample = Array.fill(numBands)(16)
        sampleFormats = Array.fill(numBands)(1)
      case ArrayType(IntegerType, _) =>
        bitsPerSample = Array.fill(numBands)(32)
        sampleFormats = Array.fill(numBands)(1)
      case ArrayType(FloatType, _) =>
        bitsPerSample = Array.fill(numBands)(32)
        sampleFormats = Array.fill(numBands)(3)
    }
  }

  /**
   * Write the given tile to the raster file
   * @param tile the tile to write to the file
   */
  def write(tile: ITile[T]): Unit = {
    numBands = tile.numComponents
    if (sampleFormats == null || bitsPerSample == null)
      inferSampleFormats(tile)
    val tileOffset: Long = tileDataOut.getPos

    // Since a tile is typically small, we first write the entire tile to memory and then compress if needed.
    val baos = new ByteArrayOutputStream()
    val dataOutput: OutputStream = compression match {
      case TiffConstants.COMPRESSION_NONE => baos
      case TiffConstants.COMPRESSION_LZW => new LZWOutputStream(baos)
      case TiffConstants.COMPRESSION_DEFLATE => new DeflaterOutputStream(baos)
    }
    val pixelOutput = new BitOutputStream(dataOutput)

    // According to TIFF specs, the data of all tiles must be equal to tileWidth x tileHeight
    // Even if it goes beyond the raster bounds
    val y2 = tile.y1 + rasterMetadata.tileHeight - 1
    val x2 = tile.x1 + rasterMetadata.tileWidth - 1
    for (j <- tile.y1 to y2; i <- tile.x1 to x2) {
      // TODO apply the difference predictor if needed
      if (j > tile.y2 || i > tile.x2 || tile.isEmpty(i, j)) {
        // Empty pixel. Write the fill value for all samples
        for (b <- 0 until numBands)
          pixelOutput.write(fillValue, bitsPerSample(b))
      } else {
        // A valid value
        if (numBands == 1) {
          tile.getPixelValue(i, j) match {
            case i: Byte => pixelOutput.write(i, bitsPerSample(0))
            case i: Short => pixelOutput.write(i, bitsPerSample(0))
            case i: Int => pixelOutput.write(i, bitsPerSample(0))
            case f: Float => pixelOutput.write(java.lang.Float.floatToIntBits(f), bitsPerSample(0))
            case d: Double => pixelOutput.write(java.lang.Double.doubleToLongBits(d), bitsPerSample(0))
          }
        } else {
          tile.getPixelValue(i, j) match {
            case is: Array[Int] =>
              for (b <- 0 until numBands)
                pixelOutput.write(is(b), bitsPerSample(b))
            case fs: Array[Float] =>
              for (b <- 0 until numBands)
                pixelOutput.write(java.lang.Float.floatToIntBits(fs(b)), bitsPerSample(b))
            case fs: Array[Double] =>
              for (b <- 0 until numBands)
                pixelOutput.write(java.lang.Double.doubleToLongBits(fs(b)), bitsPerSample(b))
            case os: Array[Any] =>
              for (b <- 0 until numBands) {
                os(b) match {
                  case i: Int => pixelOutput.write(i, bitsPerSample(b))
                  case f: Float => pixelOutput.write(java.lang.Float.floatToIntBits(f), bitsPerSample(b))
                  case d: Double => pixelOutput.write(java.lang.Double.doubleToLongBits(d), bitsPerSample(b))
                }
              }
          }
        }
      }
    }
    pixelOutput.close()
    val tileData = baos.toByteArray
    tileDataOut.write(tileData)
    val tileLength: Long = tileDataOut.getPos - tileOffset
    tileOffsetsLengthsOut.writeInt(tile.tileID)
    tileOffsetsLengthsOut.writeLong(tileOffset)
    tileOffsetsLengthsOut.writeLong(tileLength)
  }

  /**
   * Close the tile data output without writing a header file. Returns a path to a file that contains the offset
   * and length of each tile that was written.
   * @return a binary file that contains triplets of (tileID: Int, tileOffset: Long, tileLength: Long).
   */
  def closeTileDataOnly(): Path = {
    tileDataOut.close()
    fileSystem.rename(tileDataPath, outPath)
    tileOffsetsLengthsOut.close()
    tileOffsetsLengthPath
  }

  override def close(): Unit = {
    tileDataOut.close()
    tileOffsetsLengthsOut.close()
    // In addition to the bare data, we also need to prepend a header
    if (!bare)
      prependGeoTiffHeader(rasterMetadata, bitsPerSample, sampleFormats, compression, tileOffsetsLengthPath, fillValue,
        outPath, Array(tileDataPath), opts)
  }
}

object GeoTiffWriter extends Logging {
  @OperationParam(description = "Overwrite the output if it already exists {true, false}.", defaultValue = "false")
  val OverwriteOutput = "overwrite"

  @OperationParam(description = "Bits per sample. Comma separated list", defaultValue = "8")
  val BitsPerSample = "bitspersample"

  @OperationParam(description = "Instructs GeoTiffWriter to use as few bits as possible by computing the maximum first",
    defaultValue = "false")
  val CompactBits = "compactbits"

  @OperationParam(description = "A special value that marks empty pixels")
  val FillValue = "fillvalue"

  @OperationParam(description = "The data type of all samples (1=Unsigned Int, 2= Signed Int, 3=Float)", defaultValue = "1")
  val SampleFormats = "sampleformats"

  @OperationParam(description = "Compression technique to use for GeoTIFF (1=PackBits, 5=LZW, 8=Deflate)", defaultValue = "5")
  val Compression = "compression"

  @OperationParam(description = "GeoTIFF predictor (0=none, 1=difference)", defaultValue = "0")
  val Predictor = "predictor"

  @OperationParam(description = "The mode for writing the file {compatibility, distributed}", defaultValue = "distributed")
  val WriteMode = "writemode"

  @OperationParam(description = "Whether to write data for all tiles including empty ones", defaultValue = "false")
  val NoEmptyTiles = "noemptytiles"

  @OperationParam(description = "Use BigTiff to write GeoTiff files {yes,no,auto}", defaultValue = "auto")
  val BigTiff = "bigtiff"

  // TODO add other options such as writing big/little endian

  /**
   * Create a temporary IFDEntry with one of the standard data types
   * @param tag
   * @param valueType
   * @param value
   * @return
   */
  def makeIFDEntry(tag: Short, valueType: Short, value: Any): TempIFDEntry = {
    val finalValue = value match {
      case s: String => s
      case v: Array[Int] => v
      case v: Array[Double] => v
      case v: Byte => Array[Int](v.toInt)
      case v: Short => Array[Int](v.toInt)
      case v: Int => Array[Int](v)
      case v: Long => Array[Long](v)
      case v: Double => Array[Double](v)
      case v: Array[Byte] => v.map(_.toInt)
      case v: Array[Short] => v.map(_.toInt)
      case v: Array[Long] => v
    }
    TempIFDEntry(tag, valueType, finalValue)
  }

  /**
   * Save a RasterRDD as a GeoTiff file in compatibility mode where it produces a set of GeoTiff files that can be
   * read by other GIS software such as QGIS. It concatenates all the tiles that belong to the same RasterMetadata
   * (same file) into one file that contains all tiles for all partitions.
   * @param rasterRDD the RDD of tiles to write to the output
   * @param outPath the output path to write the file to
   * @param opts additional options for writing the file
   */
  private def saveAsGeoTiffCompatibilityMode[T](rasterRDD: RDD[ITile[T]], outPath: String, opts: BeastOptions): Unit = {
    val sc: SparkContext = rasterRDD.sparkContext
    val tempPath: String = new File(outPath, "temp").getPath
    // Intermediate results from all executors in the following format
    // - RasterMetadata for a given file
    // - String: The path of the file that contains tile data (no header)
    // - String: Path to a file that contains (tileID, tileOffset, tileLength) triplets
    // - Array[Int]: bit per sample for the pixels in that file
    // - Array[Int]: sample formats for the pixels in that file
    val interimResults: Array[(RasterMetadata, String, String, Array[Int], Array[Int])] =
      sc.runJob(rasterRDD, (taskContext: TaskContext, tiles: Iterator[ITile[T]]) => {
        val taskTempDir: Path = new Path(new Path(tempPath), taskContext.taskAttemptId().toString)
        val fileSystem: FileSystem = taskTempDir.getFileSystem(opts.loadIntoHadoopConf())
        fileSystem.mkdirs(taskTempDir)
        // Cleanup handler
        val deleteTempDir: (TaskContext, Throwable) => Unit = (_, _) => fileSystem.delete(taskTempDir, true)
        taskContext.addTaskFailureListener(deleteTempDir)
        val writers: mutable.HashMap[RasterMetadata, GeoTiffWriter[T]] = new mutable.HashMap()
        for (tile <- tiles) {
          val writer = writers.getOrElseUpdate(tile.rasterMetadata, {
            val filename = f"part-${taskContext.partitionId()}%05d-${writers.size}%03d_tiledata-${taskContext.taskAttemptId().toString}"
            val geoTiffPath = new Path(new Path(outPath), filename)
            val newWriter = new GeoTiffWriter[T](geoTiffPath, tile.rasterMetadata, true, opts)
            newWriter
          })
          writer.write(tile)
        }
        writers.map(w => {
          val tileInfoPath: Path = new Path(new Path(outPath), f"part-${taskContext.partitionId()}%05d-${writers.size}%03d_tileinfo-${taskContext.taskAttemptId().toString}")
          val tileOffsetsLengthsPath = w._2.closeTileDataOnly()
          fileSystem.rename(tileOffsetsLengthsPath, tileInfoPath)
          (w._1, w._2.outPath.toString, tileInfoPath.toString, w._2.bitsPerSample, w._2.sampleFormats)
        }).toArray
      }
    ).flatMap(_.iterator)
    logInfo(s"Done with writing data files. Now merging ${interimResults.length} files into one")
    try {
      // Merge intermediate files for each RasterMetadata
      // Build a table of intermediate data for each file.
      // Each file is identified by RasterMetadata
      val resultsPerFile: Array[(RasterMetadata, Array[(RasterMetadata, String, String, Array[Int], Array[Int])])] =
          interimResults.groupBy(_._1).toArray
      // Now, merge all partial files for each RasterMetadata
      val fileSystem = new Path(outPath).getFileSystem(opts.loadIntoHadoopConf())
      val compression: Int = opts.getInt(Compression, TiffConstants.COMPRESSION_LZW)
      val fillValue: Int = opts.getInt(GeoTiffWriter.FillValue, -1)
      Parallel2.forEach(resultsPerFile.length, (i1, i2) => {
        for (fileIndex <- i1 until i2) {
          val metadata: RasterMetadata = resultsPerFile(fileIndex)._1
          val partialResults = resultsPerFile(fileIndex)._2

          val tileDataPaths: Array[String] = partialResults.map(_._2)
          // We assume that all files have the same bitsPerSample and sampleFormats
          val bitsPerSample: Array[Int] = partialResults(0)._4
          val sampleFormats: Array[Int] = partialResults(0)._5
          // Write a new tileOffsetsLengths file that combines all files after adjusting for concatenated data files
          val filename = f"part-${fileIndex}%05d.tif"
          val tileOffsetsLengthsPath = new Path(outPath, filename + "_metadata")
          val tileOffsetsLengthsOut = fileSystem.create(tileOffsetsLengthsPath)
          var deltaOffset: Long = 0
          // Calculate the final tile offsets after accounting for concatenation
          // Since data files are concatenated in order, the offsets will be shifted according to the accumulated size
          for (tileInfoFile <- partialResults.map(x => x._3)) {
            var totalSize: Long = 0
            val tileInfoIn = fileSystem.open(new Path(tileInfoFile))
            while (tileInfoIn.available() > 0) {
              val tileID: Int = tileInfoIn.readInt()
              val tileOffset: Long = tileInfoIn.readLong()
              val tileLength: Long = tileInfoIn.readLong()
              assert(tileLength > 0, "Zero-length tiles are not expected")
              tileOffsetsLengthsOut.writeInt(tileID)
              tileOffsetsLengthsOut.writeLong(tileOffset + deltaOffset)
              tileOffsetsLengthsOut.writeLong(tileLength)
              totalSize += tileLength
            }
            tileInfoIn.close()
            deltaOffset += totalSize
            // Delete the temporary file
            fileSystem.delete(new Path(tileInfoFile), false)
          }
          tileOffsetsLengthsOut.close()

          val geoTiffPath: Path = new Path(new Path(outPath), filename)
          prependGeoTiffHeader(metadata, bitsPerSample, sampleFormats, compression, tileOffsetsLengthsPath,
            fillValue, geoTiffPath, tileDataPaths.map(new Path(_)), opts)
        }
      })
      logInfo("Files merged into one file. Now deleting temporary files.")
    } finally {
      // Delete the temporary directory in the output
      new Path(tempPath).getFileSystem(opts.loadIntoHadoopConf()).delete(new Path(tempPath), true)
    }
  }

  /**
   * Writes a raster RDD as a set of GeoTiff files.
   * @param rasterRDD the RDD of tiles to write to the output
   * @param outPath the output path to write the file to
   * @param opts additional options for writing the file
   */
  private def saveAsGeoTiffDistributedMode[T](rasterRDD: RDD[ITile[T]], outPath: String, opts: BeastOptions): Unit = {
    val sc: SparkContext = rasterRDD.sparkContext
    val tempPath: String = new File(outPath, "temp").getPath
    val interimFiles: Array[String] = sc.runJob(rasterRDD, (taskContext: TaskContext, tiles: Iterator[ITile[T]]) => {
      val taskTempDir: Path = new Path(new Path(tempPath), taskContext.taskAttemptId().toString)
      val fileSystem: FileSystem = taskTempDir.getFileSystem(opts.loadIntoHadoopConf())
      fileSystem.mkdirs(taskTempDir)
      // Cleanup handler
      val deleteTempDir: (TaskContext, Throwable) => Unit = (_, _) => fileSystem.delete(taskTempDir, true)
      taskContext.addTaskFailureListener(deleteTempDir)
      val writers: mutable.HashMap[RasterMetadata, GeoTiffWriter[T]] = new mutable.HashMap()
      // Completion handler that closes all open files
      val closeAllFiles: TaskContext => Unit = _ => {
        var counter: Int = 0
        for (writer <- writers.valuesIterator) {
          writer.close()
          val tempFile: Path = writer.outPath
          val filename: String = if (writers.size == 1)
            f"part-${taskContext.partitionId()}%05d.tif"
          else
            f"part-${taskContext.partitionId()}%05d-${counter}%03d.tif"
          val finalFile: Path = new Path(taskTempDir, filename)
          fileSystem.rename(tempFile, finalFile)
          counter += 1
        }
      }
      taskContext.addTaskCompletionListener[Unit](closeAllFiles)
      for (tile <- tiles) {
        val writer = writers.getOrElseUpdate(tile.rasterMetadata, {
          val filename = f"part-${taskContext.partitionId()}%05d-${writers.size}%03d-${taskContext.taskAttemptId().toString}.tif"
          val geoTiffPath = new Path(taskTempDir, filename)
          if (!fileSystem.exists(geoTiffPath)) {
            logWarning("Output file dose not exist, Create empty file on HDFS: " + geoTiffPath)
            val out = fileSystem.create(geoTiffPath)
            out.close()
          }
          new GeoTiffWriter[T](geoTiffPath, tile.rasterMetadata, false, opts)
        })
        writer.write(tile)
      }
      taskTempDir.toString
    })
    // Move interim files to the output directory and cleanup any remaining files
    val outP: Path = new Path(outPath)
    val fileSystem = outP.getFileSystem(opts.loadIntoHadoopConf())
    for (interimFile <- interimFiles) {
      val files = fileSystem.listStatus(new Path(interimFile))
      for (file <- files) {
        val targetFile = new Path(outP, file.getPath.getName)
        fileSystem.rename(file.getPath, targetFile)
      }
      fileSystem.delete(new Path(interimFile), false)
    }
    fileSystem.delete(new Path(tempPath), false)
  }

  def saveAsGeoTiff[T](rasterRDD: RDD[ITile[T]], outPath: String, opts: BeastOptions): Unit = {
    def maxArrayB(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
      for (i <- a.indices)
        a(i) = a(i) max b(i)
      a
    }
    def maxArrayS(a: Array[Short], b: Array[Short]): Array[Short] = {
      for (i <- a.indices)
        a(i) = a(i) max b(i)
      a
    }
    def maxArrayI(a: Array[Int], b: Array[Int]): Array[Int] = {
      for (i <- a.indices)
        a(i) = a(i) max b(i)
      a
    }
    def maxArrayL(a: Array[Long], b: Array[Long]): Array[Long] = {
      for (i <- a.indices)
        a(i) = a(i) max b(i)
      a
    }
    if (!opts.contains(BitsPerSample) && opts.getBoolean(CompactBits, false)) {
      // Automatically compute bits per sample based on the maximum value
      val pixelType = rasterRDD.first().pixelType
      val measures = RasterOperationsGlobal.flatten(rasterRDD)
      val max: Array[Long] = pixelType match {
        case ByteType =>
          Array(measures.map(_._4.asInstanceOf[Byte]).reduce(_ max _))
        case ShortType =>
          Array(measures.map(_._4.asInstanceOf[Short]).reduce(_ max _))
        case IntegerType =>
          Array(measures.map(_._4.asInstanceOf[Int]).reduce(_ max _))
        case LongType =>
          Array(measures.map(_._4.asInstanceOf[Long]).reduce(_ max _))
        case ArrayType(ByteType, _) =>
          measures.map(_._4.asInstanceOf[Array[Byte]]).reduce(maxArrayB).map(_.toLong)
        case ArrayType(ShortType, _) =>
          measures.map(_._4.asInstanceOf[Array[Short]]).reduce(maxArrayS).map(_.toLong)
        case ArrayType(IntegerType, _) =>
          measures.map(_._4.asInstanceOf[Array[Int]]).reduce(maxArrayI).map(_.toLong)
        case ArrayType(LongType, _) =>
          measures.map(_._4.asInstanceOf[Array[Long]]).reduce(maxArrayL).map(_.toLong)
        case _ => null // Float types will not need a maximum value
      }
      if (max != null) {
        val bitsPerSample = max.map(x => MathUtil.log2(x + 1))
        opts.set(BitsPerSample, bitsPerSample.mkString(","))
        opts.set(FillValue, max.max + 1)
      }
    }
    opts.getString(WriteMode, "distributed") match {
      case "distributed" => saveAsGeoTiffDistributedMode(rasterRDD, outPath, opts)
      case "compatibility" => saveAsGeoTiffCompatibilityMode(rasterRDD, outPath, opts)
    }
  }

  /**
   * Takes a file that contains tile data and prepends an appropriate GeoTiff header to it to make it a correct
   * GeoTiff file.
   * @param rasterMetadata the metadata of the raster file
   * @param bitsPerSample number of bits for each sample in the tile [[BitsPerSample]]
   * @param sampleFormats the formats of each sample in the file [1, 2 = Int, 3 = Float) [[SampleFormats]]
   * @param compression the compression technique used to write tile data [[Compression]]
   * @param tileOffsetsLengthsPath a file that contains triplets (tileID: Int, tileOffset: Long, tileLength: Long)
   * @param fillValue the value that marks empty pixels or `null` if no such value
   * @param outPath the path to write the final GeoTiff file
   * @param _tileDataPath the list of files (in order) that contain tile data
   * @param opts additional user-defined options
   */
  def prependGeoTiffHeader(rasterMetadata: RasterMetadata, bitsPerSample: Array[Int],
                           sampleFormats: Array[Int], compression: Int, tileOffsetsLengthsPath: Path,
                           fillValue: Int, outPath: Path, _tileDataPath: Array[Path],
                           opts: BeastOptions): Unit = {
    val numBands = bitsPerSample.length
    val software: String = "UCR Beast"
    var bigTiff: String = opts.getString(BigTiff, "auto")
    // 1. Prepare the list of entries
    var ifdEntries: mutable.ArrayBuffer[TempIFDEntry] = new ArrayBuffer[TempIFDEntry]()
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_SOFTWARE, TiffConstants.TYPE_ASCII, software))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_IMAGE_WIDTH, TiffConstants.TYPE_LONG, rasterMetadata.rasterWidth))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_IMAGE_LENGTH, TiffConstants.TYPE_LONG, rasterMetadata.rasterHeight))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_SAMPLES_PER_PIXEL, TiffConstants.TYPE_SHORT, numBands))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_BITS_PER_SAMPLE, TiffConstants.TYPE_SHORT, bitsPerSample))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_SAMPLE_FORMAT, TiffConstants.TYPE_SHORT, Array.fill(numBands)(sampleFormats(0))))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_COMPRESSION, TiffConstants.TYPE_SHORT, compression))
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_PHOTOMETRIC_INTERPRETATION, TiffConstants.TYPE_SHORT, 2))
    // Fill empty tiles with (0, 0) for offset and length. This no-GeoTiff-standard is supported by GDAL and Beast
    // See: https://gdal.org/drivers/raster/gtiff.html#sparse-files
    val tileOffsets: Array[Long] = new Array[Long](rasterMetadata.numTiles)
    val tileLengths: Array[Int] = new Array[Int](rasterMetadata.numTiles)
    val fileSystem: FileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf())
    val tilesOffsetsLengthIn: FSDataInputStream = fileSystem.open(tileOffsetsLengthsPath)
    val tileMetadataLength: Long = fileSystem.getFileStatus(tileOffsetsLengthsPath).getLen
    while (tilesOffsetsLengthIn.getPos < tileMetadataLength) {
      val tileID: Int = tilesOffsetsLengthIn.readInt()
      tileOffsets(tileID) = tilesOffsetsLengthIn.readLong()
      tileLengths(tileID) = tilesOffsetsLengthIn.readLong().toInt
    }
    tilesOffsetsLengthIn.close()
    fileSystem.delete(tileOffsetsLengthsPath, false) // Clean up, no longer needed
    var tileDataPath = _tileDataPath
    if (tileLengths.contains(0) && opts.getBoolean(NoEmptyTiles, defaultValue = false)) {
      // Append an additional tile that is all empty to be pointed to by all non-existent tiles
      // This is to ensure compatibility with all readers that cannot detect tiles with zero length as empty
      val emptyTilePath = new Path(tileDataPath(0).getParent, "emptyTile.tif")
      val emptyTileOut = fileSystem.create(emptyTilePath)
      val dataOutput: OutputStream = compression match {
        case TiffConstants.COMPRESSION_NONE => emptyTileOut
        case TiffConstants.COMPRESSION_LZW => new LZWOutputStream(emptyTileOut)
        case TiffConstants.COMPRESSION_DEFLATE => new DeflaterOutputStream(emptyTileOut)
      }
      val pixelOutput = new BitOutputStream(dataOutput)
      for (_ <- 0 until rasterMetadata.tileWidth * rasterMetadata.tileHeight; b <- 0 until numBands)
        pixelOutput.write(fillValue, bitsPerSample(b))
      pixelOutput.close()
      val emptyTileLength: Int = fileSystem.getFileStatus(emptyTilePath).getLen.toInt
      // We always append the empty tile so the offset is after the last tile
      var emptyTileOffset: Long = 0
      for (iTile <- tileOffsets.indices)
        emptyTileOffset = emptyTileOffset max (tileOffsets(iTile) + tileLengths(iTile))
      // Now, append the empty tile path to the list of paths and set its offset and length for all empty tiles
      for (iTile <- tileOffsets.indices; if tileLengths(iTile) == 0) {
        tileOffsets(iTile) = emptyTileOffset
        tileLengths(iTile) = emptyTileLength
      }
      tileDataPath = tileDataPath :+ emptyTilePath
    }
    // If the file has only one strip or tile, the offset and length should be encoded in the IFD Entry
    if (rasterMetadata.tileWidth == rasterMetadata.rasterWidth) {
      // Stripped file
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_STRIP_OFFSETS, TiffConstants.TYPE_LONG, tileOffsets))
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_STRIP_BYTE_COUNTS, TiffConstants.TYPE_LONG, tileLengths))
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_ROWS_PER_STRIP, TiffConstants.TYPE_LONG, rasterMetadata.tileHeight))
    } else {
      // Tiled file
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_TILE_OFFSETS, TiffConstants.TYPE_LONG, tileOffsets))
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_TILE_BYTE_COUNTS, TiffConstants.TYPE_LONG, tileLengths))
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_TILE_WIDTH, TiffConstants.TYPE_LONG, rasterMetadata.tileWidth))
      ifdEntries.append(makeIFDEntry(TiffConstants.TAG_TILE_LENGTH, TiffConstants.TYPE_LONG, rasterMetadata.tileHeight))
    }
    ifdEntries.append(makeIFDEntry(TiffConstants.TAG_PLANAR_CONFIGURATION, TiffConstants.TYPE_SHORT, 1))
    // Fill value
    ifdEntries.append(makeIFDEntry(GeoTiffConstants.GDALNoDataTag, TiffConstants.TYPE_ASCII, fillValue.toString))
    if (rasterMetadata.srid != 0) {
      val crs: CoordinateReferenceSystem = CRSServer.sridToCRS(rasterMetadata.srid)
      val adapter = new CRS2GeoTiffMetadataAdapter(crs)
      val data = adapter.parseCoordinateReferenceSystem()
      val entryData = new Array[Int](data.getNumGeoKeyEntries * 4)
      var doubleParams: Array[Double] = Array()
      var asciiParams: String = ""
      entryData(0) = data.getGeoTIFFVersion
      entryData(1) = data.getKeyRevisionMajor
      entryData(2) = data.getKeyRevisionMinor
      entryData(3) = data.getNumGeoKeyEntries - 1
      for (i <- 1 until data.getNumGeoKeyEntries) {
        val geoKeyEntry = data.getGeoKeyEntryAt(i)
        entryData(i * 4 + 0) = geoKeyEntry.getKeyID
        entryData(i * 4 + 1) = geoKeyEntry.getTiffTagLocation
        entryData(i * 4 + 2) = geoKeyEntry.getCount
        entryData(i * 4 + 3) = geoKeyEntry.getValueOffset
        if (geoKeyEntry.getTiffTagLocation.toShort == GeoTiffConstants.GeoDoubleParamsTag) {
          entryData(i * 4 + 3) = doubleParams.length
          doubleParams = doubleParams ++ data.getGeoDoubleParams(geoKeyEntry.getKeyID)
        }
        if (geoKeyEntry.getTiffTagLocation.toShort == GeoTiffConstants.GeoAsciiParamsTag) {
          entryData(i * 4 + 3) = asciiParams.length
          asciiParams = asciiParams + data.getGeoAsciiParam(geoKeyEntry.getKeyID)
        }
      }
      ifdEntries.append(makeIFDEntry(GeoTiffConstants.GeoKeyDirectoryTag, TiffConstants.TYPE_SHORT, entryData))
      if (doubleParams.nonEmpty)
        ifdEntries.append(makeIFDEntry(GeoTiffConstants.GeoDoubleParamsTag, TiffConstants.TYPE_DOUBLE, doubleParams))
      if (asciiParams.nonEmpty)
        ifdEntries.append(makeIFDEntry(GeoTiffConstants.GeoAsciiParamsTag, TiffConstants.TYPE_ASCII, asciiParams))
    }
    // Write Affine Transform
    if (rasterMetadata.g2m != null) {
      val affineTransform = rasterMetadata.g2m
      if (affineTransform.getShearX == 0.0 & affineTransform.getShearY == 0.0) {
        // The most common special case when no shearing is in the transformation matrix
        ifdEntries.append(makeIFDEntry(GeoTiffConstants.ModelTiepointTag, TiffConstants.TYPE_DOUBLE,
          Array(0.0, 0.0, 0.0, affineTransform.getTranslateX, affineTransform.getTranslateY, 0.0)))
        ifdEntries.append(makeIFDEntry(GeoTiffConstants.ModelPixelScaleTag, TiffConstants.TYPE_DOUBLE,
          Array(affineTransform.getScaleX, -affineTransform.getScaleY, 0.0)))
      } else {
        // TODO A full affine transformation, write all entries
        throw new RuntimeException("Full affine transformations not yet supported")
      }
    }
    // Sort by tag as required by TIFF specifications
    ifdEntries = ifdEntries.sortBy(_.tag & 0xffff)
    // The array of tile lengths in bytes. Used to calculate overall data size
    val lengthsArray: Array[Int] = ifdEntries
      .find(e => e.tag == TiffConstants.TAG_TILE_BYTE_COUNTS || e.tag == TiffConstants.TAG_STRIP_BYTE_COUNTS)
      .get.value.asInstanceOf[Array[Int]]

    // 2. Now, calculate the size of the header to assign offsets to IFD entries and adjust tile offsets in file
    val dataSize: Long = lengthsArray.map(_.toLong).sum
    var metadataSize: Long = if (bigTiff == "yes") calculateHeaderSizeBigTiff(ifdEntries) else calculateHeaderSizeNormalTiff(ifdEntries)
    val totalSize = metadataSize + dataSize
    if (totalSize > Int.MaxValue) {
      if (bigTiff == "no")
        throw new RuntimeException(s"Tiff file size $totalSize exceeds the maximum of ${Int.MaxValue} and BigTiff flag is set to '${bigTiff}'")
      // Set the big tiff flag and recalculate the header size
      if (bigTiff == "auto") {
        bigTiff = "yes"
        logInfo(s"Automatically switched to BigGeoTiff to handle an estimated file of size $totalSize")
      }
      // Update the type of the tile offset to 8 bytes
      for (i <- ifdEntries.indices) {
        if (ifdEntries(i).tag == TiffConstants.TAG_TILE_OFFSETS || ifdEntries(i).tag == TiffConstants.TAG_STRIP_OFFSETS)
          ifdEntries(i) = makeIFDEntry(ifdEntries(i).tag, TiffConstants.TYPE_LONG8, ifdEntries(i).value)
      }
      // Recalculate metadata size for Big TIFF
      metadataSize = calculateHeaderSizeBigTiff(ifdEntries)
    } else {
      // The file size is small. No need for Big TIFF
      if (bigTiff == "yes")
        logInfo("Unnecessarily writing a Big GeoTiff file")
      else if (bigTiff == "auto")
        bigTiff = "no"
    }
    // Update tile offsets based on the metadata size since tiles will be written after the header + metadata
    val offsetsArray: Array[Long] = ifdEntries
      .find(e => e.tag == TiffConstants.TAG_TILE_OFFSETS || e.tag == TiffConstants.TAG_STRIP_OFFSETS)
      .get.value.asInstanceOf[Array[Long]]
    // Update offsets only for tile with positive length. Tiles with zero length are invalid and should have offset=0
    // See: https://gdal.org/drivers/raster/gtiff.html#sparse-files
    for (tileID <- offsetsArray.indices; if lengthsArray(tileID) > 0)
      offsetsArray(tileID) = offsetsArray(tileID) + metadataSize

    val headerPath = new Path(outPath.toString + "_header")
    // 3. Write the actual file to the given output stream
    val headerOut = fileSystem.create(headerPath)
    if (bigTiff == "no")
      writeNormalHeader(headerOut, ifdEntries)
    else
      writeBigHeader(headerOut, ifdEntries)
    headerOut.close()

    // Concatenate both files
    FileUtil.concat(fileSystem, outPath, (headerPath +: tileDataPath):_*)
  }

  /**
   * Calculate the size of the Tiff header in bytes for the given list of entries
   * @param ifdEntries the list of entries to write in the header
   * @return the total size in bytes
   */
  private def calculateHeaderSizeNormalTiff(ifdEntries: mutable.ArrayBuffer[TempIFDEntry]): Long = {
    var headerSize: Long = 0
    headerSize = 8 // 2 byte ordering + Identifier (42) + offset of first IFD entry
    headerSize += 2 + ifdEntries.length * 12 + 4 // IFD Table
    for (entry <- ifdEntries) {
      val valueLength = entry.valueLength
      if (valueLength > 4) {
        entry.offset = headerSize
        headerSize += valueLength
      }
    }
    headerSize
  }

  private def writeNormalHeader(headerOut: FSDataOutputStream, ifdEntries: ArrayBuffer[TempIFDEntry]): Unit = {
    headerOut.writeShort(TiffConstants.BIG_ENDIAN)
    headerOut.writeShort(TiffConstants.SIGNATURE)
    headerOut.writeInt(8)
    // Write IFD Table
    headerOut.writeShort(ifdEntries.length)
    for (entry <- ifdEntries) {
      headerOut.writeShort(entry.tag)
      headerOut.writeShort(entry.valueType)
      headerOut.writeInt(entry.count)
      headerOut.writeInt(entry.getValueOrOffsetNormal())
    }
    headerOut.writeInt(0)
    // Write long values that do not fit in the IFDEntry itself
    for (entry <- ifdEntries) {
      if (entry.valueLength > 4) {
        assert(entry.offset != 0, "Unexpected zero offset for a big value")
        entry.valueType match {
          case TiffConstants.TYPE_ASCII =>
            headerOut.writeBytes(entry.value.asInstanceOf[String])
            headerOut.writeByte(0)
          case TiffConstants.TYPE_BYTE =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeByte(v)
          case TiffConstants.TYPE_SHORT =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeShort(v)
          case TiffConstants.TYPE_LONG if entry.value.isInstanceOf[Array[Int]] =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeInt(v)
          case TiffConstants.TYPE_LONG if entry.value.isInstanceOf[Array[Long]] =>
            for (v <- entry.value.asInstanceOf[Array[Long]])
              headerOut.writeInt(v.toInt)
          case TiffConstants.TYPE_DOUBLE =>
            for (v <- entry.value.asInstanceOf[Array[Double]])
              headerOut.writeDouble(v)
        }
      }
    }
  }

  private def writeBigHeader(headerOut: FSDataOutputStream, ifdEntries: ArrayBuffer[TempIFDEntry]): Unit = {
    headerOut.writeShort(TiffConstants.BIG_ENDIAN)
    headerOut.writeShort(TiffConstants.BIG_SIGNATURE)
    headerOut.writeShort(8) // 8 bytes per value
    headerOut.writeShort(0) // Always zero
    headerOut.writeLong(16) // Offset of first entry. Always immediately after the header in our case
    // Write IFD Table
    headerOut.writeLong(ifdEntries.length) // Number of entries
    for (entry <- ifdEntries) {
      headerOut.writeShort(entry.tag)
      headerOut.writeShort(entry.valueType)
      headerOut.writeLong(entry.count)
      headerOut.writeLong(entry.getValueOrOffsetBig())
    }
    headerOut.writeLong(0)
    // Write long values that do not fit in the IFDEntry itself
    for (entry <- ifdEntries) {
      if (entry.valueLength > 8) {
        assert(entry.offset != 0, "Unexpected zero offset for a big value")
        entry.valueType match {
          case TiffConstants.TYPE_ASCII =>
            headerOut.writeBytes(entry.value.asInstanceOf[String])
            headerOut.writeByte(0)
          case TiffConstants.TYPE_BYTE =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeByte(v)
          case TiffConstants.TYPE_SHORT =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeShort(v)
          case TiffConstants.TYPE_LONG if entry.value.isInstanceOf[Array[Int]] =>
            for (v <- entry.value.asInstanceOf[Array[Int]])
              headerOut.writeInt(v)
          case TiffConstants.TYPE_LONG if entry.value.isInstanceOf[Array[Long]] =>
            for (v <- entry.value.asInstanceOf[Array[Long]])
              headerOut.writeInt(v.toInt)
          case TiffConstants.TYPE_DOUBLE =>
            for (v <- entry.value.asInstanceOf[Array[Double]])
              headerOut.writeDouble(v)
          case TiffConstants.TYPE_LONG8 =>
            for (v <- entry.value.asInstanceOf[Array[Long]])
              headerOut.writeLong(v)
        }
      }
    }
  }
  /**
   * Calculate the size of the Tiff header in bytes for the given list of entries assuming writing in Big Tiff mode.
   * @param ifdEntries the list of entries to write in the header
   * @return the total size in bytes
   */
  private def calculateHeaderSizeBigTiff(ifdEntries: mutable.ArrayBuffer[TempIFDEntry]): Long = {
    var headerSize: Long = 0
    // 2-byte ordering + 2-byte Identifier (43) + 2-byte entry size (8) + 2-byte zero + 8-byte offset of first entry
    headerSize = 16
    // Tags table: 8-byte number of entries + 20 bytes per entry + 8-byte offset of next IFD or zero
    headerSize += 8 + ifdEntries.length * 20 + 8 // IFD Table
    for (entry <- ifdEntries) {
      val valueLength = entry.valueLength
      if (valueLength > 8) {
        entry.offset = headerSize
        headerSize += valueLength
      }
    }
    headerSize
  }


  /**
   * A temporary IFDEntry that is kept in memory before being written to disk
   *
   * @param tag       the numeric tag
   * @param valueType the type of the value stored in this entry
   * @param value     the values stored in this entry. The value must be one of two possibilities,
   *                  Array[Int] for all numeric values, Array[Double] for doubles, or String
   * @param offset    the offset of this entry in the file
   */
  case class TempIFDEntry(tag: Short, valueType: Short, value: Any, var offset: Long = 0) {
    require(value.isInstanceOf[Array[Int]] || value.isInstanceOf[Array[Long]] ||
      value.isInstanceOf[String] || value.isInstanceOf[Array[Double]])

    def count: Int = value match {
      case ar: Array[Int] => ar.length
      case ar: Array[Long] => ar.length
      case ar: Array[Double] => ar.length
      case str: String => str.length + 1
    }

    /** Total length of the value in bytes */
    def valueLength: Int = TiffConstants.TypeSizes(this.valueType) * count

    /** The value or offset that should be written to the file */
    def getValueOrOffsetNormal(bigEndian: Boolean = true): Int = {
      if (valueLength > 4) {
        offset.toInt
      } else value match {
        case ar: Array[Int] => IFDEntry.makeValue(valueType, ar, bigEndian)
        case ar: Array[Long] => IFDEntry.makeValue(valueType, ar.map(_.toInt), bigEndian)
        case str: String => IFDEntry.makeValue(valueType, (str.getBytes.map(_.toInt) :+ 0), bigEndian)
      }
    }

    /** The value or offset that should be written to the file for BigTiff file */
    def getValueOrOffsetBig(bigEndian: Boolean = true): Long = {
      if (valueLength > 8) {
        offset
      } else value match {
        case ar: Array[Int] => BigIFDEntry.makeValueBig(valueType, ar.map(_.toLong), bigEndian)
        case ar: Array[Long] => BigIFDEntry.makeValueBig(valueType, ar, bigEndian)
        case str: String => BigIFDEntry.makeValueBig(valueType, str.getBytes.map(_.toLong) :+ 0L, bigEndian)
      }
    }

    override def toString: String = {
      val valueString: String = value match {
        case s: String => s"'${s}'"
        case v: Array[Int] => s"[${v.mkString(",")}]"
        case ar: Array[Double] => s"[${ar.mkString(",")}]"
        case ar: Array[Long] => s"[${ar.mkString(",")}]"
      }
      val tagName: String = TiffConstants.TagNames.get(tag)
      s"IFDEntry #${tag} ${tagName} value ${valueString} offset ${offset}"
    }
  }
}
