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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.util.{FileUtil, IConfigurable, OperationException, OperationParam}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.TaskFailureListener

import java.util

/**
  * A helper class for writing the output
  */
object SpatialWriter extends IConfigurable with Logging {
  /** The configuration for the class name of the FeatureWriterClass */
  val FeatureWriterClass = "SpatialOutputFormat.FeatureWriterClass"
  @OperationParam(description = """The format of the input file {point(xcol,ycol),envelope(x1col,y1col,x2col,y2col),wkt(gcol)}
    point(xcol,ycol) indicates a CSV input where xcol and ycol indicate the indexes of the columns that" + "contain the x and y coordinates
    envelope(x1col,y1col,x2col,y2col) indicate an input that contains rectangles stores in (x1,y1,x2,y2) format
    wkt(gcol) indicate a CSV file with the field (gcol) containing a WKT-encoded geometry.
    shapefile: Esri shapefile. Accepts both .shp+.shx+.dbf files or a compressed .zip file with these three files
    rtree: An optimized R-tree index""")
  val OutputFormat = "oformat"

  @OperationParam(description = "Overwrite the output if it already exists {true, false}.", defaultValue = "false")
  val OverwriteOutput = "overwrite"

  @OperationParam(description = "If the RDD has one partition, it will write a single file to the output.", defaultValue = "false")
  val CompatibilityMode = "compatibility"

  /**
   * Get the feature writer class that corresponds to the given user-friendly short code.
   * @param oFormat the output format in a user-friendly way
   * @return the class of the feature writer that corresponds to the given user-friendly output format
   */
  def getFeatureWriterClass(oFormat: String): Class[_ <: FeatureWriter] = {
    if (CSVFeatureReader.recognize(oFormat))
      // CSVFeatureReader was able to detect the input format.
      classOf[CSVFeatureWriter]
    else
      FeatureWriter.featureWriters.get(oFormat)
  }

  /**
   * Returns the feature writer class configured in the given configuration.
   * It uses the following order to get the writer class:
   *
   *  - A class configured in [[SpatialWriter.FeatureWriterClass]]</li>
   *  - A short name in [[SpatialWriter# OutputFormat]]</li>
   *  - Assume input/output format of the same type and use [[SpatialFileRDD# InputFormat]]</li>
   *  - If none of these are set, a `null` is returned.</li>
   *
   * @param conf the job configuration
   * @return the class of the feature writer
   */
  def getConfiguredFeatureWriterClass(conf: Configuration): Class[_ <: FeatureWriter] = {
    var writerClass = conf.getClass(SpatialWriter.FeatureWriterClass, null, classOf[FeatureWriter])
    if (writerClass == null) { // Try short name for the output format
      val oFormat = conf.get(SpatialWriter.OutputFormat, conf.get(SpatialFileRDD.InputFormat))
      writerClass = FeatureWriter.featureWriters.get(oFormat)
      if (writerClass == null && CSVFeatureReader.recognize(oFormat)) { // CSVFeatureReader was able to detect the input format.
        writerClass = classOf[CSVFeatureWriter]
      }
      conf.setClass(SpatialWriter.FeatureWriterClass, writerClass, classOf[FeatureWriter])
    }
    writerClass
  }

  /**
   * Adds the FeatureWriter class assigned in the user options to the list of classes with parameters
   *
   * @param opts             user options
   * @param parameterClasses (output) the dependent classes will be added to this list
   */
  override def addDependentClasses(opts: BeastOptions, parameterClasses: util.Stack[Class[_]]): Unit = {
    if (opts == null) return
    var oFormat: String = opts.getString(SpatialWriter.OutputFormat, opts.getString(SpatialFileRDD.InputFormat))
    if (oFormat == null) return
    if (oFormat == "*auto*") {
      val detected: Tuple2[Class[_ <: FeatureReader], BeastOptions] = SpatialFileRDD.autodetectInputFormat(null, new BeastOptions)
      if (detected == null) throw new OperationException("Failed to auto-detect input format")
      val detectedOptions: BeastOptions = detected._2
      // If an input format is detected, use it also as an output format
      opts.mergeWith(detectedOptions)
      opts.set(SpatialWriter.OutputFormat, oFormat = opts.getString(SpatialFileRDD.InputFormat))
    }
    // TODO can we remove the next line to reflect that the output format was not explicitly set by the user?
    opts.set(SpatialWriter.OutputFormat, oFormat)
    val writerClass: Class[_ <: FeatureWriter] = opts.getClass(SpatialWriter.FeatureWriterClass, null, classOf[FeatureWriter])
    if (writerClass != null) parameterClasses.push(writerClass)
  }

  /**Java shortcut*/
  def saveFeaturesJ(features: JavaSpatialRDD,  oFormat: String, outPath: String, opts: BeastOptions): Unit =
    saveFeatures(features.rdd, oFormat, outPath, opts)

  /**
    * Saves the given set of features to the output using the provided output format.
    * @param features the set of features to store to the output
    * @param oFormat the output format to use for writing
    * @param outPath the path to write the output to
    * @param opts user options to configure the writer
    */
  def saveFeatures(features: SpatialRDD,  oFormat: String, outPath: String, opts: BeastOptions): Unit = {
    val _opts: BeastOptions = new BeastOptions(features.sparkContext.hadoopConfiguration).mergeWith(opts)
    _opts.set(OutputFormat, oFormat)
    val hadoopConf = _opts.loadIntoHadoopConf()
    // Delete existing output if overwrite flag is set
    val out: Path = new Path(outPath)
    if (opts.getBoolean(OverwriteOutput, false)) {
      val filesystem: FileSystem = out.getFileSystem(hadoopConf)
      if (filesystem.exists(out))
        filesystem.delete(out, true)
    }
    // Extract the spatial writer class
    val writerClass: Class[_ <: FeatureWriter] = getConfiguredFeatureWriterClass(hadoopConf)
    // Determine the extension of the output files
    val extension: String = writerClass.getConstructor().newInstance().getFileExtension(outPath, _opts)
    val numPartitions = features.getNumPartitions
    val files: Array[String] = features.sparkContext.runJob(features, (context: TaskContext, fs: Iterator[IFeature]) => if (fs.hasNext) {
      // Create a temporary directory for this task output
      val taskTempDir: Path = new Path(outPath, f"temp-${context.partitionId()}-${context.taskAttemptId()}")
      val fileSystem = taskTempDir.getFileSystem(_opts.loadIntoHadoopConf())
      context.addTaskFailureListener(new TaskFailureListener() {
        override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
          if (fileSystem.exists(taskTempDir))
            fileSystem.delete(taskTempDir, true)
        }
      })
      fileSystem.mkdirs(taskTempDir)
      val partitionId: Int = context.partitionId()
      // Initialize the feature writer
      val partitionFileName: String = f"part-$partitionId%05d$extension"
      val partitionFile: Path = new Path(taskTempDir, partitionFileName)
      val featureWriter = writerClass.newInstance()
      featureWriter.initialize(partitionFile, _opts.loadIntoHadoopConf())
      if (opts.getBoolean(CompatibilityMode, false) && numPartitions > 1 && featureWriter.isInstanceOf[MultipartFeatureWriter]) {
        // Special case when compatibility mode is requested and more than one partition is written and the writer support multipart write mode
        val multipartFeatureWriter: MultipartFeatureWriter = featureWriter.asInstanceOf[MultipartFeatureWriter]
        var firstRecord = partitionId == 0
        while (fs.hasNext) {
          val feature = fs.next()
          val lastRecord = (partitionId == numPartitions - 1) && !fs.hasNext
          if (firstRecord && lastRecord)
            multipartFeatureWriter.writeFirstLast(feature)
          else if (firstRecord)
            multipartFeatureWriter.writeFirst(feature)
          else if (lastRecord)
            multipartFeatureWriter.writeLast(feature)
          else
            multipartFeatureWriter.writeMiddle(feature)
          firstRecord = false
        }
      } else {
        for (feature <- fs)
          featureWriter.write(feature)
      }
      featureWriter.close()
      partitionFile.toString
      // Move all files from the temporary directory to the final output directory
      // Notice that some file formats, e.g., Shapefile, might produce more than one file so all should be moved
      val filesToMove = fileSystem.listStatus(taskTempDir).map(_.getPath)
      var finalFilePath: Path = null
      for (fileToMove <- filesToMove) {
        finalFilePath = new Path(outPath, fileToMove.getName)
        try {
          fileSystem.rename(fileToMove, finalFilePath)
        } catch {
          // Note: If the task fails at this point, the final output file will not be cleaned up
          // This will cause all subsequent attempts to fail because they will not be able to move their final output file
          // To address this issue, we absorb the corresponding IOException so that the subsequent tasks will
          // succeed even though they cannot move their final file.
          // We report this as a warning to double check that there is no mistake
          case e: java.io.IOException => logWarning("Moving the final file failed. Check SpatialWriter#saveFeatures", e)
        }
      }
      // Delete temporary directory (on success)
      if (fileSystem.exists(taskTempDir))
        fileSystem.delete(taskTempDir, true)
      // Return final file path
      finalFilePath.toString
    } else null)
    if (opts.getBoolean(CompatibilityMode, false)) {
      if (files.length > 1 && !classOf[MultipartFeatureWriter].isAssignableFrom(writerClass))
        logWarning("Compatibility mode is only usable when the SpatialRDD contains one partition. Ignoring!")
      else {
        val outDir = out.getParent
        val fileSystem = outDir.getFileSystem(_opts.loadIntoHadoopConf())
        var tempfile: Path = null
        do {
          tempfile = new Path(outDir, f"temp-${(Math.random() * 1000000).toInt}%05d")
        } while (fileSystem.exists(tempfile))
        if (files.length > 1) {
          // Concatenate all output files into a single file
          FileUtil.concat(fileSystem, tempfile, files.filter(_ != null).map(f => new Path(f)):_*)
          fileSystem.delete(out, true)
          fileSystem.rename(tempfile, out)
        } else {
          // Move all files in the output directory to the output
          fileSystem.rename(new Path(files(0)), tempfile)
          fileSystem.delete(out, true)
          fileSystem.rename(tempfile, out)
        }
      }
    }
  }
}
