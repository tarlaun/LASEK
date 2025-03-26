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
package edu.ucr.cs.bdlab.beast.io.geojsonv2

import edu.ucr.cs.bdlab.beast.util.FileUtil
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import java.io.OutputStream

class GeoJSONFormat extends FileFormat with DataSourceRegister {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = ???

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = new OutputWriterFactory {
    var compressionExtension: String = _

    override def getFileExtension(context: TaskAttemptContext): String = {
      if (compressionExtension == null) {
        compressionExtension = CodecStreams.getCompressionExtension(context)
        if (compressionExtension.isEmpty) {
          // Try to get from the file path
          val path: String = if (options.contains("path"))
            options("path")
          else
            options("paths")
          val extension = FileUtil.getExtension(path)
          if (extension == ".bz2" || extension == ".gz")
            compressionExtension = extension
        }
      }
      ".geojson" + compressionExtension
    }

    override def newInstance(filepath: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
      val outPath = new Path(filepath)
      val conf = context.getConfiguration
      val fileSystem = outPath.getFileSystem(conf)
      var out: OutputStream = fileSystem.create(outPath)
      if (compressionExtension != null) {
        val codecFactory = new CompressionCodecFactory(conf)
        val codec = codecFactory.getCodec(new Path(filepath))
        if (codec != null)
          out = codec.createOutputStream(out)
      }
      val geojsonWriter = new GeoJSONWriter(out, dataSchema)
      new OutputWriter {
        override def write(row: InternalRow): Unit = geojsonWriter.write(row)

        override def close(): Unit = geojsonWriter.close()

        override def path(): String = filepath
      }
    }
  }

  override def shortName(): String = "geojson"
}
