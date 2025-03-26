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
package edu.ucr.cs.bdlab.beast.io.kmlv2

import edu.ucr.cs.bdlab.beast.util.FileUtil
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

class KMLFormat extends FileFormat with DataSourceRegister {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = ???

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = new OutputWriterFactory {

    override def getFileExtension(context: TaskAttemptContext): String = ".kml"

    override def newInstance(filepath: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
      val outPath = new Path(filepath)
      val conf = context.getConfiguration
      val fileSystem = outPath.getFileSystem(conf)
      val out: OutputStream = fileSystem.create(outPath)
      val kmlWriter = new KMLWriter(out, dataSchema)
      new OutputWriter {
        override def write(row: InternalRow): Unit = kmlWriter.write(row)

        override def close(): Unit = kmlWriter.close()

        override def path(): String = filepath
      }
    }
  }

  override def shortName(): String = "kml"
}
