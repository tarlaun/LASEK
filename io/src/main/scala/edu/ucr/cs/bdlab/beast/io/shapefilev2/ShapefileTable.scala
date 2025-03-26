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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.io.shapefilev2.ShapefileTable.geometryField
import edu.ucr.cs.bdlab.beast.io.{SpatialFilePartition2, SpatialFilePartitioner}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.zip.{ZipEntry, ZipInputStream}
import scala.collection.JavaConverters._

case class ShapefileTable (name: String,
                           sparkSession: SparkSession,
                           options: CaseInsensitiveStringMap,
                           paths: Seq[String],
                           userSpecifiedSchema: Option[StructType],
                           fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) with Logging {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ShapefileScanBuilder =
    ShapefileScanBuilder(sparkSession, paths.toArray, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val t1 = System.nanoTime()
    try {
      if (files.isEmpty)
        return Option.empty
      // Hadoop Configurations are case sensitive.
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
      val fileSystem: FileSystem = files.head.getPath.getFileSystem(hadoopConf)
      // Infer the schema from the Shapefile and DBF file
      val dbfAndZipFiles = new SpatialFilePartitioner(fileSystem, files.map(_.getPath).iterator,
        extensions = Array(".dbf", ".zip"), useMaster = true)
      if (!dbfAndZipFiles.hasNext)
        return Option.empty
      val firstFile: SpatialFilePartition2 = dbfAndZipFiles.next()
      // Load the file depending on its extension
      if (firstFile.filePath.toLowerCase.endsWith(".zip")) {
        // A compressed file. Open the first DBF file and use it to infer the schema
        val zipFile = new ZipInputStream(fileSystem.open(new Path(firstFile.filePath)))
        try {
          var zipEntry: ZipEntry = zipFile.getNextEntry
          while (zipEntry != null && !zipEntry.getName.toLowerCase.endsWith(".dbf"))
            zipEntry = zipFile.getNextEntry
          // If no DBF entry is found, return a schema with a geometry type only
          if (zipEntry == null)
            return Option(StructType(Seq(geometryField)))
          // Get the schema from the DBF file
          val dbfSchema = DBFHelper.inferSchema(zipFile)
          Option(StructType(geometryField +: dbfSchema))
        } finally {
          zipFile.close()
        }
      } else if (firstFile.filePath.toLowerCase.endsWith(".dbf")) {
        // Infer schema from a DBF file
        val dbfFile = fileSystem.open(new Path(firstFile.filePath))
        try {
          val dbfSchema = DBFHelper.inferSchema(dbfFile)
          Option(StructType(geometryField +: dbfSchema))
        } finally {
          dbfFile.close()
        }
      } else {
        throw new RuntimeException(s"Cannot infer schema from file '${firstFile.filePath}'")
      }
    } finally {
      val t2 = System.nanoTime()
      logInfo(s"ShapefileTable inferSchema took ${(t2-t1)*1E-9} seconds")
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

  override def formatName: String = "shapefile"
}

object ShapefileTable {
  val geometryField: StructField = StructField("geometry", GeometryDataType)
}