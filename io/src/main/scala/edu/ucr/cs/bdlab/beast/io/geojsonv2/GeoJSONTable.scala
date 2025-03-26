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

import edu.ucr.cs.bdlab.beast.io.SpatialFilePartitioner
import edu.ucr.cs.bdlab.beast.io.geojsonv2.GeoJSONTable.openFile
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.io.InputStream
import scala.collection.JavaConverters._

case class GeoJSONTable(name: String,
                        sparkSession: SparkSession,
                        options: CaseInsensitiveStringMap,
                        paths: Seq[String],
                        userSpecifiedSchema: Option[StructType],
                        fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) with Logging {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val t1 = System.nanoTime()
    try {
      if (files.isEmpty)
        return Option.empty
      // Hadoop Configurations are case sensitive.
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
      val fileSystem: FileSystem = files.head.getPath.getFileSystem(hadoopConf)
      // Infer the schema from the first GeoJSON file
      val geoJSONFiles = new SpatialFilePartitioner(fileSystem, files.map(_.getPath).iterator,
        extensions = GeoJSONTable.extensions, useMaster = true, splitFiles = _ => false)
      if (!geoJSONFiles.hasNext)
        return Option.empty
      val geoJSONFile = geoJSONFiles.next()
      // Read the file and infer schema from it
      val dataIn = openFile(fileSystem, new Path(geoJSONFile.filePath))
      try {
        val size = options.getLong(GeoJSONTable.GEOJSON_SCHEMA_SIZE.key, 1024 * 1024)
        val schema = GeoJSONReader.inferSchema(dataIn, size)
        Option(schema)
      } catch {
        case e: Exception =>
          logWarning(s"Error inferring schema from file '${geoJSONFile.filePath}'", e)
          Option.empty
      } finally {
        dataIn.close()
      }
    } finally {
      val t2 = System.nanoTime()
      logInfo(s"GeoJSON inferSchema took ${(t2-t1)*1E-9} seconds")
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    GeoJSONScanBuilder(sparkSession, paths.toArray, schema, dataSchema, options)


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

  override def formatName: String = "geojson"
}

object GeoJSONTable {
  /**
   * Opens the given file and decompresses it if it's compressed
   * @param fileSystem the file system at which the file exists
   * @param path the path of the file
   * @return an input stream that that points to the data in the file
   */
  def openFile(fileSystem: FileSystem, file: Path): InputStream = {
    val fileIn = fileSystem.open(file)
    val hadoopConf = fileSystem.getConf
    val codec = new CompressionCodecFactory(hadoopConf).getCodec(file)
    if (codec == null) {
      fileIn
    } else {
      codec.createInputStream(fileIn)
    }
  }

  val GEOJSON_SCHEMA_SIZE = buildConf("beast.sql.geojson.schema.size")
    .internal()
    .doc("Size in bytes to read from the beginning of the GeoJSON file to infer schema")
    .version("0.10.0")
    .longConf
    .createWithDefault(1024 * 10)

  /** The file extensions that would be automatically recognized as GeoJSON data files */
  val extensions: Array[String] =
    Array(".geojson", ".geojsonl", ".json", ".geojson.gz", ".geojson.bz2", ".json.bz2", ".json.gz")
}