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
package edu.ucr.cs.bdlab.beast.io.gpxv2

import edu.ucr.cs.bdlab.beast.io.geojsonv2.{GeoJSONPartitionReaderFactory, GeoJSONTable}
import edu.ucr.cs.bdlab.beast.io.{SpatialFilePartition2, SpatialFilePartitioner}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class GPXScan(sparkSession: SparkSession,
                   files: Array[String],
                   dataSchema: StructType,
                   options: CaseInsensitiveStringMap)
  extends Scan with Batch with Logging {

  override def readSchema(): StructType = dataSchema

  override def planInputPartitions(): Array[InputPartition] = partitions.toArray

  private lazy val partitions: Array[SpatialFilePartition2] = {
    val t1 = System.nanoTime()
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    // Disable cache to ensure that it uses the given configuration including the [min, max] split size
    val path = new Path(files.head)
    val scheme = path.toUri.getScheme
    hadoopConf.setBoolean(s"fs.${if (scheme == null) "file" else scheme}.impl.disable.cache", true)
    val fileSystem: FileSystem = path.getFileSystem(hadoopConf)

    val p: Iterator[SpatialFilePartition2] = new SpatialFilePartitioner(fileSystem,
      files.map(p => new Path(p)).iterator,
      Array(".gpx"), recursive = true, skipHidden = true, useMaster = true, splitFiles = _ => false)
    val ret = p.toArray
    val t2 = System.nanoTime()
    logInfo(s"GPX created ${ret.length} partitions in ${(t2-t1)*1E-9} seconds")
    ret
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    GPXPartitionReaderFactory(sparkSession.sessionState.conf,
      sparkSession.sparkContext.getConf, broadcastedConf, dataSchema, options.asScala.toMap)
  }
}
