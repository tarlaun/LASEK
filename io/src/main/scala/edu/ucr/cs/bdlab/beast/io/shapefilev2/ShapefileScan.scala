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

import edu.ucr.cs.bdlab.beast.io.{SpatialFilePartition2, SpatialFilePartitioner, SupportsSpatialFilterPushDown}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.locationtech.jts.geom.Envelope

import java.util.OptionalLong
import scala.collection.JavaConverters._

case class ShapefileScan(sparkSession: SparkSession,
                         files: Array[String],
                         dataSchema: StructType,
                         requiredColumns: StructType,
                         options: CaseInsensitiveStringMap,
                         partitionFilters: Seq[Expression] = Seq.empty,
                         dataFilters: Seq[Expression] = Seq.empty)
  extends Scan with Batch with SupportsReportStatistics with SupportsSpatialFilterPushDown with Logging {

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    ShapefilePartitionReaderFactory(sparkSession.sessionState.conf,
      sparkSession.sparkContext.getConf,
      broadcastedConf,
      dataSchema, requiredColumns, options.asScala.toMap, spatialFilter)
  }

  override def toBatch: Batch = this

  override def equals(obj: Any): Boolean = obj match {
    case c: ShapefileScan => super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      spatialFilter == c.spatialFilter
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def description(): String = {
    super.description()
  }

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }

  // TODO apply column pruning, if possible
  override def readSchema(): StructType = dataSchema

  private lazy val partitions: Array[SpatialFilePartition2] = {
    val t1 = System.nanoTime()
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val fileSystem: FileSystem = new Path(files.head).getFileSystem(hadoopConf)

    var p: Iterator[SpatialFilePartition2] = new SpatialFilePartitioner(fileSystem, files.map(p => new Path(p)).iterator,
      Array(".shp", ".zip"), recursive = true, skipHidden = true, useMaster = true, splitFiles = _ => false)
    if (spatialFilter != null)
      p = p.filter(x => x.mbr == null || x.mbr.intersectsEnvelope(spatialFilter))
    val ret = p.toArray
    val t2 = System.nanoTime()
    logInfo(s"Shapefile created ${ret.length} partitions in ${(t2-t1)*1E-9} seconds")
    ret
  }

  override def planInputPartitions(): Array[InputPartition] = partitions.toArray

  override def estimateStatistics(): Statistics = {
    val length = if (partitions.forall(_.length > 0)) partitions.map(_.length).sum else -1
    val numRecords = if (partitions.forall(_.numFeatures > 0)) partitions.map(_.numFeatures).sum else -1
    new Statistics {
      override def sizeInBytes(): OptionalLong = OptionalLong.of(length)

      override def numRows(): OptionalLong = OptionalLong.of(numRecords)
    }
  }

  /** A geometry filter that was pushed down */
  var spatialFilter: Envelope = _

  override def pushDownSpatialFilter(envelope: Envelope): Unit = {
    logInfo(s"Spatial push-down filter ${envelope}")
    this.spatialFilter = envelope
  }
}

