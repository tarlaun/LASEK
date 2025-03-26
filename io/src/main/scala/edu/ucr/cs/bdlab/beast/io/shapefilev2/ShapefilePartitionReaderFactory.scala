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

import edu.ucr.cs.bdlab.beast.io.SpatialFilePartition2
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.locationtech.jts.geom.Envelope

case class ShapefilePartitionReaderFactory(sqlConf: SQLConf,
                                           sparkConf: SparkConf,
                                           broadcastedConf: Broadcast[SerializableConfiguration],
                                           dataSchema: StructType,
                                           requiredSchema: StructType,
                                           parsedOptions: Map[String, String],
                                           filter: Envelope) extends PartitionReaderFactory with Logging {
  private val columnPruning: Boolean = sqlConf.getConf(ShapefileHelper.SHAPEFILE_PARSER_COLUMN_PRUNING)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val spartition = partition.asInstanceOf[SpatialFilePartition2]
    val conf: Configuration = broadcastedConf.value.value
    val skipSHPFile: Boolean = columnPruning && (requiredSchema.isEmpty || requiredSchema(0) != dataSchema(0))
    val skipDBFFile: Boolean = columnPruning && (requiredSchema.isEmpty || requiredSchema == StructType(Seq(dataSchema(0))))
    logInfo(s"Skip Shapefile? $skipSHPFile. Skip DBF file? $skipDBFFile")
    if (spartition.filePath.toLowerCase.endsWith(".zip"))
      new CompressedShapefileReader(conf, spartition, filter, skipSHPFile, skipDBFFile)
    else
      new ShapefileReader(conf, spartition, filter, skipSHPFile, skipDBFFile)
  }
}
