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

import edu.ucr.cs.bdlab.beast.io.SpatialFilePartition2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, Seekable}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, SplitCompressionInputStream, SplittableCompressionCodec}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.InputStream

case class GeoJSONPartitionReaderFactory(sqlConf: SQLConf,
                                         sparkConf: SparkConf,
                                         broadcastedConf: Broadcast[SerializableConfiguration],
                                         dataSchema: StructType,
                                         requiredSchema: StructType,
                                         parsedOptions: Map[String, String])
  extends PartitionReaderFactory with Logging {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val spartition = partition.asInstanceOf[SpatialFilePartition2]
    val conf: Configuration = broadcastedConf.value.value
    val path = new Path(spartition.filePath)
    val fileSystem = path.getFileSystem(conf)
    val fileIn: FSDataInputStream = fileSystem.open(path)
    val codec = new CompressionCodecFactory(conf).getCodec(path)
    var finalIn: InputStream = null
    var end: Long = spartition.offset + spartition.length
    var pos: Seekable = fileIn
    codec match {
      case null =>
        // No compression. Read the file directly
        fileIn.seek(spartition.offset)
        finalIn = fileIn
        end = spartition.length
        pos = null
      case sc: SplittableCompressionCodec =>
        // A codec that supports semi-random access
        val decompressor = CodecPool.getDecompressor(codec)
        val cIn: SplitCompressionInputStream =  sc.createInputStream(fileIn, decompressor, spartition.offset,
          spartition.offset + spartition.length, SplittableCompressionCodec.READ_MODE.BYBLOCK)
        end = cIn.getAdjustedEnd
        finalIn = cIn
        pos = cIn
      case _ =>
        // A non-splittable compression codec, need to decompress the entire file from the beginning
        val decompressor = CodecPool.getDecompressor(codec)
        finalIn = codec.createInputStream(fileIn, decompressor)
        // Read until the end of the file
        pos = finalIn.asInstanceOf[Seekable]
        end = Long.MaxValue
    }
    // Create the GeoJSONReader and wrap it in a PartitionReader
    val geoJSONReader = new GeoJSONReader(finalIn, dataSchema, pos, end)
    new PartitionReader[InternalRow]() {
      var currentValue: InternalRow = _
      override def next(): Boolean = {
        if (!geoJSONReader.hasNext) {
          currentValue = null
          false
        } else {
          currentValue = geoJSONReader.next()
          true
        }
      }

      override def get(): InternalRow = currentValue

      override def close(): Unit = geoJSONReader.close
    }
  }
}
