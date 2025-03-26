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

import org.apache.spark.sql.internal.SQLConf.buildConf

import java.io.DataInputStream


/**
 * Helper functions for Shapefile reading in Data Source API
 */
object ShapefileHelper {
  val SHAPEFILE_FILTER_PUSHDOWN_ENABLED = buildConf("beast.sql.shapefile.filterPushdown.enabled")
    .doc("When true, enable filter pushdown to Shapefile datasource.")
    .version("0.10.0")
    .booleanConf
    .createWithDefault(false)

  val SHAPEFILE_PARSER_COLUMN_PRUNING = buildConf("beast.sql.shapefile.columnPruning.enabled")
    .internal()
    .doc("If it is set to true, column names of the requested schema are passed to Shapefile parser. " +
      "Other column values can be ignored during parsing even if they are malformed.")
    .version("0.10.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * Read the offsets from a .shx file
   * @param in the input to the .shx file
   * @return array of offsets for the file
   */
  def readSHXFile(in: DataInputStream): Array[Int] = {
    in.skipBytes(24)
    val fileLength: Int = in.readInt()
    val numRecords = (fileLength * 2 - 100) / 8
    // Skip the rest of the header
    in.skipBytes(100 - 24 - 4)
    val offsets = new Array[Int](numRecords)
    for (i <- offsets.indices) {
      offsets(i) = in.readInt() * 2
      in.skipBytes(4)
    }
    offsets
  }

  def numOfRecordsFromSHXFile(in: DataInputStream): Int = {
    in.skipBytes(24)
    val fileLength: Int = in.readInt()
    (fileLength * 2 - 100) / 8
  }
}
