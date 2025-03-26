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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class GeoJSONScanBuilder(sparkSession: SparkSession,
                              files: Array[String],
                              schema: StructType,
                              dataSchema: StructType,
                              options: CaseInsensitiveStringMap)
  extends ScanBuilder with SupportsPushDownRequiredColumns {

  /** The columns needed by this reader */
  var requiredColumns: StructType = StructType(Seq())

  override def build(): Scan =
    GeoJSONScan(sparkSession,
      files,
      dataSchema,
      requiredColumns,
      options)

  override def pruneColumns(requiredSchema: StructType): Unit = this.requiredColumns = requiredSchema
}
