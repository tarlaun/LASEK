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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

case class SpatialFilePartition2(var index: Int, filePath: String, offset: Long, length: Long,
                                override val preferredLocations: Array[String], numFeatures: Long, numPoints: Long,
                                avgSideLength: Array[Double], mbr: EnvelopeNDLite)
  extends Partition with InputPartition {
  def end: Long = offset + length
}