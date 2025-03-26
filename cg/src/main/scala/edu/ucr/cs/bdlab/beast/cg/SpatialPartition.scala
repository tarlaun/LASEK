/*
 * Copyright 2021 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, GeometryType}
import edu.ucr.cs.bdlab.beast.synopses.Summary
import org.apache.spark.Partition

/**
 * A Spark partition that points to spatial data. In addition to the data, it caches some
 * statistics that help with optimizing the processing such as the MBR and number of records.
 */
trait SpatialPartition extends Partition {

  /** The minimum bounding rectangle of this partition */
  def mbr: EnvelopeNDLite

  /** Total number of features (records) in this partition */
  def numFeatures: Long

  /** Number of non-empty geometries in this partition */
  def numNonEmptyGeometries: Long

  /**Total number of points for all geometries*/
  def numPoints: Long

  /**Size in bytes*/
  def size: Long

  /**
   * The sum of side length along each dimension. Combined with numNonEmptyGeometries, it can be used
   * to compute average side length per dimension.
   */
  def sumSideLength: Array[Double]

  /**
   * The most inclusive geometry type for this partition. This can be interpreted as below.
   * - Empty: All geometries are empty
   * - Point: Contains at least one point and zero or more empty geometries
   * - LineString: Contains at least one linestring and zero or more empty geometries
   * - Polygon: Contains at least one polygon and zero or more empty geometries
   * - MultiPoint: Contains at least one multipoint, and zero or more point or empty geometry.
   * - MultiLineString: Contains at least one MultiLineString, and zero or more linestrings and empty geometries.
   * - MultiPolygon: Contains at least one MultiPolygon, and zero or more poylgons and empty geometries.
   * - GeometryCollection: Everything else, i.e., none of the above.
   */
  def geometryType: GeometryType

  def asSummary: Summary = {
    val summary = new Summary()
    summary.setCoordinateDimension(mbr.getCoordinateDimension)
    summary.set(mbr)
    summary.numFeatures = this.numFeatures
    summary.numNonEmptyGeometries = this.numNonEmptyGeometries
    summary.numPoints = this.numPoints
    summary.size = this.size
    summary.sumSideLength = this.sumSideLength
    summary.geometryType = this.geometryType
    summary
  }
}
