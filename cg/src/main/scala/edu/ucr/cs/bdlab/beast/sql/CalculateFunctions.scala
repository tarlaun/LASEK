/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.sql

import org.locationtech.jts.geom.Geometry

object CalculateFunctions {
  /** Computes the area of the input geometry */
  val ST_Area: Geometry => Double = (geom: Geometry) => geom.getArea

  /** Calculates the length of the geometry */
  val ST_Length: Geometry => Double = (geom: Geometry) => geom.getLength

  /** Returns the dimension of the boundary (0 = point, 1 = line) */
  val ST_BoundaryDimension: Geometry => Int = (geom: Geometry) => geom.getBoundaryDimension

  /** Returns the dimension of this geometry (0 = point, 1 = line, 2 = polygon) */
  val ST_Dimension: Geometry => Int = (geom: Geometry) => geom.getDimension

  /** Returns the spatial reference identifier (SRID) associated with this geometry */
  val ST_SRID: Geometry => Int = (geom: Geometry) => geom.getSRID

  /** Computes the left bound of the input geometry */
  val ST_XMin: Geometry => Double = (geom: Geometry) => geom.getEnvelopeInternal.getMinX

  /** Computes the lower bound of the input geometry along the y-axis */
  val ST_YMin: Geometry => Double = (geom: Geometry) => geom.getEnvelopeInternal.getMinY

  /** Computes the right bound of the input geometry */
  val ST_XMax: Geometry => Double = (geom: Geometry) => geom.getEnvelopeInternal.getMaxX

  /** Computes the upper bound of the input geometry along the y-axis */
  val ST_YMax: Geometry => Double = (geom: Geometry) => geom.getEnvelopeInternal.getMaxY

  /** Computes the number of points of the input geometry */
  val ST_NumPoints: Geometry => Int = (geom: Geometry) => geom.getNumPoints

  /** Returns the number of sub-geometries within this geometry, or one if it is a single geometry */
  val ST_NumGeometries: Geometry => Int = (geom: Geometry) => geom.getNumGeometries

  /** Returns the nth geometry within this geometry */
  val ST_GeometryN: (Geometry, Int) => Geometry = (geom: Geometry, n: Int) => geom.getGeometryN(n)

  /**
   * Returns the type of the geometry {POINT, LINESTRING, POLYGON, MULTIPOINT,
   * MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION}
   */
  val ST_GeometryType: Geometry => String = (geom: Geometry) => geom.getGeometryType
}
