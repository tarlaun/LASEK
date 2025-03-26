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

object AnalysisFunctions {
  /** Computes the smallest convex Polygon of the input geometry */
  val ST_ConvexHull: Geometry => Geometry = (geom: Geometry) => geom.convexHull

  /** Computes the envelope (bounding box) of the input geometry */
  val ST_Envelope: Geometry => Geometry = (geom: Geometry) => geom.getEnvelope

  /** Computes the boundary of the input geometry */
  val ST_Boundary: Geometry => Geometry = (geom: Geometry) => geom.getBoundary

  /** Creates a new Geometry which is a normalized copy of the input one */
  val ST_Norm: Geometry => Geometry = (geom: Geometry) => geom.norm

  /**
   * Computes a new geometry which has all component coordinate sequences in reverse order (opposite orientation)
   * to the input one
   */
  val ST_Reverse: Geometry => Geometry = (geom: Geometry) => geom.reverse

  /** Computes the centroid of the input geometry */
  val ST_Centroid: Geometry => Geometry = (geom: Geometry) => geom.getCentroid

  /** Computes the intersection of two input geometries */
  val ST_Intersection: (Geometry, Geometry) => Geometry = (geom1: Geometry, geom2: Geometry) => geom1.intersection(geom2)

  /** Computes a Geometry representing the point-set which is contained in two input Geometries. */
  val ST_Union: (Geometry, Geometry) => Geometry = (geom1: Geometry, geom2: Geometry) => geom1.union(geom2)

  /**
   * Computes a Geometry representing the closure of the point-set of the points contained in the first input Geometry
   * that are not contained in the second one
   */
  val ST_Difference: (Geometry, Geometry) => Geometry = (geom1: Geometry, geom2: Geometry) => geom1.difference(geom2)
}
