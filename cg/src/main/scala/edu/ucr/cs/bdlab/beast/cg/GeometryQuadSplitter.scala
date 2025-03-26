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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.geolite.{Feature, GeometryReader}
import edu.ucr.cs.bdlab.beast.util.MathUtil
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.DoubleAccumulator
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Envelope, Geometry, Polygon}

import scala.collection.mutable

/**
 * An iterator that takes a single geometry as input and produces a sequence of geometries by recursively breaking
 * the geometry into four pieces until a threshold is met in terms of number of points. In other words, all produced
 * geometries must have at most the threshold number of points.
 */
class GeometryQuadSplitter(geometry: Geometry, threshold: Int) extends Iterator[Geometry] {
  val toBreak: mutable.ArrayBuffer[Geometry] = new mutable.ArrayBuffer[Geometry]()
  toBreak.append(geometry)

  override def hasNext: Boolean = toBreak.nonEmpty

  override def next(): Geometry = {
    var g: Geometry = toBreak.remove(toBreak.length - 1)
    while (g != null && (g.getNumPoints > threshold || g.getNumGeometries > 1)) {
      if (g.getNumGeometries > 1) {
        // Break down a multi geometry into individual geometries
        for (i <- 1 until g.getNumGeometries)
          toBreak.append(g.getGeometryN(i))
        g = g.getGeometryN(0)
      } else {
        // A cheap way to fix some invalid polygons to avoid throwing exceptions when splitting
        if (!g.isValid)
          g = g.union(g)
        // Split a complex geometry into simpler ones
        val e: Envelope = g.getEnvelopeInternal
        val centerX: Double = (e.getMinX + e.getMaxX) / 2
        val centerY: Double = (e.getMinY + e.getMaxY) / 2
        try {toBreak.append(g.intersection(g.getFactory.toGeometry(new Envelope(e.getMinX, centerX, e.getMinY, centerY))))}
        catch { case _: Exception => }
        try {toBreak.append(g.intersection(g.getFactory.toGeometry(new Envelope(centerX, e.getMaxX, e.getMinY, centerY))))}
        catch { case _: Exception => }
        try {toBreak.append(g.intersection(g.getFactory.toGeometry(new Envelope(e.getMinX, centerX, centerY, e.getMaxY))))}
        catch { case _: Exception => }
        try {g = g.intersection(g.getFactory.toGeometry(new Envelope(centerX, e.getMaxX, centerY, e.getMaxY)))}
        catch { case _: Exception => g = if (toBreak.isEmpty) null else toBreak.remove(toBreak.length - 1)}
      }
    }
    g
  }
}

object GeometryQuadSplitter {
  /**
   * Splits all geometries in the given RDD[IFeature] into a new RDD[IFeature] where geometries are broken down
   * using the quad split partitioning approach. If `keepBothGeometries` is set to `true`, the resulting features
   * will contain both geometries where the broken geometry appears first. If `keepBothGeometries` is set to `false`,
   * on the simplified geometry is produced and the original geometry is removed.
   * @param spatialRDD the original rdd to split
   * @param threshold the quad split threshold
   * @param keepBothGeometries if `true`, both geometries will be kept in the output. If `false` only the smaller
   *                           decomposed geometry will appear.
   * @return
   */
  def splitRDD(spatialRDD: SpatialRDD, threshold: Int, keepBothGeometries: Boolean): SpatialRDD = {
    spatialRDD.mapPartitions(features => {
      features.flatMap(feature => {
        val smallGeometries: Iterator[Geometry] = new GeometryQuadSplitter(feature.getGeometry, threshold)
        if (keepBothGeometries) {
          smallGeometries.map(g => {
            // Keep both geometries
            val values: Seq[Any] = g +: Row.unapplySeq(feature).get
            val schema: Seq[StructField] = StructField("geometry", GeometryDataType) +: feature.schema
            new Feature(values.toArray, StructType(schema))
          })
        } else {
          smallGeometries.map(g => {
            // Replace the original geometry with the simplified one
            Feature.create(feature, g)
          })
        }
      })
    }, true)
  }

  /** A rectangle that represents the easter hemisphere */
  val EasternHemisphere: Geometry = GeometryReader.DefaultGeometryFactory
    .toGeometry(new Envelope(0, 180, -90, 90))
  /** A rectangle that represents the western hemisphere */
  val WesternHemisphere: Geometry = GeometryReader.DefaultGeometryFactory
    .toGeometry(new Envelope(180, 360, -90, 90))

  /**
   * Splits the given geometry across the dateline (-180 or +180 meridian) to avoid errors.
   *  1. This function assumes that the input consists of a polygon with a single ring (outer shell).
   *  1. We assume that the width cannot be greater than 180 degrees.
   *  1. If the geometry width is greater than 180, we assume that it crosses the dateline.
   *  1. To fix the geometry, we rotate all negative longitudes by adding 360.
   *  1. After that, we split the geometry by intersecting it with the western hemisphere once and with the easter
   *     hemisphere once.
   * @param geometry the input geometry to detect and split
   * @return Either the same geometry if it does not cross the dateline, or the same one split into two if it
   *         crosses the dateline.
   */
  def splitGeometryAcrossDateLine(geometry: Geometry): Geometry = {
    if (geometry == null || geometry.isEmpty)
      return geometry
    require(geometry.getSRID == 4326, "Can only work with geometries in the EPSG:4326 format")
    require(geometry.getNumGeometries == 1, "splitGeometryAcrossDateLine expects a simple geometry")
    require(geometry.getGeometryType == "Polygon", "splitGeometryAcrossDateLine expects a polygon geometry")
    require(geometry.asInstanceOf[Polygon].getNumInteriorRing == 0, "splitGeometryAcrossDateLine expects a single ring")
    if (geometry.getEnvelopeInternal.getWidth <= 180) {
      // Geometry does not need to be split
      geometry
    } else {
      // Split the geometry
      val coords = geometry.getCoordinates
      for (i <- coords.indices) {
        if (coords(i).getX < 0)
          coords(i).setX(coords(i).getX + 360)
      }
      val polygon = geometry.getFactory.createPolygon(coords)
      val wPart: Polygon = polygon.intersection(WesternHemisphere).asInstanceOf[Polygon]
      val ePart: Polygon = polygon.intersection(EasternHemisphere).asInstanceOf[Polygon]
      val wPartCoords = wPart.getExteriorRing.getCoordinateSequence
      for (i <- 0 until wPartCoords.size())
        wPartCoords.setOrdinate(i, 0, wPartCoords.getOrdinate(i, 0) - 360)
      geometry.getFactory.createMultiPolygon(Array(wPart, ePart))
    }
  }
}