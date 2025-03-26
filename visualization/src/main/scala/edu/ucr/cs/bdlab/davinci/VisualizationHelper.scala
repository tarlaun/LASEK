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
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.geolite.{EmptyGeometry, EnvelopeND, Feature, PointND}
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryCollection, LineString, LinearRing, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}

/**
 * Helper functions for visualization
 */
object VisualizationHelper {

  /**
   * Converts the given features to web mercator and removes or clips any geometry to fit within the boundaries
   * of the web mercator space.
   * @param features the input features to convert
   * @return the converted features
   */
  def toWebMercator(features: SpatialRDD): SpatialRDD = {
    var transformedFeatures: SpatialRDD = features
    val transformationInfo = Reprojector.findTransformationInfo(features.first().getGeometry.getSRID,
      CommonVisualizationHelper.MercatorCRS)
    if (transformationInfo.sourceSRID == 4326) {
      // Clip and remove geometries based on the web mercator space
      // Not doing that could result in a transformation exception for geometries very close to the poles
      transformedFeatures = transformedFeatures.map { feature =>
        try {
          val clippedG: Geometry = feature.getGeometry.intersection(CommonVisualizationHelper.MercatorMapBoundariesPolygon)
          Feature.create(feature, clippedG)
        } catch {
          case _: Throwable => Feature.create(feature, EmptyGeometry.instance)
        }
      }
        .filter(!_.getGeometry.isEmpty)
        .asInstanceOf[SpatialRDD]
    }
    if (transformationInfo.sourceSRID != 3857) {
      transformedFeatures = transformedFeatures.map( feature => {
        val transformedG: Geometry = Reprojector.reprojectGeometry(feature.getGeometry, transformationInfo)
        Feature.create(feature, transformedG)
      })
    }
    transformedFeatures
  }

  /**
   * Flip all geometries vertically by negating the y-coordinate. This is needed to ensures that the y-coordinate
   * increases from bottom to top (map style) and not from top to bottom (screen or image style)
   * @param features
   * @return
   */
  def verticalFlip(features: SpatialRDD): SpatialRDD = features.map(feature => {
      val flippedGeometry: Geometry = flipGeometry(feature.getGeometry)
      Feature.create(feature, flippedGeometry)
    })

  /**
   * Flip a geometry along the y-axis by negating the y-coordinate.
   * @param geometry the geometry to flip vertically (treated as immutable)
   * @return a new geometry after flipping
   */
  def flipGeometry(geometry: Geometry): Geometry = geometry match {
    case p: Point => {
      val coordinate: Coordinate = p.getCoordinate.copy()
      coordinate.y = -coordinate.y
      geometry.getFactory.createPoint(coordinate)
    }
    case p: PointND => {
      val coordinates = new Array[Double](p.getCoordinateDimension)
      for (d <- 0 until p.getCoordinateDimension)
        coordinates(d) = p.getCoordinate(d)
      coordinates(1) = -coordinates(1)
      new PointND(geometry.getFactory, coordinates.length, coordinates:_*)
    }
    case e: EnvelopeND => {
      val minCoords = new Array[Double](e.getCoordinateDimension)
      val maxCoords = new Array[Double](e.getCoordinateDimension)
      for (d <- 0 until e.getCoordinateDimension) {
        minCoords(d) = e.getMinCoord(d)
        maxCoords(d) = e.getMaxCoord(d)
      }
      // Flip and negate the y-coordinates (because the minimum will become the maximum after negation)
      val temp: Double = minCoords(1)
      minCoords(1) = -maxCoords(1)
      maxCoords(1) = -temp
      new EnvelopeND(geometry.getFactory, minCoords, maxCoords)
    }
    case lineString: LineString => {
      val coordinates: CoordinateSequence = lineString.getCoordinateSequence.copy()
      for (i <- 0 until coordinates.size())
        coordinates.setOrdinate(i, 1, -coordinates.getOrdinate(i, 1))
      lineString match {
        case _: LinearRing => geometry.getFactory.createLinearRing(coordinates)
        case _: LineString => geometry.getFactory.createLineString(coordinates)
      }
    }
    case polygon: Polygon => {
      val exteriorRing: LinearRing = flipGeometry(polygon.getExteriorRing).asInstanceOf[LinearRing]
      val interiorRings: Array[LinearRing] = new Array[LinearRing](polygon.getNumInteriorRing)
      for (iRing <- interiorRings.indices)
        interiorRings(iRing) = flipGeometry(polygon.getInteriorRingN(iRing)).asInstanceOf[LinearRing]
      geometry.getFactory.createPolygon(exteriorRing, interiorRings)
    }
    case multiPoint: MultiPoint => {
      val points = new Array[Point](geometry.getNumGeometries)
      for (i <- 0 until geometry.getNumGeometries)
        points(i) = flipGeometry(geometry.getGeometryN(i)).asInstanceOf[Point]
      geometry.getFactory.createMultiPoint(points)
    }
    case multiLineString: MultiLineString => {
      val lineStrings = new Array[LineString](geometry.getNumGeometries)
      for (i <- 0 until geometry.getNumGeometries)
        lineStrings(i) = flipGeometry(geometry.getGeometryN(i)).asInstanceOf[LineString]
      geometry.getFactory.createMultiLineString(lineStrings)
    }
    case multiPolygon: MultiPolygon => {
      val polygons = new Array[Polygon](geometry.getNumGeometries)
      for (i <- 0 until geometry.getNumGeometries)
        polygons(i) = flipGeometry(geometry.getGeometryN(i)).asInstanceOf[Polygon]
      geometry.getFactory.createMultiPolygon(polygons)
    }
    case geometryCollection: GeometryCollection => {
      val geometries = new Array[Geometry](geometry.getNumGeometries)
      for (i <- 0 until geometry.getNumGeometries)
        geometries(i) = flipGeometry(geometry.getGeometryN(i))
      geometry.getFactory.createGeometryCollection(geometries)
    }
  }
}
