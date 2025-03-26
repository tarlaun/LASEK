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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.geolite._
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.geotools.referencing.CRS
import org.geotools.referencing.CRS.AxisOrder
import org.geotools.referencing.operation.projection.ProjectionException
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform, IdentityTransform}
import org.locationtech.jts.geom._
import org.opengis.metadata.extent.GeographicBoundingBox
import org.opengis.referencing.crs.{CoordinateReferenceSystem, ProjectedCRS}
import org.opengis.referencing.operation.{MathTransform, MathTransform2D, TransformException}

import java.awt.geom.AffineTransform

object Reprojector extends Logging {

  /**
   * A class that stores information for transforming geometries
   *
   * @param sourceCRS the source CRS object. Used to determine the axis order.
   * @param targetCRS the target CRS object. Used to determine the axis order.
   * @param sourceSRID the source SRID. Used to double check that it matches the geometry.
   * @param targetSRID the target SRID. Can be used to set the target SRID.
   * @param targetFactory the geometry factory for target geometries. Used to ensure that all derivative geometries
   *                      will have the targetSRID
   */
  case class TransformationInfo(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem,
                                sourceSRID: Int, targetSRID: Int,
                                mathTransform: MathTransform, targetFactory: GeometryFactory) {
    /** The extents of the source CRS in the coordinate space of the source CRS */
    lazy val sourceBounds: Envelope = {
      if (targetCRS == null || sourceCRS == null) {
        null
      }else if (targetCRS.getDomainOfValidity == null) {
        null
      } else if (targetSRID == 3857) {
        // Special case for WebMercator to compute the bounds more accurately than provided by GeoTools
        // The calculation below ensures square extends which make for accurate visualization
        val mercatorMaxLatitudeRadians: Double = 2 * Math.atan(Math.exp(Math.PI)) - Math.PI / 2
        val mercatorMaxLatitudeDegrees: Double = Math.toDegrees(mercatorMaxLatitudeRadians)
        val bounds = Array[Double](-180, -mercatorMaxLatitudeDegrees, +180, +mercatorMaxLatitudeDegrees)
        Reprojector.reprojectEnvelopeInPlaceNoCheck(bounds, 4326, sourceSRID)
        new Envelope(bounds(0), bounds(2), bounds(1), bounds(3))
      } else {
        val extents = targetCRS.getDomainOfValidity.getGeographicElements.iterator().next()
          .asInstanceOf[GeographicBoundingBox]
        val bounds = Array[Double](extents.getWestBoundLongitude, extents.getSouthBoundLatitude,
          extents.getEastBoundLongitude, extents.getNorthBoundLatitude)
        if (targetCRS.isInstanceOf[ProjectedCRS]) {
          // Handle an issue with Landsat data
          // Landsat uses northern UTM projections to store southern tiles.
          // This means that the entire raster will fall outside its own region
          // To handle this issue, we extend the northern UTM to cover the southern hemisphere as well
          if (bounds(1) == 0)
            bounds(1) = -bounds(3)
          else if (bounds(3) == 0)
            bounds(3) = -bounds(1)
        }
        if (sourceCRS.getDomainOfValidity != null) {
          // Further limit it to the source CRS domain of validity
          val extents2 = sourceCRS.getDomainOfValidity.getGeographicElements.iterator().next()
            .asInstanceOf[GeographicBoundingBox]
          val bounds2 = Array[Double](extents2.getWestBoundLongitude, extents2.getSouthBoundLatitude,
            extents2.getEastBoundLongitude, extents2.getNorthBoundLatitude)
          if (sourceCRS.isInstanceOf[ProjectedCRS]) {
            // Handle an issue with Landsat data
            // Landsat uses northern UTM projections to store southern tiles.
            // This means that the entire raster will fall outside its own region
            // To handle this issue, we extend the northern UTM to cover the southern hemisphere as well
            if (bounds2(1) == 0)
              bounds2(1) = -bounds2(3)
            else if (bounds2(3) == 0)
              bounds2(3) = -bounds2(1)
          }
          bounds(0) = bounds(0) max bounds2(0)
          bounds(1) = bounds(1) max bounds2(1)
          bounds(2) = bounds(2) min bounds2(2)
          bounds(3) = bounds(3) min bounds2(3)
        }
        try {
          var keyPoints = bounds
          if (keyPoints(1) < 0 && keyPoints(3) > 0) {
            // If the bounds cross the equator, split it across the equator
            // This ensures that the projected bounds will not miss any region.
            // Check how the bounds of EPSG:32608 will look like when converted to EPSG:4326
            keyPoints ++= Array(keyPoints(0), 0, keyPoints(1), 0)
          }
          Reprojector.reprojectEnvelopeInPlaceNoCheck(keyPoints, 4326, sourceSRID)
          if (keyPoints.length > 4) {
            // Calculate the minimum and maximum across all key points
            bounds(0) = keyPoints(0)
            bounds(2) = keyPoints(0)
            bounds(1) = keyPoints(1)
            bounds(3) = keyPoints(1)
            for (i <- 2 until keyPoints.length by 2) {
              bounds(0) = bounds(0) min keyPoints(i)
              bounds(2) = bounds(2) max keyPoints(i)
              bounds(1) = bounds(1) min keyPoints(i+1)
              bounds(3) = bounds(3) max keyPoints(i+1)
            }
          }
          new Envelope(bounds(0), bounds(2), bounds(1), bounds(3))
        } catch {
          case _: Exception => null
        }
      }
    }

    /**
     * A MathTransform that snaps points in the source CRS that are outside the source bounds (domain of validity)
     * to the nearest points on the bounds.
     * @return
     */
    def sourceSnap: MathTransform = {
      new SnapTransform(sourceBounds)
    }
  }

  lazy val CachedTransformationInfo: scala.collection.mutable.HashMap[(Int, Int), TransformationInfo] =
    scala.collection.mutable.HashMap.empty[(Int, Int), TransformationInfo]

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceCRS source coordinate reference system
   * @param targetCRS target coordinate reference system
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem): TransformationInfo = {
    if (sourceCRS == null || targetCRS == null)
      return null
    val sourceSRID = CRSServer.crsToSRID(sourceCRS)
    val targetSRID = CRSServer.crsToSRID(targetCRS)
    if (sourceSRID == 0 || targetSRID == 0) {
      // Do not use cache for invalid SRID
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        calculateMathTransform(sourceCRS, targetCRS), GeometryReader.getGeometryFactory(targetSRID))
    } else {
      val key = (sourceSRID, targetSRID)
      CachedTransformationInfo.getOrElseUpdate(key, {
        TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
          calculateMathTransform(sourceCRS, targetCRS), GeometryReader.getGeometryFactory(targetSRID))
      })
    }
  }

  /**
   * Calculate the [[MathTransform]] that converts from the source to the target CRS.
   * This method does not use any caching and directly calculates the transform.
   * Furthermore, this method will arrange the order of the axes between the source and target CRS.
   * @param sourceCRS the source [[CoordinateReferenceSystem]]
   * @param targetCRS the target [[CoordinateReferenceSystem]]
   * @return a [[MathTransform]] that can convert a coordinate from the source to the destination
   */
  private def calculateMathTransform(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem): MathTransform = {
    def isLatitudeFirst(crs: CoordinateReferenceSystem): Boolean = CRS.getAxisOrder(crs) == AxisOrder.NORTH_EAST

    var t: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS, true)
    if (isLatitudeFirst(sourceCRS))
      t = ConcatenatedTransform.create(reverseAxesTransform, t)
    if (isLatitudeFirst(targetCRS))
      t = ConcatenatedTransform.create(t, reverseAxesTransform)
    t
  }

  /**An affine transform that reverses x and y coordinates*/
  val reverseAxesTransform: MathTransform = new AffineTransform2D(0, 1, 1, 0, 0, 0)

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceSRID the SRID of the source coordinate reference system (EPSG:sourceSRID)
   * @param targetSRID the SRID of the target coordinate reference system (EPSG:sourceSRID)
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceSRID: Int, targetSRID: Int): TransformationInfo = {
    if (sourceSRID == targetSRID)
      return TransformationInfo(null, null, sourceSRID, targetSRID, IdentityTransform.create(2),
        GeometryReader.getGeometryFactory(targetSRID))
    val key = (sourceSRID, targetSRID)
    CachedTransformationInfo.getOrElseUpdate(key, {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID)
      val targetCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(targetSRID)
      val transform: MathTransform = calculateMathTransform(sourceCRS, targetCRS)
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID, transform,
        GeometryReader.getGeometryFactory(targetSRID))
    })
  }

  /**
   * Creates or retrieves a cached math transform to transform between the given two CRS
   * @param sourceSRID the SRID of the source coordinate reference system (EPSG:sourceSRID)
   * @param targetCRS target coordinate reference system
   * @return the math transformation that transforms from source to destination
   */
  def findTransformationInfo(sourceSRID: Int, targetCRS: CoordinateReferenceSystem): TransformationInfo = {
    val targetSRID: Integer = CRS.lookupEpsgCode(targetCRS, false)
    if (targetSRID == null) {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID)
      val transform: MathTransform = calculateMathTransform(sourceCRS, targetCRS)
      return TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        transform, GeometryReader.getGeometryFactory(targetSRID))
    }
    val key = (sourceSRID, targetSRID.toInt)
    CachedTransformationInfo.getOrElseUpdate(key, {
      val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceSRID)
      val transform: MathTransform = calculateMathTransform(sourceCRS, targetCRS)
      TransformationInfo(sourceCRS, targetCRS, sourceSRID, targetSRID,
        transform, GeometryReader.getGeometryFactory(targetSRID))
    })
  }

  /**
   * Reproject the given RDD to the target CRS. The source CRS is retrieved from the first element of the source RDD.
   * @param sourceRDD the RDD to transform
   * @param targetCRS the target Coordinate Reference System
   * @return the transformed RDD
   */
  def reprojectRDD(sourceRDD: SpatialRDD, targetCRS: CoordinateReferenceSystem): SpatialRDD = {
    val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(sourceRDD.first().getGeometry.getSRID)
    val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS)
    reprojectRDD(sourceRDD, transform)
  }

  /**
   * Reproject the given RDD using the provided transformation info. This method ignores (overrides) the CRS of the
   * source RDD.
   * @param sourceRDD the RDD to transform
   * @param transform the transformation info
   * @return the transformed RDD
   */
  def reprojectRDD(sourceRDD: SpatialRDD, transform: TransformationInfo): SpatialRDD = {
    val result: SpatialRDD = sourceRDD.map(f => {
      val transformedGeometry =
        try {
          Reprojector.reprojectGeometry(f.getGeometry, transform)
        } catch {
          // Replace geometries in error with an empty geometry
          case e: ProjectionException =>
            logWarning("Projection error", e)
            EmptyGeometry.instance
        }
      Feature.create(f, transformedGeometry)
    })
    result.filter(!_.getGeometry.isEmpty)
  }
  /**
   * Reprojects the given geometry from source to target CRS. This method ignores the SRID of the geometry and
   * assumes it to be in the source CRS.
   * @param geometry the geometry to transform
   * @param sourceCRS source coordinate reference system
   * @param targetCRS target coordinate reference system
   * @return a new geometry that is transformed
   */
  def reprojectGeometry(geometry: Geometry, sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem): Geometry =
    reprojectGeometry(geometry, findTransformationInfo(sourceCRS, targetCRS))

  /**
   * Reprojects the given geometry to target CRS. This method uses the SRID in the given geometry to determine
   * the source coordinate reference system. If the SRID is invalid, this method will fail.
   * @param geometry the geometry to transform
   * @param targetCRS target coordinate reference system
   * @return a new geometry that is transformed
   */
  def reprojectGeometry(geometry: Geometry, targetCRS: CoordinateReferenceSystem): Geometry =
    reprojectGeometry(geometry, CRSServer.sridToCRS(geometry.getSRID), targetCRS)

  /**
   * Reprojects the given geometry to the target SRID from the SRID encoded in the geometry.
   * @param geometry the geometry to transform
   * @param targetSRID the SRID of the target CRS
   * @return either the same geometry if no need to convert or a new converted geometry
   */
  def reprojectGeometry(geometry: Geometry, targetSRID: Int): Geometry = {
    if (geometry.getSRID == targetSRID || geometry.getSRID == 0 || targetSRID == 0)
      geometry
    else
      reprojectGeometry(geometry, findTransformationInfo(geometry.getSRID, targetSRID))
  }

  /**
   * Converts the given geometry using the provided transformation.
   * If the geometry is outside the bounds of the target CRS, null is returned
   *
   * @param geometry the geometry to reproject to the target CRS
   * @param transform the transformation to apply on the given geometry
   * @return a new geometry after being reprojected or `null` if the result is empty
   */
  def reprojectGeometry(geometry: Geometry, transform: TransformationInfo): Geometry = {
    if (transform == null || geometry.isEmpty) return geometry
    try {
      return reprojectGeometryInternal(geometry, transform)
    } catch {
      case _: TransformException | _: ProjectionException if transform.sourceBounds != null =>
        if (transform.sourceBounds.disjoint(geometry.getEnvelopeInternal))
          return null
        // If an error happens during transformation, try to limit the geometry to the source bounds
        val geom = geometry.intersection(geometry.getFactory.toGeometry(transform.sourceBounds))
        return reprojectGeometryInternal(geom, transform)
    }
    null
  }

  /**
   * Transforms a geometry using the given transformation info
   * @param geometry the geometry to reproject to the target CRS
   * @param transform the transformation to apply on the given geometry
   * @return a new geometry after being reprojected
   * @throws TransformException if an error happens while transforming the geometry.
   */
  @throws[TransformException]
  protected def reprojectGeometryInternal(geometry: Geometry, transform: TransformationInfo): Geometry = {
    var tmpCoords: Array[Double] = null
    geometry.getGeometryType match {
      case "Point" =>
        val c = new Coordinate(geometry.getCoordinate)
        tmpCoords = Array[Double](c.getX, c.getY)
        transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 1)
        c.setX(tmpCoords(0))
        c.setY(tmpCoords(1))
        return transform.targetFactory.createPoint(c)
      case "Envelope" =>
        val e = geometry.asInstanceOf[EnvelopeND]
        assert(e.getCoordinateDimension == 2, "Transform can only work with two-dimensional data")
        tmpCoords = new Array[Double](e.getCoordinateDimension)
        val tmpCoords2: Array[Double] = new Array[Double](tmpCoords.length)
        for ($d <- tmpCoords.indices) {
          tmpCoords($d) = e.getMinCoord($d)
          tmpCoords2($d) = e.getMaxCoord($d)
        }
        transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 1)
        transform.mathTransform.transform(tmpCoords2, 0, tmpCoords2, 0, 1)
        return new EnvelopeND(transform.targetFactory, tmpCoords, tmpCoords2)
      case "MultiPoint" | "LineString" | "MultiLineString" | "Polygon" | "MultiPolygon" | "GeometryCollection" =>
        return reprojectGeometryJTS(geometry, transform)
      case _ =>
        throw new RuntimeException(String.format("Cannot reproject geometries of type '%s'", geometry.getGeometryType))
    }
    geometry
  }

  @throws[TransformException]
  protected def reprojectGeometryJTS(geometry: Geometry, transform: TransformationInfo): Geometry = {
    var cs: CoordinateSequence = null
    var tmp: Array[Double] = null
    geometry.getGeometryType match {
      case "Point" =>
        val coord = new Coordinate(geometry.asInstanceOf[Point].getCoordinate)
        val tmp = Array[Double](coord.getX, coord.getY)
        transform.mathTransform.transform(tmp, 0, tmp, 0, tmp.length / 2)
        coord.setX(tmp(0))
        coord.setY(tmp(1))
        transform.targetFactory.createPoint(coord)
      case "LineString" | "LinearRing" =>
        cs = transform.targetFactory.getCoordinateSequenceFactory.create(geometry.asInstanceOf[LineString].getCoordinateSequence)
        tmp = new Array[Double](2 * cs.size)
        for (iPoint <- 0 until cs.size) {
          tmp(iPoint * 2) = cs.getX(iPoint)
          tmp(iPoint * 2 + 1) = cs.getY(iPoint)
        }
        transform.mathTransform.transform(tmp, 0, tmp, 0, tmp.length / 2)
        for (iPoint <- 0 until cs.size) {
          cs.setOrdinate(iPoint, 0, tmp(2 * iPoint))
          cs.setOrdinate(iPoint, 1, tmp(2 * iPoint + 1))
        }
        if (geometry.getGeometryType == "LineString") transform.targetFactory.createLineString(cs)
        else transform.targetFactory.createLinearRing(cs)
      case "Polygon" =>
        val p = geometry.asInstanceOf[Polygon]
        val shell = reprojectGeometryJTS(p.getExteriorRing, transform).asInstanceOf[LinearRing]
        val holes = new Array[LinearRing](p.getNumInteriorRing)
        for (iRing <- 0 until p.getNumInteriorRing) {
          holes(iRing) = reprojectGeometryJTS(p.getInteriorRingN(iRing), transform).asInstanceOf[LinearRing]
        }
        transform.targetFactory.createPolygon(shell, holes)
      case "MultiPoint" =>
        val geometries = new Array[Point](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[Point]
        transform.targetFactory.createMultiPoint(geometries)
      case "MultiLineString" =>
        val geometries = new Array[LineString](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[LineString]
        transform.targetFactory.createMultiLineString(geometries)
      case "MultiPolygon" =>
        val geometries = new Array[Polygon](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform).asInstanceOf[Polygon]
        transform.targetFactory.createMultiPolygon(geometries)
      case "GeometryCollection" =>
        val geometries = new Array[Geometry](geometry.getNumGeometries)
        for (iGeom <- 0 until geometry.getNumGeometries)
          geometries(iGeom) = reprojectGeometryJTS(geometry.getGeometryN(iGeom), transform)
        transform.targetFactory.createGeometryCollection(geometries)
      case _ =>
        throw new RuntimeException("Not supported type " + geometry.getGeometryType)
    }
  }

  /**
   * Reprojects an envelope from one SRID to another SRID
   * @param envelope the envelope to reproject with dimensions in source SRID
   * @param sourceSRID the SRID of the given envelope
   * @param targetSRID the desired SRID of the reprojected envelope
   * @return the envelope after being reprojected to target SRID
   */
  def reprojectEnvelope(envelope: Envelope, sourceSRID: Int, targetSRID: Int): Envelope = {
    val transform: TransformationInfo = findTransformationInfo(sourceSRID, targetSRID)
    val tmpCoords: Array[Double] = new Array[Double](4)
    tmpCoords(0) = envelope.getMinX
    tmpCoords(1) = envelope.getMinY
    tmpCoords(2) = envelope.getMaxX
    tmpCoords(3) = envelope.getMaxY
    transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 2)
    new Envelope(tmpCoords(0), tmpCoords(2), tmpCoords(1), tmpCoords(3))
  }

  /**
   * Reproject an envelope (orthogonal rectangle) to another envelope
   * @param envelope the input envelope to convert
   * @param sourceCRS the source coordinate reference system (CRS)
   * @param targetCRS the target coordinate reference system (CRS)
   * @return the converted envelope
   */
  def reprojectEnvelope(envelope: Envelope,
                        sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem): Envelope = {
    val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS)

    val tmpCoords: Array[Double] = new Array[Double](4)
    tmpCoords(0) = envelope.getMinX
    tmpCoords(1) = envelope.getMinY
    tmpCoords(2) = envelope.getMaxX
    tmpCoords(3) = envelope.getMaxY
    transform.mathTransform.transform(tmpCoords, 0, tmpCoords, 0, 2)
    new Envelope(tmpCoords(0), tmpCoords(2), tmpCoords(1), tmpCoords(3))
  }

  /**
   * Reproject an envelope (orthogonal rectangle) to the target CRS in-place
   * @param envelope the input envelope to convert in the form (x1, y1, x2, y2)
   * @param sourceSRID the source coordinate reference system (CRS)
   * @param targetSRID the target coordinate reference system (CRS)
   * @return the converted envelope
   */
  def reprojectEnvelopeInPlace(envelope: Array[Double], sourceSRID: Int, targetSRID: Int): Unit = {
    val transform: TransformationInfo = findTransformationInfo(sourceSRID, targetSRID)
    if (transform.sourceBounds != null) {
      // Cap the envelope bounds in the source range
      envelope(0) = (transform.sourceBounds.getMinX max envelope(0)) min transform.sourceBounds.getMaxX
      envelope(2) = (transform.sourceBounds.getMinX max envelope(2)) min transform.sourceBounds.getMaxX
      envelope(1) = (transform.sourceBounds.getMinY max envelope(1)) min transform.sourceBounds.getMaxY
      envelope(3) = (transform.sourceBounds.getMinY max envelope(3)) min transform.sourceBounds.getMaxY
    }
    transform.mathTransform.transform(envelope, 0, envelope, 0, 2)
  }

  private def reprojectEnvelopeInPlaceNoCheck(envelope: Array[Double], sourceSRID: Int, targetSRID: Int): Unit = {
    val transform: TransformationInfo = findTransformationInfo(sourceSRID, targetSRID)
    transform.mathTransform.transform(envelope, 0, envelope, 0, envelope.length / 2)
  }
}
