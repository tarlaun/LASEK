/*
 * Copyright 2018 University of California, Riverside
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

package edu.ucr.cs.bdlab.beast.geolite;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

/**
 * A class that reads geometries from an input stream written in WKB format.
 */
public class GeometryReader {

  public static final GeometryFactory DefaultGeometryFactory = new GeometryFactory(
      new PrecisionModel(PrecisionModel.FLOATING), 4326, CoordinateArraySequenceFactory.instance());

  /**A default instance that is used for serializing/deserializing features by Spark*/
  public static final GeometryReader DefaultInstance = new GeometryReader(DefaultGeometryFactory);

  /**The geometry reader used to create all geometries*/
  protected GeometryFactory geometryFactory;

  public GeometryReader(GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
  }

  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
  }

  public static GeometryFactory getGeometryFactory(int srid) {
    return new GeometryFactory(DefaultGeometryFactory.getPrecisionModel(), srid,
        DefaultGeometryFactory.getCoordinateSequenceFactory());
  }

  public static GeometryReader getGeometryReader(int srid) {
    return new GeometryReader(getGeometryFactory(srid));
  }

  /**
   * Adjusts the geometry factory to use the given SRID for all future geometries
   * @param srid the Spatial Reference Identifier to use for subsequent geometries
   */
  protected void setSRID(int srid) {
    if (srid != this.geometryFactory.getSRID()) {
      // Need a new geometry factory with the new SRID
      this.geometryFactory = new GeometryFactory(this.geometryFactory.getPrecisionModel(), srid,
          this.geometryFactory.getCoordinateSequenceFactory());
    }
  }

  /**
   * Parses the given WKB and returns a new geometry.
   *
   * @param dataIn     the input stream that contains the WKB representation
   * @return if the given WKB represents a geometry of the same type as given, the same object is returned. Otherwise,
   * a new object is created and returned.
   * @throws IOException if an error happens while reading from the input
   */
  public Geometry parse(DataInput dataIn) throws IOException {
    byte byteOrdering = dataIn.readByte();
    assert byteOrdering == WKBConstants.wkbXDR : "Only BIG ENDIAN is supported";
    int geometryTypeDimension = dataIn.readInt();
    boolean sridIncluded = (geometryTypeDimension & WKBConstants.wkbIncludeSRID) != 0;
    if (sridIncluded)
      setSRID(dataIn.readInt());
    geometryTypeDimension &= ~WKBConstants.wkbIncludeSRID;
    int geometryType = geometryTypeDimension % 1000;
    int dimensionMarker = geometryTypeDimension - geometryType;
    boolean usePointND = false;
    int dimension;
    boolean measure;
    switch (dimensionMarker) {
      case WKBConstants.wkbMarkerXY:
        dimension = 2;
        measure = false;
        break;
      case WKBConstants.wkbMarkerXYZ:
        dimension = 3;
        measure = false;
        break;
      case WKBConstants.wkbMarkerXYM:
        dimension = 2;
        measure = true;
        break;
      case WKBConstants.wkbMarkerXYZM:
        dimension = 3;
        measure = true;
        break;
      default:
        dimension = dimensionMarker / 1000 - 3;
        measure = false;
        usePointND = true;
        break;
    }
    switch (geometryType) {
      case WKBConstants.wkbEmpty:
        return EmptyGeometry.instance;
      case WKBConstants.wkbPoint:
        return parsePoint(dataIn, dimension, measure, usePointND);
      case WKBConstants.wkbEnvelope:
        dimension = dimensionMarker / 1000;
        return parseEnvelope(dataIn, dimension);
      case WKBConstants.wkbLineString:
        return parseLineString(dataIn, dimension, measure, false);
      case WKBConstants.wkbPolygon:
        return parsePolygon(dataIn, dimension, measure);
      case WKBConstants.wkbMultiPoint:
        return parseMultiPoint(dataIn);
      case WKBConstants.wkbMultiLineString:
        return parseMultiLineString(dataIn);
      case WKBConstants.wkbMultiPolygon:
        return parseMultiPolygon(dataIn);
      case WKBConstants.wkbGeometryCollection:
        return parseGeometryCollection(dataIn);
      default:
        throw new RuntimeException(String.format("Unsupported geometry type %d", geometryType));
    }
  }

  protected Geometry parsePoint(DataInput dataIn, int dimension, boolean measure, boolean usePointND) throws IOException {
    double[] coordinates = new double[dimension];
    double m = Double.NaN;
    boolean allNan = true;
    for (int $d = 0; $d < dimension; $d++) {
      coordinates[$d] = dataIn.readDouble();
      if (!Double.isNaN(coordinates[$d]))
        allNan = false;
    }
    if (measure) {
      m = dataIn.readDouble();
      if (!Double.isNaN(m))
        allNan = false;
    }
    // Special handling for empty points
    if (allNan)
      return geometryFactory.createPoint();
    Geometry g;
    // Due to a bug in some spatial operations, we always add a measure dimension even if we don't use it
    // See https://github.com/locationtech/jts/issues/434
    // See https://github.com/locationtech/jts/issues/597
    if (!usePointND && dimension <= 3) {
      CoordinateSequence cs = geometryFactory.getCoordinateSequenceFactory().create(1, dimension  + 1, 1);
      for (int d = 0; d < dimension; d++)
        cs.setOrdinate(0, d, coordinates[d]);
      cs.setOrdinate(0, dimension, m);
      g = geometryFactory.createPoint(cs);
    } else {
      // All higher dimensional points are handled using PointND
      g = new PointND(geometryFactory, dimension, coordinates);
    }
    return g;
  }

  protected Geometry parseEnvelope(DataInput dataIn, int dimension) throws IOException {
    try {
      EnvelopeND envelope = new EnvelopeND(geometryFactory, dimension);
      for (int $d = 0; $d < dimension; $d++)
        envelope.setMinCoord($d, dataIn.readDouble());
      for (int $d = 0; $d < dimension; $d++)
        envelope.setMaxCoord($d, dataIn.readDouble());
      return envelope;
    } catch (EOFException e) {
      throw new RuntimeException("Error parsing envelope with dimension "+dimension, e);
    }
  }

  protected void readCoordinateSequence(DataInput dataIn, CoordinateSequence coordinateSequence, int numDimensions)
      throws IOException {
    for (int $i = 0; $i < coordinateSequence.size(); $i++) {
      for (int $d = 0; $d < numDimensions; $d++) {
        coordinateSequence.setOrdinate($i, $d, dataIn.readDouble());
      }
      if (numDimensions < coordinateSequence.getDimension())
        coordinateSequence.setOrdinate($i, numDimensions, Double.NaN);
    }
  }

  protected Geometry parseLineString(DataInput dataIn, int dimension, boolean measure, boolean ring) throws IOException {
    int numPoints = dataIn.readInt();
    CoordinateSequence coordinateSequence =
        geometryFactory.getCoordinateSequenceFactory().create(numPoints, dimension ==2? dimension + 1 : dimension,
            measure || dimension == 2? 1 : 0);
    readCoordinateSequence(dataIn, coordinateSequence, dimension + (measure? 1 : 0));
    // Force last point to be similar to first point
    if (ring)
      for (int $d = 0; $d < dimension; $d++)
        coordinateSequence.setOrdinate(numPoints - 1, $d, coordinateSequence.getOrdinate(0, $d));
    return ring? geometryFactory.createLinearRing(coordinateSequence) :
        geometryFactory.createLineString(coordinateSequence);
  }

  protected Geometry parsePolygon(DataInput dataIn, int dimension, boolean measure) throws IOException {
    int numRings = dataIn.readInt();
    if (numRings == 0)
      return geometryFactory.createPolygon();
    LinearRing outerShell = (LinearRing) parseLineString(dataIn, dimension, measure, true);
    LinearRing[] holes = new LinearRing[numRings - 1];
    for (int $iHole = 0; $iHole < holes.length; $iHole++)
      holes[$iHole] = (LinearRing) parseLineString(dataIn, dimension, measure, true);
    return geometryFactory.createPolygon(outerShell, holes);
  }

  protected Geometry parseMultiPoint(DataInput dataIn) throws IOException {
    int numGeometries = dataIn.readInt();
    Point[] points = new Point[numGeometries];
    for (int $iGeom = 0; $iGeom < numGeometries; $iGeom++)
      points[$iGeom] = (Point) parse(dataIn);
    return geometryFactory.createMultiPoint(points);
  }

  protected Geometry parseMultiLineString(DataInput dataIn) throws IOException {
    int numGeometries = dataIn.readInt();
    LineString[] lineStrings = new LineString[numGeometries];
    for (int $iGeom = 0; $iGeom < numGeometries; $iGeom++)
      lineStrings[$iGeom] = (LineString) parse(dataIn);
    return geometryFactory.createMultiLineString(lineStrings);
  }

  protected Geometry parseMultiPolygon(DataInput dataIn) throws IOException {
    int numGeometries = dataIn.readInt();
    Polygon[] polygons = new Polygon[numGeometries];
    for (int $iGeom = 0; $iGeom < numGeometries; $iGeom++)
      polygons[$iGeom] = (Polygon) parse(dataIn);
    return geometryFactory.createMultiPolygon(polygons);
  }

  protected Geometry parseGeometryCollection(DataInput dataIn) throws IOException {
    int numGeometries = dataIn.readInt();
    Geometry[] geometries = new Geometry[numGeometries];
    for (int $iGeom = 0; $iGeom < numGeometries; $iGeom++)
      geometries[$iGeom] = parse(dataIn);
    return geometryFactory.createGeometryCollection(geometries);
  }
}
