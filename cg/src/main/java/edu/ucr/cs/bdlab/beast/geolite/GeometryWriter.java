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
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Converts geometries to WKB format
 */
public class GeometryWriter {

  public void write(Geometry geometry, DataOutput out, boolean includeSRID) throws IOException {
    // Only big endian is supported for now
    out.writeByte(WKBConstants.wkbXDR);

    if (geometry instanceof EmptyGeometry) {
      // Ignore SRID for the empty geometry
      out.writeInt(WKBConstants.wkbEmpty);
    } else if (geometry instanceof PointND) {
      writePointND((PointND) geometry, out, includeSRID);
    } else if (geometry instanceof Point) {
      writePoint((Point) geometry, out, includeSRID);
    } else if (geometry instanceof EnvelopeND) {
      writeEnvelopeND((EnvelopeND) geometry, out, includeSRID);
    } else if (geometry instanceof LineString) {
      writeLineString((LineString) geometry, out, includeSRID);
    } else if (geometry instanceof Polygon) {
      writePolygon((Polygon) geometry, out, includeSRID);
    } else if (geometry instanceof GeometryCollection) {
      // All types of geometry collection are handled similarly
      writeGeometryCollection((GeometryCollection) geometry, out, includeSRID);
    }
  }

  private void writePointND(PointND point, DataOutput out, boolean includeSRID) throws IOException {
    int type = WKBConstants.wkbPoint;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    if (point.getCoordinateDimension() == 2)
      type += WKBConstants.wkbMarkerXY;
    else
      // Higher dimension
      type += (point.getCoordinateDimension()+3) * WKBConstants.wkbMarkerXYZ;
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(point.getSRID());
    for (int $d = 0; $d < point.getCoordinateDimension(); $d++)
      out.writeDouble(point.getCoordinate($d));
  }

  private int getDimensionMarker(CoordinateSequence cs) {
    if (!cs.hasZ() && !cs.hasM())
      return WKBConstants.wkbMarkerXY;
    else if (cs.hasZ() && !cs.hasM())
      return WKBConstants.wkbMarkerXYZ;
    else if (cs.hasM() && !cs.hasZ())
      return WKBConstants.wkbMarkerXYM;
    else // Both Z and M are there
      return WKBConstants.wkbMarkerXYZM;
  }

  private void writePoint(Point point, DataOutput out, boolean includeSRID) throws IOException {
    if (point.isEmpty()) {
      // Special handling for empty points
      out.writeInt(WKBConstants.wkbPoint);
      out.writeDouble(Double.NaN);
      out.writeDouble(Double.NaN);
      return;
    }
    int type = WKBConstants.wkbPoint;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    CoordinateSequence cs = point.getCoordinateSequence();
    type += getDimensionMarker(cs);
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(point.getSRID());
    // Write coordinates
    out.writeDouble(cs.getX(0));
    out.writeDouble(cs.getY(0));
    if (cs.hasZ())
      out.writeDouble(cs.getZ(0));
    if (cs.hasM())
      out.writeDouble(cs.getM(0));
  }
  private void writeEnvelopeND(EnvelopeND envelope, DataOutput out, boolean includeSRID) throws IOException {
    int type = WKBConstants.wkbEnvelope;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    type += envelope.getCoordinateDimension() * WKBConstants.wkbMarkerXYZ;
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(envelope.getSRID());
    for (int $d = 0; $d < envelope.getCoordinateDimension(); $d++)
      out.writeDouble(envelope.getMinCoord($d));
    for (int $d = 0; $d < envelope.getCoordinateDimension(); $d++)
      out.writeDouble(envelope.getMaxCoord($d));
  }

  private void writeCoordinateSequence(CoordinateSequence cs, DataOutput out) throws IOException {
    out.writeInt(cs.size());
    for (int $i = 0; $i < cs.size(); $i++) {
      out.writeDouble(cs.getX($i));
      out.writeDouble(cs.getY($i));
      if (cs.hasZ())
        out.writeDouble(cs.getZ($i));
      if (cs.hasM())
        out.writeDouble(cs.getM($i));
    }
  }

  private void writeLineString(LineString lineString, DataOutput out, boolean includeSRID) throws IOException {
    int type = WKBConstants.wkbLineString;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    CoordinateSequence cs = lineString.getCoordinateSequence();
    type += getDimensionMarker(cs);
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(lineString.getSRID());
    writeCoordinateSequence(cs, out);
  }

  private void writePolygon(Polygon polygon, DataOutput out, boolean includeSRID) throws IOException {
    int type = WKBConstants.wkbPolygon;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    type += getDimensionMarker(polygon.getExteriorRing().getCoordinateSequence());
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(polygon.getSRID());
    if (polygon.isEmpty()) {
      // An empty polygon is written with zero rings
      out.writeInt(0);
    } else {
      out.writeInt(polygon.getNumInteriorRing() + 1);
      writeCoordinateSequence(polygon.getExteriorRing().getCoordinateSequence(), out);
      for (int $iHole = 0; $iHole < polygon.getNumInteriorRing(); $iHole++)
        writeCoordinateSequence(polygon.getInteriorRingN($iHole).getCoordinateSequence(), out);
    }
  }

  private void writeGeometryCollection(GeometryCollection collection, DataOutput out, boolean includeSRID) throws IOException {
    int type;
    if (collection instanceof MultiPoint)
      type = WKBConstants.wkbMultiPoint;
    else if (collection instanceof MultiLineString)
      type = WKBConstants.wkbMultiLineString;
    else if (collection instanceof MultiPolygon)
      type = WKBConstants.wkbMultiPolygon;
    else
      type = WKBConstants.wkbGeometryCollection;
    if (includeSRID)
      type |= WKBConstants.wkbIncludeSRID;
    out.writeInt(type);
    if (includeSRID)
      out.writeInt(collection.getSRID());
    out.writeInt(collection.getNumGeometries());
    // Write each geometry. Note that we do not repeat SRID for each sub geometry
    for (int $iGeometry = 0; $iGeometry < collection.getNumGeometries(); $iGeometry++)
      write(collection.getGeometryN($iGeometry), out, false);
  }
}
