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

import org.apache.spark.beast.sql.GeometryDataType$;
import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Some fundamental operations for computational geometry operations.
 */
public class GeometryHelper {

  public static final DataType GeometryType = GeometryDataType$.MODULE$;
  /**
   * Letters to use for dimensions in case arbitrarily high number of dimensions is used. If we need more than this,
   * we can start concatenating two letters, e.g., xx, xy, and xz, for dimensions 27, 28, and 29, respectively.
   */
  public static final char[] DimensionNames = {'x', 'y', 'z', 'w', 'v', 'u', 't', 's', 'r', 'q', 'p', 'o', 'n', 'm', 'l', 'k', 'j',
      'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'};

  /**
   * Computes the cross product of the two vectors (x1, y1) and (x2, y2)
   * @param x1 x dimension of the first vector
   * @param y1 y dimension of the first vector
   * @param x2 x dimension of the second vector
   * @param y2 y dimension of the second vector
   * @return the cross produce of the given two vectors
   */
  public static double crossProduct(double x1, double y1, double x2, double y2) {
    return x1 * y2 - x2 * y1;
  }

  /**
   * Finds the relationship between the directed line (x1, y1) &rarr; (x2, y2) and the point (x,y).
   * If the point (x,y) is to the left of the line (x1,y1)-(x2,y2), a positive value is returned.
   * If the point (x,y) is to the right of the line (x1,y1)-(x2,y2), a negative value is returned.
   * If the point (x,y) is right on the line, a zero is returned.
   * @param x1 the x coordinate of the first point in the line
   * @param y1 the y coordinate of the first point in the line
   * @param x2 the x coordinate of the second point in the line
   * @param y2 the y coordinate of the second point in the line
   * @param x the x coordinate of the point
   * @param y the y coordinate of the point
   * @return a positive value if the point is to the left, a negative value if the point is to the right,
   *   zero if the point is exactly on the line.
   */
  public static int relate(double x1, double y1, double x2, double y2, double x, double y) {
    double cp = crossProduct(x2 - x1, y2 - y1, x - x1, y - y1);
    if (cp < 0)
      return -1;
    if (cp > 0)
      return 1;
    return 0;
  }

  /**
   * Returns {@code true} iff the point (x,y) is on the line segment (x1,y1)-(x2,y2). The point has to be on the line
   * segment and not on the infinite straight line denoted by the two points (x1,y1)-(x2,y2).
   * @param x1 the x coordinate of the first point in the line
   * @param y1 the y coordinate of the first point in the line
   * @param x2 the x coordinate of the second point in the line
   * @param y2 the y coordinate of the second point in the line
   * @param x the x coordinate of the point
   * @param y the y coordinate of the point
   * @return {@code true} if the point is on the line segment (not on its extents) or {@code false} it is not
   */
  public static boolean pointOnLineSegment(double x1, double y1, double x2, double y2, double x, double y) {
    if (relate(x1, y1, x2, y2, x, y) != 0)
      return false;
    return x >= x1 && x <= x2 || x >= x2 && x <= x1;
  }

  /**
   * Returns {@code true} iff the two line segments (x1,y1)-(x2,y2) and (x3,y3)-(x4,y4) intersect in at least one point.
   * @param x1 the x coordinate of the first point in the first line
   * @param y1 the y coordinate of the first point in the first line
   * @param x2 the x coordinate of the second point in the first line
   * @param y2 the y coordinate of the second point in the first line
   * @param x3 the x coordinate of the first point in the second line
   * @param y3 the y coordinate of the first point in the second line
   * @param x4 the x coordinate of the second point in the second line
   * @param y4 the y coordinate of the second point in the second line
   * @return {@code true} if the two line segments intersect in at least one point, this includes if they overlap
   *   in a line segment.
   */
  public static boolean lineSegmentOverlap(double x1, double y1, double x2, double y2,
                                           double x3, double y3, double x4, double y4) {
    boolean aCrossesB = relate(x1, y1, x2, y2, x3, y3) != relate(x1, y1, x2, y2, x4, y4);
    if (!aCrossesB)
      return false;
    boolean bCrossesA = relate(x3, y3, x4, y4, x1, y1) != relate(x3, y3, x4, y4, x2, y2);
    if (!bCrossesA)
      return false;
    return true;
  }

  /**
   * Computes the absolute area of a triangle given its three corners.
   * @param x1 the x coordinate of the first point on the triangle
   * @param y1 the y coordinate of the first point on the triangle
   * @param x2 the x coordinate of the second point on the triangle
   * @param y2 the y coordinate of the second point on the triangle
   * @param x3 the x coordinate of the third point on the triangle
   * @param y3 the y coordinate of the third point on the triangle
   * @return the absolute area of the triangle in the Euclidean space
   */
  public static double absTriangleArea(double x1, double y1, double x2, double y2, double x3, double y3) {
    return Math.abs(crossProduct(x2 - x1, y2 - y1, x3 - x1, y3 - y1)) / 2.0;
  }

  /**
   * Return the signed triangle area. If the points are in CCW order, a positive value is returned. Otherwise, if they
   * are in CW order, a negative value is returned.
   * @param x1 the x coordinate of the first point on the triangle
   * @param y1 the y coordinate of the first point on the triangle
   * @param x2 the x coordinate of the second point on the triangle
   * @param y2 the y coordinate of the second point on the triangle
   * @param x3 the x coordinate of the third point on the triangle
   * @param y3 the y coordinate of the third point on the triangle
   * @return the absolute value represents the area of the triangle, a positive sign indicates that the points of
   *   the triangle are given in counter-clockwise (positive) order.
   */
  public static double signedTriangleArea(double x1, double y1, double x2, double y2, double x3, double y3) {
    return crossProduct(x2 - x1, y2 - y1, x3 - x1, y3 - y1) / 2.0;
  }

  public static int nextPowerOfTwo(int i) {
    return Integer.highestOneBit(i) << 1;
  }

  /**
   * Fixed overhead for each geometry object added by JTS.
   */
  public static int FixedGeometryOverhead = 8 + // SRID
      8 + // Envelope reference
      8 * 4 + // Envelope data
      8 + // GeometryFactory Reference
      8; // UserDAta reference

  /**
   * Estimates the storage size needed for the given geometry
   * @param g a geometry
   * @return the estimated size needed to store the geometry without compression and in a binary format
   */
  public static int getGeometryStorageSize(Geometry g) {
    int size = FixedGeometryOverhead;
    if (g == null)
      return size;
    if (g instanceof EnvelopeND) {
      size += 8 * 2 * GeometryHelper.getCoordinateDimension(g);
      return size;
    }
    size += g.getNumPoints() * GeometryHelper.getCoordinateDimension(g) * 8;
    if (g instanceof GeometryCollection)
      size += g.getNumGeometries() * 4;
    if (g instanceof Polygon)
      size += (((Polygon)g).getNumInteriorRing() + 1) * 4;

    return size;
  }

  public static void writeIEnvelope(EnvelopeNDLite e, DataOutput out) throws IOException {
    out.writeInt(e.getCoordinateDimension());
    for (int $d = 0; $d < e.getCoordinateDimension(); $d++) {
      out.writeDouble(e.getMinCoord($d));
      out.writeDouble(e.getMaxCoord($d));
    }
  }

  public static void readIEnvelope(EnvelopeNDLite e, DataInput in) throws IOException {
    e.setCoordinateDimension(in.readInt());
    for (int $d = 0; $d < e.getCoordinateDimension(); $d++) {
      e.setMinCoord($d, in.readDouble());
      e.setMaxCoord($d, in.readDouble());
    }
  }

  public static void writeEnvelope(Envelope e, DataOutput out) throws IOException {
    out.writeDouble(e.getMinX());
    out.writeDouble(e.getMinY());
    out.writeDouble(e.getMaxX());
    out.writeDouble(e.getMaxY());
  }

  public static Envelope readEnvelope(Envelope mbr, DataInput in) throws IOException {
    double minx = in.readDouble();
    double miny = in.readDouble();
    double maxx = in.readDouble();
    double maxy = in.readDouble();
    mbr.init(minx, maxx, miny, maxy);
    return mbr;
  }

  public static int getCoordinateDimension(Geometry g) {
    if (g == null || g.isEmpty())
      return 0;
    if (g instanceof PointND)
      return ((PointND)g).getCoordinateDimension();
    else if (g instanceof EnvelopeND)
      return ((EnvelopeND)g).getCoordinateDimension();
    else if (g instanceof GeometryCollection)
      return getCoordinateDimension(g.getGeometryN(0));
    else if (g instanceof Point) {
      CoordinateSequence cs = ((Point) g).getCoordinateSequence();
      int dimension = 2;
      if (cs.hasZ() && !Double.isNaN(cs.getZ(0)))
        dimension = 4;
      else if (cs.hasM() && !Double.isNaN(cs.getM(0)))
        dimension = 3;
      return dimension;
    } else {
      return 2;
    }
  }
}
