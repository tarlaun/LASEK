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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateFilter;
import org.locationtech.jts.geom.CoordinateSequenceComparator;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFilter;
import org.locationtech.jts.geom.Point;

/**
 * A singleton class that represents the empty geometry.
 */
public class EmptyGeometry extends Geometry {
  /**The singleton instance of the geometry*/
  public static final EmptyGeometry instance = new EmptyGeometry();

  /**The private constructor is used to create an immutable singleton*/
  private EmptyGeometry() {
    super(GeometryReader.DefaultInstance.getGeometryFactory());
  }

  /**
   * Computes the minimum bounding box (envelope) of this geometry
   * @param envelope (output) the envelope to fill with the information
   * @return the given envelope
   */
   public EnvelopeND envelope(EnvelopeND envelope) {
    envelope.setEmpty();
    return envelope;
  }

  @Override
  public boolean intersects(Geometry geometry) {
    return false;
  }

  @Override
  public Envelope getEnvelopeInternal() {
    return new Envelope();
  }

  @Override
  public double getArea() {
    return 0;
  }

  @Override
  public String getGeometryType() {
    return "Empty";
  }

  @Override
  public Coordinate getCoordinate() {
    return new Coordinate();
  }

  @Override
  public Coordinate[] getCoordinates() {
    return new Coordinate[0];
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  public StringBuilder toWKT(StringBuilder out) {
    // An empty geometry must still have a type
    out.append("GEOMETRYCOLLECTION EMPTY");
    return out;
  }

  @Override
  public String toText() {
    return toWKT(new StringBuilder()).toString();
  }

  @Override
  protected Geometry reverseInternal() {
    return this;
  }

  @Override
  public boolean equalsExact(Geometry other, double tolerance) {
    return this == other;
  }

  @Override
  public void apply(CoordinateFilter filter) {

  }

  @Override
  public void apply(CoordinateSequenceFilter filter) {

  }

  @Override
  public void apply(GeometryFilter filter) {

  }

  @Override
  public void apply(GeometryComponentFilter filter) {

  }

  @Override
  protected Geometry copyInternal() {
    return this;
  }

  @Override
  public void normalize() {

  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    return new Envelope();
  }

  @Override
  protected int compareToSameClass(Object o) {
    return 0;
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    return 0;
  }

  @Override
  protected int getTypeCode() {
    return 0;
  }

  @Override
  public Point getCentroid() {
    return GeometryReader.DefaultInstance.geometryFactory.createPoint();
  }

  @Override
  public int getDimension() {
    return 0;
  }

  @Override
  public Geometry getBoundary() {
    return this;
  }

  @Override
  public int getBoundaryDimension() {
    return 0;
  }

  public int getCoordinateDimension() {
    return 0;
  }

  @Override
  public int getNumPoints() {
    return 0;
  }
}
