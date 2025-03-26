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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryFilter;

import java.util.Arrays;

/**
 * A k-dimensional point shape
 */
public class PointND extends Geometry {
  /**The coordinate of the point in the k-dimension*/
  private double[] coords;

  public PointND(GeometryFactory geometryFactory) {
    // An empty point with no initial coordinates
    super(geometryFactory);
  }

  public PointND(Geometry g) {
    super(g.getFactory());
    if (g instanceof PointND) {
      set(((PointND) g).coords);
    } else if (g instanceof EnvelopeND) {
      EnvelopeND e = (EnvelopeND) g;
      this.setCoordinateDimension(e.getCoordinateDimension());
      for (int $d = 0; $d < this.getCoordinateDimension(); $d++)
        this.coords[$d] = e.getCenter($d);
    } else {
      Envelope e = g.getEnvelopeInternal();
      this.coords = new double[] {(e.getMinX() + e.getMaxX())/ 2.0,
          (e.getMinY() + e.getMaxY())/ 2.0};
    }
  }

  public PointND(GeometryFactory geometryFactory, int numDimensions, double ... coords) {
    super(geometryFactory);
    setCoordinateDimension(numDimensions);
    if (coords.length > 0)
      System.arraycopy(coords, 0, this.coords, 0, numDimensions);
  }

  /**
   * Initialize a point to the given coordinate
   * @param geometryFactory the factory associated with this geometry
   * @param coords the initial coordinates of the point
   */
  public PointND(GeometryFactory geometryFactory, double ... coords) {
    super(geometryFactory);
    this.coords = Arrays.copyOf(coords, coords.length);
  }

  /**
   * Computes the minimum bounding box (envelope) of this geometry
   * @param envelope (output) the envelope to fill with the information
   * @return the given envelope
   */
  public EnvelopeND envelope(EnvelopeND envelope) {
    assert envelope.getCoordinateDimension() == this.getCoordinateDimension();
    envelope.set(this);
    return envelope;
  }

  @Override
  public Envelope getEnvelopeInternal() {
    return new Envelope(this.coords[0], this.coords[0], this.coords[1], this.coords[1]);
  }

  @Override
  public double getArea() {
    return 0;
  }

  @Override
  public String getGeometryType() {
    return GeometryType.POINT.typename;
  }

  @Override
  public Coordinate getCoordinate() {
    return new Coordinate(this.coords[0], this.coords[1]);
  }

  @Override
  public Coordinate[] getCoordinates() {
    return new Coordinate[0];
  }

  /**
   * A point is empty if it has zero dimensions or if all its dimensions are NaN
   * @return {@code ture} if this point is empty
   */
  @Override
  public boolean isEmpty() {
    if (getCoordinateDimension() == 0)
      return true;
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (Double.isFinite(coords[d]))
        return false;
    }
    return true;
  }

  public void setEmpty() {
    if (coords != null)
      coords[0] = Double.NaN;
  }

  public StringBuilder toWKT(StringBuilder out) {
    // TODO a correct WKT representation should use POINT M, POINT Z, or POINT MZ and does not support arbitrary k
    out.append("POINT(");
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (d > 0)
        out.append(' ');
      out.append(coords[d]);
    }
    out.append(')');
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
    PointND another = other instanceof PointND? (PointND) other : new PointND(other);
    if (this.getCoordinateDimension() != another.getCoordinateDimension())
      return false;
    for (int $d = 0; $d < getCoordinateDimension(); $d++)
      if (Math.abs(this.coords[$d] - another.coords[$d]) > tolerance)
        return false;
    return true;
  }

  @Override
  public void apply(CoordinateFilter filter) {
    filter.filter(this.getCoordinate());
  }

  @Override
  public void apply(CoordinateSequenceFilter filter) {
    filter.filter(this.factory.getCoordinateSequenceFactory().create(new Coordinate[] {this.getCoordinate()}), 0);
  }

  @Override
  public void apply(GeometryFilter filter) {
    filter.filter(this);
  }

  @Override
  public void apply(GeometryComponentFilter filter) {
    filter.filter(this);
  }

  @Override
  protected Geometry copyInternal() {
    return new PointND(this.factory, this.coords);
  }

  @Override
  public void normalize() {

  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    this.envelope.init(this.coords[0], this.coords[0], this.coords[1], this.coords[1]);
    return this.envelope;
  }

  @Override
  protected int compareToSameClass(Object o) {
    PointND another = (PointND) o;
    int diff = this.getCoordinateDimension() - another.getCoordinateDimension();
    if (diff != 0)
      return diff;
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      diff = (int) Math.signum(this.coords[$d] - another.coords[$d]);
      if (diff != 0)
        return diff;
    }
    return 0;
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    PointND another = (PointND) o;
    int diff = this.getCoordinateDimension() - another.getCoordinateDimension();
    if (diff != 0)
      return diff;
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      diff = comp.compare(this.coords[$d], another.coords[$d]);
      if (diff != 0)
        return diff;
    }
    return 0;
  }

  @Override
  protected int getTypeCode() {
    return 0; // Geometry.SORTINDEX_POINT
  }

  @Override
  public org.locationtech.jts.geom.Point getCentroid() {
    return this.factory.createPoint(this.getCoordinate());
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

  public void set(double ... coords) {
    this.setCoordinateDimension(coords.length);
    System.arraycopy(coords, 0, this.coords, 0, this.coords.length);
  }

  public int getCoordinateDimension() {
    return coords == null ? 0 : coords.length;
  }

  @Override
  public int getNumPoints() {
    return 1;
  }

  public void setCoordinateDimension(int k) {
    if (k == 0){
      this.coords = null;
    } else if (this.coords == null || this.coords.length != k) {
      this.coords = new double[k];
      Arrays.fill(this.coords, Double.NaN);
    }
  }

  @Override
  public String toString() {
    StringBuffer b = new StringBuffer();
    b.append("Point (");
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (d != 0)
        b.append(", ");
      b.append(coords[d]);
    }
    b.append(')');
    return b.toString();
  }

  public final double getCoordinate(int d) {
    return coords[d];
  }

  public final void setCoordinate(int d, double v) {
    this.coords[d] = v;
  }
}
