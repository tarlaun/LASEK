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
package edu.ucr.cs.bdlab.beast.geolite;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A light-weight class that stores an n-dimensional envelope without geometry information
 */
public class EnvelopeNDLite implements Serializable {
  /**The coordinate of the minimum corner*/
  private double[] minCoord;

  /**The coordinate of the maximum corner*/
  private double[] maxCoord;

  public EnvelopeNDLite() {
  }

  /**
   * Create an envelope from a list of coordinates. The passed arguments is a list in the form (x1, y1, z1, ...,
   * x2, y1, z2, ...)
   * @param numDimensions the number of dimensions to initialize to
   * @param args (optional) the coordinates to initialize to. If not set, the envelope is initialized to an
   *             inverse infinite envelope that has the range (+&infin;, -&infin;) in all dimensions.
   */
  public EnvelopeNDLite(int numDimensions, double ... args) {
    this.setCoordinateDimension(numDimensions);
    if (args.length == 0)
      return;
    if (args.length != numDimensions * 2)
      throw new RuntimeException("The arguments should contain 2 * numDimensions numbers");
    for (int d = 0; d < numDimensions; d++) {
      this.minCoord[d] = args[d];
      this.maxCoord[d] = args[numDimensions + d];
    }
  }

  /**
   * Creates an envelope initialized to the given two corners.
   * @param minCoord the coordinate of the lower corner (on all dimensions and inclusive)
   * @param maxCoord the coordinate of the upper corner (on all dimensions and exclusive)
   */
  public EnvelopeNDLite(double[] minCoord, double[] maxCoord) {
    assert minCoord.length == maxCoord.length;
    this.minCoord = Arrays.copyOf(minCoord, minCoord.length);
    this.maxCoord = Arrays.copyOf(maxCoord, maxCoord.length);
  }

  public EnvelopeNDLite(EnvelopeNDLite other) {
    this.minCoord = Arrays.copyOf(other.minCoord, other.minCoord.length);
    this.maxCoord = Arrays.copyOf(other.maxCoord, other.maxCoord.length);
  }

  /**
   * Construct from a geometry envelope
   * @param mbr a geometry envelope to initialize from
   */
  public EnvelopeNDLite(EnvelopeND mbr) {
    this.set(mbr);
  }

  public EnvelopeNDLite(Envelope e) {
    this.setCoordinateDimension(2);
    this.setMinCoord(0, e.getMinX());
    this.setMinCoord(1, e.getMinY());
    this.setMaxCoord(0, e.getMaxX());
    this.setMaxCoord(1, e.getMaxY());
  }

  /**
   * Sets the number of dimensions for the envelope. If the number of dimensions is different from the current one,
   * the envelope is updated to be an empty envelope, i.e., all minimum and maximum coordinates are set to
   * &infin; and -&infin;, respectively.
   * @param numDimensions the new number of dimensions
   */
  public void setCoordinateDimension(int numDimensions) {
    if (this.minCoord == null || numDimensions != this.getCoordinateDimension()) {
      this.minCoord = new double[numDimensions];
      this.maxCoord = new double[numDimensions];
      // Set to an infinite reversed envelope to mark it as empty in all dimensions
      Arrays.fill(this.minCoord, Double.POSITIVE_INFINITY);
      Arrays.fill(this.maxCoord, Double.NEGATIVE_INFINITY);
    }
  }

  /**
   * Returns the number of dimensions for a coordinate in this envelope.
   * @return the number of dimensions of this envelope
   */
  public final int getCoordinateDimension() {
    return minCoord == null ? 0 : minCoord.length;
  }

  /**
   * Returns the minimum coordinate of dimension d
   * @param d the dimension
   * @return the minimum coordinate of the given dimension
   */
  public final double getMinCoord(int d) {
    return minCoord[d];
  }

  /**
   * Sets the minimum coordinate of one dimension
   * @param d the coordinate
   * @param x the new minimum dimension
   */
  public final void setMinCoord(int d, double x) {
    minCoord[d] = x;
    // Invalidate the envelope
  }

  /**
   * Returns the maximum coordinate of dimension d
   * @param d the dimension
   * @return the maximum coordinate of the given dimension
   */
  public final double getMaxCoord(int d) {
    return maxCoord[d];
  }

  /**
   * Sets the maximum coordinate of one dimension
   * @param d the coordinate
   * @param x the new maximum dimension
   */
  public final void setMaxCoord(int d, double x) {
    maxCoord[d] = x;
  }

  public void set(EnvelopeNDLite other) {
    if (other.isEmpty())
      this.setEmpty();
    else {
      this.setCoordinateDimension(other.getCoordinateDimension());
      for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
        this.setMinCoord($d, other.getMinCoord($d));
        this.setMaxCoord($d, other.getMaxCoord($d));
      }
    }
  }

  public void set(EnvelopeND other) {
    if (other.isEmpty())
      this.setEmpty();
    else {
      this.setCoordinateDimension(other.getCoordinateDimension());
      for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
        this.setMinCoord($d, other.getMinCoord($d));
        this.setMaxCoord($d, other.getMaxCoord($d));
      }
    }
  }


  public void set(PointND p) {
    this.setCoordinateDimension(p.getCoordinateDimension());
    for (int $d = 0; $d < p.getCoordinateDimension(); $d++) {
      this.setMinCoord($d, p.getCoordinate($d));
      this.setMaxCoord($d, p.getCoordinate($d));
    }
  }

  public void set(Coordinate c) {
    this.setCoordinateDimension(2);
    this.setMinCoord(0, c.getX());
    this.setMaxCoord(0, c.getX());
    this.setMinCoord(1, c.getY());
    this.setMaxCoord(1, c.getY());
  }

  /**
   * Sets this envelope to cover the entire space (-&infin;, +&infin;)
   */
  public void setInfinite() {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      this.setMinCoord(d, Double.NEGATIVE_INFINITY);
      this.setMaxCoord(d, Double.POSITIVE_INFINITY);
    }
  }

  /**
   * Returns the side length of dimension {@code d}.
   * @param d the index of the dimension
   * @return the side length along the given dimension
   */
  public double getSideLength(int d) {
    return this.getMaxCoord(d) - this.getMinCoord(d);
  }

  /**
   * Returns the center coordinate along the dimension {@code d}
   * @param d the index of the dimension
   * @return the center along the given dimension
   */
  public double getCenter(int d) {
    return (this.getMinCoord(d) + this.getMaxCoord(d)) / 2.0;
  }

  /**
   * Expands this envelope to enclose the given point coordinate. The given point should have the same coordinate
   * dimension of the envelope.
   * @param point the point to include in this envelope
   * @return this geometry so that it can be merged serially
   */
  public EnvelopeNDLite merge(double[] point) {
    assert point.length == getCoordinateDimension();
    for (int d = 0; d < getCoordinateDimension(); d++) {
      this.setMinCoord(d, Math.min(this.getMinCoord(d), point[d]));
      this.setMaxCoord(d, Math.max(this.getMaxCoord(d), point[d]));
    }
    return this;
  }

  /**
   * Expands this envelope to enclose the given geometry.
   * @param geom the geometry to include in this envelope
   * @return this envelope so that it can be merged serially
   */
  public EnvelopeNDLite merge(Geometry geom) {
    if (geom == null || geom.isEmpty())
      return this;
    if (geom instanceof EnvelopeND) {
      EnvelopeND env = (EnvelopeND) geom;
      this.setCoordinateDimension(env.envelopeLite.getCoordinateDimension());
      for (int d = 0; d < getCoordinateDimension(); d++) {
        this.setMinCoord(d, Math.min(this.getMinCoord(d), env.envelopeLite.getMinCoord(d)));
        this.setMaxCoord(d, Math.max(this.getMaxCoord(d), env.envelopeLite.getMaxCoord(d)));
      }
    } else if (geom instanceof PointND) {
      PointND pt = (PointND) geom;
      this.setCoordinateDimension(pt.getCoordinateDimension());
      for (int d = 0; d < this.getCoordinateDimension(); d++) {
        this.setMinCoord(d, Math.min(this.getMinCoord(d), pt.getCoordinate(d)));
        this.setMaxCoord(d, Math.max(this.getMaxCoord(d), pt.getCoordinate(d)));
      }
    } else if (geom instanceof Point) {
      merge(((org.locationtech.jts.geom.Point) geom).getCoordinateSequence());
    } else if (geom instanceof LineString) {
      merge(((LineString)geom).getCoordinateSequence());
    } else if (geom instanceof Polygon) {
      Polygon p = (Polygon) geom;
      merge(p.getExteriorRing());
      for (int $i = 0; $i < p.getNumInteriorRing(); $i++)
        merge(p.getInteriorRingN($i));
    } else if (geom instanceof GeometryCollection) {
      for (int $i = 0; $i < geom.getNumGeometries(); $i++)
        merge(geom.getGeometryN($i));
    } else {
      throw new RuntimeException("Unsupported geometry type "+geom.getClass());
    }
    return this;
  }

  public EnvelopeNDLite merge(EnvelopeNDLite other) {
    assert this.getCoordinateDimension() == other.getCoordinateDimension() : "Mismatching number of dimensions";
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      this.setMinCoord($d, Math.min(this.getMinCoord($d), other.getMinCoord($d)));
      this.setMaxCoord($d, Math.max(this.getMaxCoord($d), other.getMaxCoord($d)));
    }
    return this;
  }

  public void merge(CoordinateSequence cs) {
    // TODO should we take Z and M into account?
    int dimension = 2;
    this.setCoordinateDimension(dimension);
    for (int $i = 0; $i < cs.size(); $i++) {
      for (int $d = 0; $d < dimension; $d++) {
        double v = cs.getOrdinate($i, $d);
        this.setMinCoord($d, Math.min(this.getMinCoord($d), v));
        this.setMaxCoord($d, Math.max(this.getMaxCoord($d), v));
      }
    }
  }

  /**
   * Adjusts the coordinate dimensions for this envelope and updates its boundaries to the given two corners.
   * It is assumed that minCoords[i] &le; maxCoords[i] for all i = 0, ..., d-1. No sanity check is done.
   * @param minCoords the coordinates of the new lower corner
   * @param maxCoords the coordinates of the new upper corner
   */
  public void set(double[] minCoords, double[] maxCoords) {
    assert minCoords.length == maxCoords.length:
        String.format("Mismatching number of dimensions for minimum and maximum coordinates (%d != %d)",
            minCoords.length, maxCoords.length);
    this.setCoordinateDimension(minCoords.length);
    for (int $d = 0; $d < minCoords.length; $d++) {
      this.setMinCoord($d, minCoords[$d]);
      this.setMaxCoord($d, maxCoords[$d]);
    }
  }

  public EnvelopeNDLite intersectionEnvelope(EnvelopeNDLite other) {
    if (this.getCoordinateDimension() != other.getCoordinateDimension())
      throw new RuntimeException(String.format("Incompatible number of dimensions %d != %d",
          this.getCoordinateDimension(), other.getCoordinateDimension()));
    EnvelopeNDLite result = new EnvelopeNDLite(this.getCoordinateDimension());
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      result.minCoord[d] = Math.max(this.minCoord[d], other.minCoord[d]);
      result.maxCoord[d] = Math.min(this.maxCoord[d], other.maxCoord[d]);
    }
    return result;
  }

  /**
   * Checks if this envelope overlaps the envelope defined by the given two coordinates.
   * @param min the lower corner of the box to test
   * @param max the upper corner of the box to test
   * @return {@code true} if the interior of the two boxes (envelopes) are not disjoint
   */
  public boolean overlaps(double[] min, double[] max) {
    if (min.length != getCoordinateDimension())
      throw new RuntimeException(String.format("The dimension of the coordinates are incompatible. Expected %d found %d!",
          getCoordinateDimension(), min.length));
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (max[d] <= this.getMinCoord(d) || this.getMaxCoord(d) <= min[d])
        return false;
    }
    return true;
  }

  /**
   * Similar to {@link #overlaps(double[], double[])}
   * @param envelope2 the envelope to test for intersection
   * @return {@code true} if the interior of this and the given envelopes are not disjoint
   */
  public boolean intersectsEnvelope(EnvelopeNDLite envelope2) {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      // Special case to address an envelope that represents a point at the lower edge of the other envelope
      if (envelope2.getMinCoord(d) != this.getMinCoord(d) &&
          (envelope2.getMaxCoord(d) <= this.getMinCoord(d) || this.getMaxCoord(d) <= envelope2.getMinCoord(d)))
        return false;
    }
    return true;
  }

  public boolean intersectsEnvelope(Envelope envelope2) {
    if (envelope2.getMaxX() <= this.getMinCoord(0) || this.getMaxCoord(0) <= envelope2.getMinX())
      return false;
    if (envelope2.getMaxY() <= this.getMinCoord(1) || this.getMaxCoord(1) <= envelope2.getMinY())
      return false;
    return true;
  }

  public boolean containsPoint(double[] coord) {
    assert coord.length == getCoordinateDimension();
    for (int d = 0; d < coord.length; d++)
      if (coord[d] < getMinCoord(d) || coord[d] >= getMaxCoord(d))
        return false;
    return true;
  }

  public boolean containsPoint(Coordinate c) {
    return c.getX() >= this.getMinCoord(0) && c.getX() < this.getMaxCoord(0) &&
        c.getY() >= this.getMinCoord(1) && c.getY() < this.getMaxCoord(1);
  }

  public boolean containsPoint(PointND p) {
    assert p.getCoordinateDimension() == getCoordinateDimension();
    for (int d = 0; d < p.getCoordinateDimension(); d++)
      if (p.getCoordinate(d) < this.getMinCoord(d) || p.getCoordinate(d) >= this.getMaxCoord(d))
        return false;
    return true;
  }

  public boolean isEmpty() {
    if (getCoordinateDimension() == 0)
      return true;
    for (int d = 0; d < getCoordinateDimension(); d++)
      if (getMaxCoord(d) < getMinCoord(d) || Double.isNaN(getMinCoord(d)) || Double.isNaN(getMaxCoord(d)))
        return true;
    return false;
  }

  public void setEmpty() {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      this.setMinCoord(d, Double.POSITIVE_INFINITY);
      this.setMaxCoord(d, Double.NEGATIVE_INFINITY);
    }
  }

  /**
   * Shrink this envelope to be fully contained in the given envelope. In other words, the result of this function
   * is the intersection of this envelope with the given envelope.
   * @param that the other envelope to shrink this envelope to.
   */
  public void shrink(EnvelopeNDLite that) {
    assert this.getCoordinateDimension() == that.getCoordinateDimension();
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      this.setMinCoord(d, Math.max(this.getMinCoord(d), that.getMinCoord(d)));
      this.setMaxCoord(d, Math.min(this.getMaxCoord(d), that.getMaxCoord(d)));
    }
  }

  /**
   * Expand the envelope in all directions by the given buffer. The buffer size can be negative to shrink the envelope.
   * If only one value is given, it is used to expand/shrink all dimensions.
   * @param delta the size of the buffer to expand in all dimensions.
   */
  public void buffer(double ... delta) {
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      this.setMinCoord(d, this.getMinCoord(d) - delta[d % delta.length]);
      this.setMaxCoord(d, this.getMaxCoord(d) + delta[d % delta.length]);
    }
  }

  /**
   * Returns {@code true} if this envelope contains the given envelope.
   * @param other the other envelope to test for containment
   * @return {@code ture} if the the other envelope is completed contained in this envelope.
   */
  public boolean containsEnvelope(EnvelopeNDLite other) {
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      if (other.getMinCoord(d) < this.getMinCoord(d) || other.getMaxCoord(d) > this.getMaxCoord(d))
        return false;
    }
    return true;
  }

  public boolean containsEnvelope(Envelope other) {
    if (other.getMinX() < this.getMinCoord(0) || other.getMaxX() > this.getMaxCoord(0))
      return false;
    if (other.getMinY() < this.getMinCoord(1) || other.getMaxY() > this.getMaxCoord(1))
      return false;
    return true;
  }

  /**
   * Checks if this envelope is infinite in all dimensions.
   * @return {@code true} if this envelope is not infinite in all dimensions
   */
  public boolean isFinite() {
    for (int d = 0; d < getCoordinateDimension(); d++) {
      if (Double.isFinite(getMinCoord(d)) || Double.isFinite(getMaxCoord(d)))
        return true;
    }
    return false;
  }

  public boolean equalsExact(EnvelopeNDLite other) {
    if (this.getCoordinateDimension() != other.getCoordinateDimension())
      return false;
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      if (this.getMinCoord($d) != other.getMinCoord($d) || this.getMaxCoord($d) != other.getMaxCoord($d))
        return false;
    }
    return true;
  }

  public boolean equalsExact(EnvelopeNDLite other, double tolerance) {
    if (this.getCoordinateDimension() != other.getCoordinateDimension())
      return false;
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      if (Math.abs(this.getMinCoord($d) - other.getMinCoord($d)) > tolerance)
        return false;
      if (Math.abs(this.maxCoord[$d] - other.getMaxCoord($d)) > tolerance)
        return false;
    }
    return true;
  }

  public StringBuilder toWKT(StringBuilder out) {
    if (this.getCoordinateDimension() != 2)
      throw new RuntimeException("WKT not supported for higher-dimension polygons");
    out.append("POLYGON((");
    out.append(this.minCoord[0]);
    out.append(' ');
    out.append(this.minCoord[1]);
    out.append(',');

    out.append(this.maxCoord[0]);
    out.append(' ');
    out.append(this.minCoord[1]);
    out.append(',');

    out.append(this.maxCoord[0]);
    out.append(' ');
    out.append(this.maxCoord[1]);
    out.append(',');

    out.append(this.minCoord[0]);
    out.append(' ');
    out.append(this.maxCoord[1]);
    out.append(',');

    out.append(this.minCoord[0]);
    out.append(' ');
    out.append(this.minCoord[1]);
    out.append(')'); // The ring
    out.append(')'); // The polygon
    return out;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("Envelope (");
    if (isEmpty()) {
      str.append("EMPTY");
    } else {
      str.append('[');
      for (int d = 0; d < getCoordinateDimension(); d++) {
        if (d > 0) {
          str.append(',');
          str.append(' ');
        }
        str.append(getMinCoord(d));
      }
      str.append("]->[");
      for (int d = 0; d < getCoordinateDimension(); d++) {
        if (d > 0) {
          str.append(',');
          str.append(' ');
        }
        str.append(getMaxCoord(d));
      }
      str.append("]");
    }
    str.append(")");
    return str.toString();
  }

  public String encodeAsString() {
    StringBuilder str = new StringBuilder();
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      str.append(getMinCoord($d));
      str.append(',');
    }
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      str.append(getMaxCoord($d));
      if ($d < this.getCoordinateDimension() - 1)
        str.append(',');
    }
    return str.toString();
  }

  public static EnvelopeNDLite decodeString(String str, EnvelopeNDLite envelope) {
    String[] parts = str.split(",");
    if (parts.length % 2 == 1)
      throw new RuntimeException(String.format("Number of coordinates is odd in '%s'", str));
    envelope.setCoordinateDimension(parts.length / 2);
    for (int $d = 0; $d < envelope.getCoordinateDimension(); $d++) {
      envelope.setMinCoord($d, Double.parseDouble(parts[$d]));
      envelope.setMaxCoord($d, Double.parseDouble(parts[$d + envelope.getCoordinateDimension()]));
    }
    return envelope;
  }

  public double getArea() {
    double area = 1.0;
    for (int $d = 0; $d < getCoordinateDimension(); $d++)
      area *= this.getSideLength($d);
    return area;
  }

  public Envelope toJTSEnvelope() {
    if (this.getCoordinateDimension() != 2)
      throw new RuntimeException("Can only convert to JTS envelope if number of dimensions is equal to 2");
    return new Envelope(this.getMinCoord(0), this.getMaxCoord(0), this.getMinCoord(1), this.getMaxCoord(1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EnvelopeNDLite that = (EnvelopeNDLite) o;
    return equalsExact(that);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(minCoord);
    result = 31 * result + Arrays.hashCode(maxCoord);
    return result;
  }
}
