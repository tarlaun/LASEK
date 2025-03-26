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
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceComparator;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Dimension;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryFilter;
import org.locationtech.jts.geom.IntersectionMatrix;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygon;

/**
 * A wrapper around EnvelopeNDLite that makes it a JTS geometry.
 */
public class EnvelopeND extends Geometry {
  public void setCoordinateDimension(int numDimensions) {
    envelopeLite.setCoordinateDimension(numDimensions);
    this.envelope = null;
  }

  /**The underlying envelope that stores the information*/
  EnvelopeNDLite envelopeLite;

  public EnvelopeND(GeometryFactory geometryFactory) {
    super(geometryFactory);
    this.envelopeLite = new EnvelopeNDLite();
  }

  public EnvelopeND(GeometryFactory geometryFactory, EnvelopeNDLite envelopeLite) {
    super(geometryFactory);
    this.envelopeLite = envelopeLite;
  }

  /**
   * Create an envelope from a list of coordinates. The passed arguments is a list in the form (x1, y1, z1, ...,
   * x2, y1, z2, ...)
   * @param geometryFactory the factory associated with this geometry
   * @param numDimensions the number of dimensions to initialize to
   * @param args (optional) the coordinates to initialize to. If not set, the envelope is initialized to an
   *             inverse infinite envelope that has the range (+&infin;, -&infin;) in all dimensions.
   */
  public EnvelopeND(GeometryFactory geometryFactory, int numDimensions, double ... args) {
    super(geometryFactory);
    envelopeLite = new EnvelopeNDLite(numDimensions, args);
  }

  /**
   * Creates an envelope initialized to the given two corners.
   * @param geometryFactory the geometry factory associated with this geometry
   * @param minCoord the coordinate of the lower corner (on all dimensions and inclusive)
   * @param maxCoord the coordinate of the upper corner (on all dimensions and exclusive)
   */
  public EnvelopeND(GeometryFactory geometryFactory, double[] minCoord, double[] maxCoord) {
    super(geometryFactory);
    envelopeLite = new EnvelopeNDLite(minCoord, maxCoord);
  }

  /**
   * A copy constructor.
   * @param other takes a value from another envelope
   */
  public EnvelopeND(EnvelopeND other) {
    this(other.getFactory());
    this.envelopeLite = new EnvelopeNDLite(other.envelopeLite);
  }

  /**
   * A constructor from JTS Envelope.
   * @param geometryFactory the factory associated with this envelope
   * @param other JTS envelope
   */
  public EnvelopeND(GeometryFactory geometryFactory, Envelope other) {
    super(geometryFactory);
    this.envelopeLite = new EnvelopeNDLite(2, other.getMinX(), other.getMinY(),
        other.getMaxX(), other.getMaxY());
  }

  /**
   * Initialize an envelope to contain the minimum bounding rectangle (MBR) of a geometry.
   * @param geom the geometry to take its MBR
   */
  public EnvelopeND(Geometry geom) {
    super(geom.getFactory());
    Envelope mbr = geom.getEnvelopeInternal();
    this.envelopeLite = new EnvelopeNDLite(2, mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY());
  }

  @Override
  public int getNumPoints() {
    return 4;
  }

  @Override
  public double getLength() {
    double length = 0;
    for (int d = 0; d < getCoordinateDimension(); d++)
      length += getSideLength(d);
    return 2 * length;
  }

  /**
   * Compute the intersection of this envelope and the given envelope and return the result as a new Envelope.
   * @param other the other envelope to test for intersection
   * @return {@code true} if the interior of the two envelopes are not disjoint
   */
  public EnvelopeND intersectionEnvelope(EnvelopeND other) {
    return new EnvelopeND(this.getFactory(), envelopeLite.intersectionEnvelope(other.envelopeLite));
  }

  /**
   * Tests if this envelope intersects the other envelope (not disjoint)
   * @param envelope2 the envelope to test for intersection
   * @return {@code true} if the interior of this and the given envelopes are not disjoint
   */
  public boolean intersectsEnvelope(EnvelopeND envelope2) {
    return this.envelopeLite.intersectsEnvelope(envelope2.envelopeLite);
  }

  public boolean intersectsEnvelope(Envelope envelope2) {
    return this.envelopeLite.intersectsEnvelope(envelope2);
  }

  /**
   * Computes the minimum bounding box (envelope) of this geometry
   * @param envelope (output) the envelope to fill with the information
   * @return the given envelope
   */
  public EnvelopeND envelope(EnvelopeND envelope) {
    if (envelope == null)
      envelope = new EnvelopeND(this.getFactory());
    envelope.envelopeLite.set(this.envelopeLite);
    return envelope;
  }

  @Override
  public double getArea() {
    double volume = 1.0;
    for (int d = 0; d < envelopeLite.getCoordinateDimension(); d++)
      volume *= envelopeLite.getSideLength(d);
    return volume;
  }

  @Override
  public String getGeometryType() {
    return GeometryType.ENVELOPE.typename;
  }

  @Override
  public Coordinate getCoordinate() {
    return new CoordinateXY(this.envelopeLite.getMinCoord(0), this.envelopeLite.getMinCoord(1));
  }

  @Override
  public Coordinate[] getCoordinates() {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new CoordinateXY(this.envelopeLite.getMinCoord(0), this.envelopeLite.getMinCoord(1));
    coordinates[1] = new CoordinateXY(this.envelopeLite.getMaxCoord(0), this.envelopeLite.getMinCoord(1));
    coordinates[2] = new CoordinateXY(this.envelopeLite.getMaxCoord(0), this.envelopeLite.getMaxCoord(1));
    coordinates[3] = new CoordinateXY(this.envelopeLite.getMinCoord(0), this.envelopeLite.getMaxCoord(1));
    coordinates[4] = coordinates[0];
    return coordinates;
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
    EnvelopeND another = other instanceof EnvelopeND? (EnvelopeND) other :
        new EnvelopeND(this.factory).merge(other);
    if (this.getCoordinateDimension() != another.getCoordinateDimension())
      return false;
    for (int $d = 0; $d < this.getCoordinateDimension(); $d++) {
      if (Math.abs(this.getMinCoord($d) - another.getMinCoord($d)) > tolerance)
        return false;
      if (Math.abs(this.getMaxCoord($d) - another.getMaxCoord($d)) > tolerance)
        return false;
    }
    return true;
  }

  @Override
  public void apply(CoordinateFilter filter) {
    toPolygon().apply(filter);
  }

  @Override
  public void apply(CoordinateSequenceFilter filter) {
    toPolygon().apply(filter);
  }

  @Override
  public void apply(GeometryFilter filter) {
    toPolygon().apply(filter);
  }

  @Override
  public void apply(GeometryComponentFilter filter) {
    toPolygon().apply(filter);
  }

  @Override
  protected Geometry copyInternal() {
    return new EnvelopeND(this);
  }

  @Override
  public void normalize() {

  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    return new Envelope(this.getMinCoord(0), this.getMaxCoord(0), this.getMinCoord(1), this.getMaxCoord(1));
  }

  @Override
  protected int compareToSameClass(Object o) {
    throw new RuntimeException("Not yet implemented");
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    throw new RuntimeException("Not yet implemented");
  }

  @Override
  protected int getTypeCode() {
    return 5; // Geometry.SORTINDEX_POLYGON
  }

  public void setEmpty() {
    this.envelopeLite.setEmpty();
    this.envelope = null;
  }

  public void shrink(EnvelopeNDLite that) {
    envelopeLite.shrink(that);
  }

  public void buffer(double... delta) {
    envelopeLite.buffer(delta);
  }

  public boolean containsEnvelope(EnvelopeNDLite other) {
    return envelopeLite.containsEnvelope(other);
  }

  public boolean containsEnvelope(Envelope other) {
    return envelopeLite.containsEnvelope(other);
  }

  public boolean isFinite() {
    return envelopeLite.isFinite();
  }

  public boolean equalsExact(EnvelopeNDLite other) {
    return envelopeLite.equalsExact(other);
  }

  public StringBuilder toWKT(StringBuilder out) {
    return this.envelopeLite.toWKT(out);
  }

  @Override
  public org.locationtech.jts.geom.Point getCentroid() {
    return factory.createPoint(new Coordinate(this.getCenter(0), this.getCenter(1)));
  }

  @Override
  public int getDimension() {
    return 2;
  }

  @Override
  public Geometry getBoundary() {
    throw new RuntimeException("Not yet implemented");
  }

  @Override
  public int getBoundaryDimension() {
    return 1;
  }

  @Override
  public String toString() {
    return envelopeLite.toString();
  }

  @Override
  public boolean isRectangle() {
    // While it makes sense to uncomment the next line, it will cause the internal functionality of JTS to break
    // since it will try to cast this object to Polygon while it is not a subclass of Polygon.
    // Check Geometry.intersects(Geometry)
    //return getCoordinateDimension() == 2;
    return false;
  }

  // Override Geometry functions to work with n-dimensional envelopes


  @Override
  public IntersectionMatrix relate(Geometry g) {
    if (g instanceof EnvelopeND) {
      EnvelopeND other = (EnvelopeND) g;
      int dimensionResult = Math.min(this.getCoordinateDimension(), other.getCoordinateDimension());

      double[] minIntersection = new double[dimensionResult];
      double[] maxIntersection = new double[dimensionResult];
      int[] intersectionDimension = new int[dimensionResult];
      int minIntersectionDimension = 2;
      int maxIntersectionDimension = -1;
      boolean aCoversB = true;
      boolean bCoversA = true;
      boolean coincidentEdge = false;
      boolean aIsInfinite = true;
      boolean bIsInfinite = true;
      for (int $d = 0; $d < dimensionResult; $d++) {
        minIntersection[$d] = Math.max(this.getMinCoord($d), other.getMinCoord($d));
        maxIntersection[$d] = Math.min(this.getMaxCoord($d), other.getMaxCoord($d));
        if (minIntersection[$d] < maxIntersection[$d])
          intersectionDimension[$d] = Dimension.L; // Intersection is a line in this dimension
        else if (minIntersection[$d] == maxIntersection[$d])
          intersectionDimension[$d] = Dimension.P; // Intersection is a point in this dimension
        else /*if (minIntersection[$d] > maxIntersection[$d])*/
          intersectionDimension[$d] = Dimension.FALSE; // Disjoint in this dimension
        minIntersectionDimension = Math.min(minIntersectionDimension, intersectionDimension[$d]);
        maxIntersectionDimension = Math.max(maxIntersectionDimension, intersectionDimension[$d]);
        bCoversA = bCoversA && minIntersection[$d] == this.getMinCoord($d) && maxIntersection[$d] == this.getMaxCoord($d);
        aCoversB = aCoversB && minIntersection[$d] == other.getMinCoord($d) && maxIntersection[$d] == other.getMaxCoord($d);
        coincidentEdge = coincidentEdge || this.getMinCoord($d) == other.getMinCoord($d);
        coincidentEdge = coincidentEdge || this.getMaxCoord($d) == other.getMaxCoord($d);
        aIsInfinite = aIsInfinite && this.getMinCoord($d) == Double.NEGATIVE_INFINITY
                && this.getMaxCoord($d) == Double.POSITIVE_INFINITY;
        bIsInfinite = bIsInfinite && other.getMinCoord($d) == Double.NEGATIVE_INFINITY
                && other.getMaxCoord($d) == Double.POSITIVE_INFINITY;
      }
      IntersectionMatrix m = new IntersectionMatrix();
      // Interior of A
      m.set(Location.INTERIOR, Location.INTERIOR, minIntersectionDimension == Dimension.L? Dimension.A : Dimension.FALSE);
      m.set(Location.INTERIOR, Location.BOUNDARY, (minIntersectionDimension == Dimension.L && // The intersection must be an area
              (!bCoversA || coincidentEdge))? // B cannot cover A unless there is at least one coincident dimension
                      Dimension.L :
                      Dimension.FALSE);
      m.set(Location.INTERIOR, Location.EXTERIOR, !bCoversA? Dimension.A : Dimension.FALSE);

      // Boundary of A
      m.set(Location.BOUNDARY, Location.INTERIOR, (minIntersectionDimension == Dimension.A && // The intersection must be an area
              (!aCoversB || coincidentEdge))? // A cannot cover B unless there is at least one coincident dimension
              Dimension.L :
              Dimension.FALSE);
      // Boundary X Boundary can be a line, a point, or none
      if (minIntersectionDimension == Dimension.L && !coincidentEdge)
        m.set(Location.BOUNDARY, Location.BOUNDARY, Dimension.P);
      else if (minIntersectionDimension == Dimension.L /*&& coincidentEdge*/)
        m.set(Location.BOUNDARY, Location.BOUNDARY, Dimension.L);
      else if (minIntersectionDimension == Dimension.P)
        m.set(Location.BOUNDARY, Location.BOUNDARY, Dimension.P);
      else /*if (minIntersectionDimension == Dimension.FALSE)*/
        m.set(Location.BOUNDARY, Location.BOUNDARY, Dimension.FALSE);
      // Boundary X Exterior can be a line or none
      m.set(Location.BOUNDARY, Location.EXTERIOR, bCoversA? Dimension.FALSE : Dimension.L);

      // Exterior of A
      m.set(Location.EXTERIOR, Location.INTERIOR, !aCoversB? Dimension.A : Dimension.FALSE);
      m.set(Location.EXTERIOR, Location.BOUNDARY, aCoversB? Dimension.FALSE : Dimension.L);

      // Exterior X Exterior can be area or none
      if (aIsInfinite || bIsInfinite)
        m.set(Location.EXTERIOR, Location.EXTERIOR, Dimension.FALSE);
      else
        m.set(Location.EXTERIOR, Location.EXTERIOR, Dimension.A);

      return m;
    }
    return this.toPolygon().relate(g);
  }

  protected Polygon toPolygon() {
    if (this.getCoordinateDimension() != 2)
      throw new RuntimeException("Converting to polygon is only supported for 2D envelopes");
    CoordinateSequence cs = this.getFactory().getCoordinateSequenceFactory().create(5, 2, 0);
    cs.setOrdinate(0, 0, this.getMinCoord(0));
    cs.setOrdinate(0, 1, this.getMinCoord(1));
    cs.setOrdinate(1, 0, this.getMaxCoord(0));
    cs.setOrdinate(1, 1, this.getMinCoord(1));
    cs.setOrdinate(2, 0, this.getMaxCoord(0));
    cs.setOrdinate(2, 1, this.getMaxCoord(1));
    cs.setOrdinate(3, 0, this.getMaxCoord(0));
    cs.setOrdinate(3, 1, this.getMinCoord(1));
    cs.setOrdinate(4, 0, this.getMinCoord(0));
    cs.setOrdinate(4, 1, this.getMinCoord(1));
    return this.getFactory().createPolygon(cs);
  }

  public boolean isEmpty() {
    if (getCoordinateDimension() == 0)
      return true;
    for (int d = 0; d < getCoordinateDimension(); d++)
      if (getMaxCoord(d) < getMinCoord(d) || Double.isNaN(getMinCoord(d)) || Double.isNaN(getMaxCoord(d)))
        return true;
    return false;
  }

  public int getCoordinateDimension() {
    return envelopeLite.getCoordinateDimension();
  }

  public double getMinCoord(int d) {
    return envelopeLite.getMinCoord(d);
  }

  public void setMinCoord(int d, double x) {
    envelopeLite.setMinCoord(d, x);
    this.envelope = null;
  }

  public double getMaxCoord(int d) {
    return envelopeLite.getMaxCoord(d);
  }

  public void setMaxCoord(int d, double x) {
    envelopeLite.setMaxCoord(d, x);
    this.envelope = null;
  }

  public void set(EnvelopeND other) {
    envelopeLite.set(other.envelopeLite);
    this.envelope = null;
  }

  public void set(PointND p) {
    envelopeLite.set(p);
    this.envelope = null;
  }

  public void set(Coordinate c) {
    envelopeLite.set(c);
    this.envelope = null;
  }

  public void setInfinite() {
    envelopeLite.setInfinite();
    this.envelope = null;
  }

  public double getSideLength(int d) {
    return envelopeLite.getSideLength(d);
  }

  public double getCenter(int d) {
    return envelopeLite.getCenter(d);
  }

  public PointND getCenter() {
    PointND center = new PointND(this.factory, this.getCoordinateDimension());
    for (int d = 0; d < this.getCoordinateDimension(); d++)
      center.setCoordinate(d, getCenter(d));
    return center;
  }

  public EnvelopeND merge(double[] point) {
    return new EnvelopeND(this.getFactory(), envelopeLite.merge(point));
  }

  public EnvelopeND merge(Geometry geom) {
    return new EnvelopeND(this.getFactory(), envelopeLite.merge(geom));
  }

  public void merge(EnvelopeND other) {
    envelopeLite.merge(other.envelopeLite);
  }

  public void merge(CoordinateSequence cs) {
    envelopeLite.merge(cs);
  }

  public void set(double[] minCoords, double[] maxCoords) {
    envelopeLite.set(minCoords, maxCoords);
    this.envelope = null;
  }

  public boolean overlaps(double[] min, double[] max) {
    return envelopeLite.overlaps(min, max);
  }

  public boolean intersectsEnvelope(EnvelopeNDLite envelope2) {
    return envelopeLite.intersectsEnvelope(envelope2);
  }

  public boolean containsPoint(double[] coord) {
    return envelopeLite.containsPoint(coord);
  }

  public boolean containsPoint(Coordinate c) {
    return envelopeLite.containsPoint(c);
  }

  public boolean containsPoint(PointND p) {
    return envelopeLite.containsPoint(p);
  }


}
