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

/**
 * An enumeration of the supported geometry types.
 */
public enum GeometryType {
  /**An empty geometry*/
  EMPTY("Empty"),

  /**A 2D point with two coordinates (x,y)*/
  POINT("Point"),

  /**An envelope is an orthogonal rectangle with two corners (x1, y1) and (x2, y2)*/
  ENVELOPE("Envelope"),

  /**A polyline or a linestring is a sequence of line segments connected to each other*/
  LINESTRING("LineString"),

  /**A polygon consists of an outer shell as a closed linestring and zero or more holes each as a closed linestring*/
  POLYGON("Polygon"),

  /**A set of independent LineStrings*/
  MULTILINESTRING("MultiLineString"),

  /**A geometry that contains a set of independent polygons*/
  MULTIPOLYGON("MultiPolygon"),

  /**A collection of geometries of any type including other geometry collections.*/
  GEOMETRYCOLLECTION("GeometryCollection"),

  /**A collection of points*/
  MULTIPOINT("MultiPoint");

  public static final String EmptyName = "Empty";
  public static final String PointName = "Point";
  public static final String EnvelopeName = "Envelope";
  public static final String LineStringName = "LineString";

  public static final String LinearRingName = "LinearRing";
  public static final String PolygonName = "Polygon";
  public static final String MultiPointName = "MultiPoint";
  public static final String MultiLineStringName = "MultiLineString";
  public static final String MultiPolygonName = "MultiPolygon";
  public static final String GeometryCollectionName = "GeometryCollection";

  public final String typename;

  GeometryType(String name) {
    this.typename = name;
  }

  /**
   * Returns a geometry type that can include this type and the given type
   * @param other the other geometry type to coerce to
   * @return the new geometry type that covers both this and the given types
   */
  public GeometryType coerce(GeometryType other) {
    if (this == EMPTY)
      return other;
    if (this == other)
      return this;
    if (this == GEOMETRYCOLLECTION || other == GEOMETRYCOLLECTION)
      return GEOMETRYCOLLECTION;
    if (this.ordinal() > other.ordinal())
      return other.coerce(this);
    if (this == POINT && other == MULTIPOINT)
      return MULTIPOINT;
    if (this == ENVELOPE && (other == POLYGON || other == MULTIPOLYGON ))
      return other;
    if (this == POLYGON || other == MULTIPOLYGON)
      return MULTIPOLYGON;
    if (this == LINESTRING || other == MULTILINESTRING)
      return MULTILINESTRING;
    return GEOMETRYCOLLECTION;
  }

}
