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

/***
 * Constants for WKB reading and writing.
 * See OpenGIS(R) Implementation Standard for Geographic information - Simple feature access -
 * Part 1: Common architecture. Section 8.2.8
 * and https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
 */
public interface WKBConstants {
  /**Marker for Big Endian*/
  byte wkbXDR = 0;

  /**Marker for little endian*/
  byte wkbNDR = 1;

  /**A special marker for empty geometries*/
  int wkbEmpty = 0;

  /**Marker for Point type in WKB*/
  int wkbPoint = 1;

  /**Marker for LineString type in WKB*/
  int wkbLineString = 2;
  /**Marker for Polygon type in WKB*/
  int wkbPolygon = 3;
  /**Marker for MultiPoint type in WKB*/
  int wkbMultiPoint = 4;
  /**Marker for MultiLineString type in WKB*/
  int wkbMultiLineString = 5;
  /**Marker for MultiPolygon type in WKB*/
  int wkbMultiPolygon = 6;
  /**Marker for GeometryCollection type in WKB*/
  int wkbGeometryCollection = 7;
  int wkbCircularString = 8;
  int wkbCompoundCurve = 9;
  int wkbCurvePolygon = 10;
  int wkbMultiCurve = 11;
  int wkbMultiSurface = 12;
  int wkbCurve = 13;
  int wkbSurface = 14;
  int wkbPolyhedralSurface = 15;
  int wkbTIN = 16;
  int wkbTriangle = 17;
  int wkbCircle = 18;
  int wkbGeodesicString = 19;
  int wkbEllipticalCurve = 20;
  int wkbNurbsCurve = 21;
  int wkbClothoid = 22;
  int wkbSpiralCurve = 23;
  int wkbCompoundSurface = 24;
  int wkbBrepSolid = 1025;
  int wkbAffinePlacement = 102;

  /**A special marker that we add for envelopes*/
  int wkbEnvelope = 30;

  int wkbMarkerXY = 0;
  int wkbMarkerXYZ = 1000;
  int wkbMarkerXYM = 2000;
  int wkbMarkerXYZM = 3000;

  /**A marker that signals that the geometry type is followed by SRID*/
  int wkbIncludeSRID = 0x20000000;
}


