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
package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.cg.Reprojector;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata;
import edu.ucr.cs.bdlab.beast.util.DynamicArrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Polygon;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Stores intersections between a set of polygons and a raster layer.
 * The intersections are represented as horizontal line segments in the form
 * (pid, tid, y, x1, x2) which represents an intersection between
 * polygon #pid in tile #tid scanline #y in the range [x1, x2] inclusive of both.
 * These segments are ordered by (tid, y, pid, x1) which ensures that scanning these intersections
 * in order will match the order of the raster file.
 */
public class Intersections implements Iterable<Intersections.TilePixelRange> {
  /** Logger for this class */
  private static final Log LOG = LogFactory.getLog(Intersections.class);

  /** Total number of intersections */
  protected int numIntersections;

  /** Scan line numbers (row in the raster file) */
  protected int[] ys;

  /** x-coordinates of the intersections */
  protected int[] xs;

  /** Indexes of the intersecting polygons */
  protected int[] polygonIndexes;

  /** The IDs of the features that correspond to the given polygons. */
  protected long[] featureIDs;

  /** Tile IDs for each intersection */
  protected int[] tileID;

  protected void makeRoomForAdditionalIntersections(int newSize) {
    int nextPowerOfTwo = Integer.highestOneBit(newSize) * 2;
    xs = DynamicArrays.expand(xs, nextPowerOfTwo);
    ys = DynamicArrays.expand(ys, nextPowerOfTwo);
    polygonIndexes = DynamicArrays.expand(polygonIndexes, nextPowerOfTwo);
  }

  /**
   * Returns the total number of intersection segments.
   * @return the total number of intersections
   */
  public int getNumIntersections() {
    return numIntersections;
  }

  /**
   * Returns the start x-coordinate of the i<sup>th</sup> intersection
   * @param i the intersection number
   * @return the start x-coordinate of that intersection (inclusive)
   */
  public int getX1(int i) {
    return xs[i * 2];
  }

  /**
   * Returns the end x-coordinate (inclusive) of the i<sup>th</sup> intersection
   * @param i the intersection number
   * @return the end x-coordinate of that intersection (inclusive)
   */
  public int getX2(int i) {
    return xs[i * 2 + 1];
  }

  /**
   * Returns the y-coordinate of the i<sup>th</sup> intersection
   * @param i the intersection number
   * @return the y-coordinate of that intersection
   */
  public int getY(int i) {
    return ys[i];
  }

  /**
   * Return the index of the polygon that corresponds to the given intersection.
   * @param i the position of the polygon to get its index
   * @return the index of the given polygon
   */
  public int getPolygonIndex(int i) {
    return polygonIndexes[i];
  }

  public long getFeatureID(int i) {
    return featureIDs[polygonIndexes[i]];
  }

  public int getTileID(int i) {
    assert tileID[i] >=0 : "Invalid tile ID "+tileID[i];
    return tileID[i];
  }

  /**
   * Compute the intersections between the given array of geometries and the raster layer.
   * @param geometries the list of geometries to compute the intersectios to
   * @param metadata the metadata of the raster file to compute the intersections against
   */
  public void compute(Geometry[] geometries, RasterMetadata metadata) {
    long[] featureIDs = new long[geometries.length];
    for (int i = 0; i < geometries.length; i++)
      featureIDs[i] = i;
    compute(geometries, featureIDs, metadata);
  }

  public void compute(ArrayList<Long> IDs, Geometry[] geometries, RasterMetadata metadata) {
    long[] featureIDs = new long[IDs.size()];
    for (int i = 0; i < geometries.length; i++)
      featureIDs[i] = IDs.get(i);
    compute(geometries, featureIDs, metadata);
  }

  /**
   * Computes the intersections for the given list of geometries against the given raster.
   * @param geometries the list of geometries to compute the intersections to
   * @param featureIDs the identifier of each feature corresponding to the given polygons
   * @param metadata the metadata of the raster file to compute the intersections against
   */
  public void compute(Geometry[] geometries, long[] featureIDs, RasterMetadata metadata) {
    if (geometries.length == 0)
      return;
    this.featureIDs = Arrays.copyOf(featureIDs, featureIDs.length);
    String geometryType = geometries[0].getGeometryType();
    // A flag set to true if the intersections represent ranges (with polygons only)
    boolean ranges;
    switch (geometryType) {
      case GeometryType.PointName:
      case GeometryType.MultiPointName:
        // Assume that all geometries are points
        for (int $i = 0; $i < geometries.length; $i++) {
          Geometry geom = geometries[$i];
          if (geom.getSRID() != metadata.srid())
            geom = Reprojector.reprojectGeometry(geom, metadata.srid());
          if (geom != null)
            appendIntersectionsPoints($i, geom, metadata);
        }
        ranges = false;
        break;
      case GeometryType.LineStringName:
      case GeometryType.MultiLineStringName:
        Point2D.Double corner1 = new Point2D.Double();
        Point2D.Double corner2 = new Point2D.Double();
        // Assume that all geometries are Multiline strings
        for (int $i = 0; $i < geometries.length; $i++) {
          Geometry geom = geometries[$i];
          if (geom.getSRID() != metadata.srid())
            geom = Reprojector.reprojectGeometry(geom, metadata.srid());

          if (geom != null) {
            Envelope lineMBR = geom.getEnvelopeInternal();
            metadata.modelToGrid(lineMBR.getMinX(), lineMBR.getMinY(), corner1);
            metadata.modelToGrid(lineMBR.getMaxX(), lineMBR.getMaxY(), corner2);

            if (!(Math.max(corner1.x, corner2.x) < metadata.x1() ||
                Math.min(corner1.x, corner2.x) > metadata.x2() ||
                Math.max(corner1.y, corner2.y) < metadata.y1() ||
                Math.min(corner1.y, corner2.y) > metadata.y2())) {
              // The object is not completely outside the raster space
              appendIntersectionsLS($i, geom, metadata);
            }
          }
        }
        ranges = false;
        break;
      case GeometryType.PolygonName:
      case GeometryType.MultiPolygonName:
      case GeometryType.GeometryCollectionName:
        // Treat all of these as polygons
        for (int $i = 0; $i < geometries.length; $i++) {
          Geometry geom = geometries[$i];
          if (geom.getSRID() != metadata.srid())
            geom = Reprojector.reprojectGeometry(geom, metadata.srid());
          if (geom != null && !geom.isEmpty())
            appendIntersectionsPolygons($i, geom, metadata);
        }
        ranges = true;
        // Polygons must always produce an even number of intersections (ranges)
        assert (numIntersections & 1) == 0 : "Number of intersections should be even";
        break;
      default:
        throw new RuntimeException("Unsupported geometry type "+geometryType);
    }

    if (numIntersections == 0)
      return;

    // If there are intersections, sort them
    tileID = new int[numIntersections];

    // Sort the intersection points by <tile ID, polygon ID, y, x> (initially, all tileIDs are now zeros)
    IndexedSortable polygonIndexSorter = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        int diff = tileID[i] - tileID[j];
        if (diff == 0) {
          diff = polygonIndexes[i] - polygonIndexes[j];
          if (diff == 0) {
            diff = ys[i] - ys[j];
            if (diff == 0)
              diff = xs[i] - xs[j];
          }
        }
        return diff;
      }

      @Override
      public void swap(int i, int j) {
        // Swap y, x, pid, and tileID
        int temp = ys[i];
        ys[i] = ys[j];
        ys[j] = temp;
        temp = xs[i];
        xs[i] = xs[j];
        xs[j] = temp;
        temp = polygonIndexes[i];
        polygonIndexes[i] = polygonIndexes[j];
        polygonIndexes[j] = temp;
        temp = tileID[i];
        tileID[i] = tileID[j];
        tileID[j] = temp;
      }
    };
    new QuickSort().sort(polygonIndexSorter, 0, numIntersections);
    int numRemovedIntersections = 0;
    // Now, make one pass over all the intersections and set the tile IDs
    // For intersection segments that cross over tile boundaries, split it across the tile boundary
    if (!ranges) {
      // In this case, each intersection represents one points in the raster.
      // This part will scan all the intersections and merge consecutive intersections that belong
      // to the same geometry, are at the same scanline (y-axis), and are in the same tile.
      // We start from the end because we might add new intersections as we process them
      for (int $i = numIntersections - 1; $i >= 0; $i -= 1) {
        // Compute tile ID for this intersection
        tileID[$i] = metadata.getTileIDAtPixel(xs[$i], ys[$i]);
        if ($i != numIntersections - 1 && // Except for the last intersection
            tileID[$i + 1] == tileID[$i] && // If this and the next intersections are in the same tile
            polygonIndexes[$i + 1] == polygonIndexes[$i] && // Correspond to the same feature
            ys[$i] == ys[$i + 1] && // Are at the same row (scanline)
            (xs[$i] == xs[$i + 1] || xs[$i] == xs[$i + 1] - 1)) { // Correspond to adjacent or overlapping pixels
          // Remove the last pixel since it is included in the range
          tileID[$i + 1] = metadata.numTiles();
          numRemovedIntersections += 1;
        } else {
          // Convert the single pixel intersection to a range
          makeRoomForAdditionalIntersections(numIntersections + 1);
          tileID = DynamicArrays.expand(tileID, xs.length);
          xs[numIntersections] = xs[$i]; // The range contains one pixel and is inclusive of beginning and end
          ys[numIntersections] = ys[$i]; // At the same scanline
          tileID[numIntersections] = tileID[$i]; // At the same tile ID
          polygonIndexes[numIntersections] = polygonIndexes[$i]; // The same geometry
          numIntersections += 1;
        }
      }
    } else {
      boolean firstIteration = true;
      for (int $i = numIntersections - 2; $i >= 0; $i -= 2) {
        if (xs[$i] >= xs[$i + 1]) {
          // An empty range. Mark it for removal.
          // The largest tileID will ensure that they will be at the end of the list after sorting
          // which will make them easy to remove
          tileID[$i] = tileID[$i + 1] = metadata.numTiles();
          numRemovedIntersections += 2;
        } else {
          tileID[$i] = metadata.getTileIDAtPixel(xs[$i], ys[$i]);
          // Decrement the end of the intersection segment to make the range inclusive
          xs[$i + 1]--;
          tileID[$i + 1] = metadata.getTileIDAtPixel(xs[$i + 1], ys[$i + 1]);
          assert polygonIndexes[$i] == polygonIndexes[$i + 1];
          if (tileID[$i] != tileID[$i + 1]) {
            // The intersection segment crosses the tile boundary.
            // Break the range into two at the tile boundary by adding two new intersections, one at each side
            makeRoomForAdditionalIntersections(numIntersections + 2 * (tileID[$i + 1] - tileID[$i]));
            tileID = DynamicArrays.expand(tileID, xs.length);
            // Insert two intersections at the boundary of each tile
            for (int iTile = tileID[$i]; iTile < tileID[$i+1]; iTile++) {
              // Insert an intersection at the end of iTile
              xs[numIntersections] = metadata.getTileX2(iTile);
              ys[numIntersections] = ys[$i];
              tileID[numIntersections] = iTile;
              polygonIndexes[numIntersections] = polygonIndexes[$i];
              numIntersections++;
              // Insert an intersection at the beginning of iTile+1
              xs[numIntersections] = metadata.getTileX1(iTile+1);
              ys[numIntersections] = ys[$i];
              tileID[numIntersections] = iTile + 1;
              polygonIndexes[numIntersections] = polygonIndexes[$i];
              numIntersections++;
            }
          }
          // Check if the range can be merged with the next one
          if (!firstIteration && polygonIndexes[$i+1] == polygonIndexes[$i+2] &&
              ys[$i+1] == ys[$i+2] && tileID[$i+1] == tileID[$i+2] && xs[$i+1] == xs[$i+2] - 1) {
            // Merge the two ranges into one
            tileID[$i+1] = tileID[$i+2] = metadata.numTiles();
            numRemovedIntersections += 2;
          }
        }
        firstIteration = false;
      }
    }
    // Sort again after adding the tile IDs. Sort order will be <tile ID, polygon index,y, x>
    new QuickSort().sort(polygonIndexSorter, 0, numIntersections);
    // Now, remove all the empty ranges that have been pushed to the end
    numIntersections -= numRemovedIntersections;
    // Finally, convert the intersections to ranges in the form (tile ID, y, polygon index, x1, x2)
    numIntersections /= 2;
    int[] newYs = new int[numIntersections];
    int[] newTileIDs = new int[numIntersections];
    int[] newPolygonIndex = new int[numIntersections];

    for (int i = 0; i < numIntersections; i++) {
      newYs[i] = ys[i * 2];
      newTileIDs[i] = tileID[i * 2];
      newPolygonIndex[i] = polygonIndexes[i * 2];
    }

    xs = Arrays.copyOf(xs, numIntersections * 2);
    ys = newYs;
    tileID = newTileIDs;
    polygonIndexes = newPolygonIndex;
  }


  /**
   * Appends intersections of the given geometry to the list of intersections in any order.
   * @param geometryID the ID of this geometry to add to the list of intersections
   * @param geometry the geometry to compute the intersections to
   * @param metadata metadata of the raster file
   */
  protected void appendIntersectionsPolygons(int geometryID, Geometry geometry, RasterMetadata metadata) {
    LineString linestring;
    Polygon polygon;
    Point2D.Double pt1 = new Point2D.Double();
    Point2D.Double pt2 = new Point2D.Double();
    if (geometry.isEmpty())
      return;
    switch (geometry.getGeometryType()) {
      case "LinearRing":
      case "LineString":
        linestring = (LineString) geometry;
        Coordinate tempPointCoords = linestring.getCoordinateN(0);
        metadata.modelToGrid(tempPointCoords.getX(), tempPointCoords.getY(), pt1);
        int numPoints = linestring.getNumPoints();
        // For non-closed LineString, force closing by repeating the first and last points
        if (linestring.getGeometryType().equals("LineString"))
          numPoints++;
        for (int $i = 1; $i < numPoints; $i++) {
          tempPointCoords = linestring.getCoordinateN($i % linestring.getNumPoints());
          metadata.modelToGrid(tempPointCoords.getX(), tempPointCoords.getY(), pt2);
          double dx = pt2.x - pt1.x;
          double dy = pt2.y - pt1.y;

          // Locate the first (inclusive) and last (exclusive) rows in the raster file for this line segment
          int row1 = (int) Math.min(metadata.y2(), Math.max(metadata.y1(), Math.round(Math.min(pt1.y, pt2.y))));
          int row2 = (int) Math.min(metadata.y2(), Math.max(metadata.y1(), Math.round(Math.max(pt1.y, pt2.y))));

          makeRoomForAdditionalIntersections(numIntersections + Math.max(0, row2 - row1));
          for (int row = row1; row < row2; row++) {
            // Find the intersection of the line segment (p1, p2) and the straight line (y = scanLineY)
            double xIntersection = pt2.x - (pt2.y - (row + 0.5)) * dx / dy;
            xs[numIntersections] = (int) Math.max(metadata.x1(), Math.min(Math.round(xIntersection), metadata.x2()));
            ys[numIntersections] = row;
            polygonIndexes[numIntersections] = geometryID;
            numIntersections++;
          }
          pt1.setLocation(pt2);
        }
        break;
      case GeometryType.PolygonName:
        polygon = (Polygon) geometry;
        appendIntersectionsPolygons(geometryID, (polygon.getExteriorRing()), metadata);
        for (int iHole = 0; iHole < polygon.getNumInteriorRing(); iHole++)
          appendIntersectionsPolygons(geometryID, (polygon.getInteriorRingN(iHole)), metadata);
        break;
      case GeometryType.GeometryCollectionName:
      case GeometryType.MultiPolygonName:
        GeometryCollection gc = (GeometryCollection) geometry;
        for (int i = 0; i < gc.getNumGeometries(); i++)
          appendIntersectionsPolygons(geometryID, (gc.getGeometryN(i)), metadata);
        break;
      default:
        LOG.warn(String.format("Unsupported geometry type '%s' for polygon intersection", geometry.getGeometryType()));
    }
  }

  protected void appendIntersectionsPoints(int geometryID, Geometry geometry, RasterMetadata metadata) {
    Point2D.Double rpt = new Point2D.Double();
    switch (geometry.getGeometryType()) {
      case GeometryType.MultiPointName:
        MultiPoint mpoint = (MultiPoint) geometry;
        for (int $i = 0; $i < mpoint.getNumGeometries(); $i++) {
          Coordinate p = mpoint.getGeometryN($i).getCoordinate();
          metadata.modelToGrid(p.getX(), p.getY(), rpt);
          if (rpt.x >= metadata.x1() && rpt.x < metadata.x2() &&
              rpt.y >= metadata.y1() && rpt.y < metadata.y2()) {
            makeRoomForAdditionalIntersections(numIntersections + 1);

            xs[numIntersections] = (int) Math.round(rpt.x);
            ys[numIntersections] = (int) Math.round(rpt.y);
            polygonIndexes[numIntersections] = geometryID;
            numIntersections++;
          }
        }
        break;
      case GeometryType.PointName:
        Coordinate p = geometry.getCoordinate();
        metadata.modelToGrid(p.getX(), p.getY(), rpt);
        if (rpt.x >= metadata.x1() && rpt.x < metadata.x2() &&
            rpt.y >= metadata.y1() && rpt.y < metadata.y2()) {
          makeRoomForAdditionalIntersections(numIntersections + 1);

          xs[numIntersections] = (int) Math.round(rpt.x);
          ys[numIntersections] = (int) Math.round(rpt.y);
          polygonIndexes[numIntersections] = geometryID;
          numIntersections++;
        }
        break;
      default:
        for (Coordinate c: geometry.getCoordinates()) {
          metadata.modelToGrid(c.getX(), c.getY(), rpt);
          if (rpt.x >= metadata.x1() && rpt.x < metadata.x2() &&
              rpt.y >= metadata.y1() && rpt.y < metadata.y2()) {
            makeRoomForAdditionalIntersections(numIntersections + 1);

            xs[numIntersections] = (int) Math.round(rpt.x);
            ys[numIntersections] = (int) Math.round(rpt.y);
            polygonIndexes[numIntersections] = geometryID;
            numIntersections++;
          }
        }
    }
  }

  /**
   * Append all pixel intersection locations between the boundary of the given geometry and the center lines
   * (scan lines) of each row and column of pixels in the metadata.
   * The main difference between this function and the polygon intersection function is that we find
   * intersections with both horizontal and vertical scanline while in polygons we compute with horizontal
   * scanlines only since we convert them to ranges.
   * @param geometryID the ID of the geometry to use when recording the intersections
   * @param geometry the actual geometry to compute the intersections
   * @param metadata the metadata that defines the raster grid
   */
  protected void appendIntersectionsLS(int geometryID, Geometry geometry, RasterMetadata metadata) {
    // A temporary point used for conversion
    Point2D.Double pt = new Point2D.Double();

    switch (geometry.getGeometryType()) {
      case "LineString":
      case "LinearRing":
        LineString linestring = (LineString) geometry;
        // We iterate with these two points over all line segments to rasterize them
        int x1, x2, y1, y2;
        CoordinateSequence cs = linestring.getCoordinateSequence();
        metadata.modelToGrid(cs.getX(0), cs.getY(0), pt);
        x1 = (int) pt.x;
        y1 = (int) pt.y;
        for (int $i = 1; $i < linestring.getNumPoints(); $i++) {
          // Get grid coordinates of the end point
          metadata.modelToGrid(cs.getX($i), cs.getY($i), pt);
          x2 = (int) pt.x;
          y2 = (int) pt.y;

          int dx = Math.abs(x2 - x1);
          int dy = Math.abs(y2 - y1);
          // Check special case for horizontal or vertical line
          if (dx == 0 || dy == 0) {
            int incx = (int) Math.signum(x2 - x1);
            int incy = (int) Math.signum(y2 - y1);
            while (x1 != x2 || y1 != y2) {
              if (x1 >= metadata.x1() && x1 < metadata.x2() && y1 >= metadata.x1() && y1 < metadata.x2()) {
                makeRoomForAdditionalIntersections(numIntersections + 1);
                xs[numIntersections] = x1;
                ys[numIntersections] = y1;
                polygonIndexes[numIntersections] = geometryID;
                numIntersections++;
              }
              x1 += incx;
              y1 += incy;
            }
          } else if  (dx > dy) {
            if (x1 > x2) {
              // Ensure that x1 <= x2
              x1 ^= x2; x2 ^= x1; x1 ^= x2;
              y1 ^= y2; y2 ^= y1; y1 ^= y2;
            }
            int incy = y1 < y2? 1 : -1;
            int p = dy - dx / 2;
            int y = y1;
            for (int x = x1; x <= x2; x++) {
              // Use dumpRect rather than setPixel because we do not want to apply the transformation
              if (x >= metadata.x1() && x < metadata.x2() && y >= metadata.x1() && y < metadata.x2()) {
                makeRoomForAdditionalIntersections(numIntersections + 1);
                xs[numIntersections] = x;
                ys[numIntersections] = y;
                polygonIndexes[numIntersections] = geometryID;
                numIntersections++;
              }
              if (p > 0) {
                y += incy;
                p += dy - dx;
              } else
                p += dy;
            }
          } else {
            if (y1 > y2) {
              // Ensure that y1 < y2
              x1 ^= x2; x2 ^= x1; x1 ^= x2;
              y1 ^= y2; y2 ^= y1; y1 ^= y2;
            }
            int incx = x1 < x2? 1 : -1;
            int p = dx - dy / 2;
            int x = x1;
            for (int y = y1; y <= y2; y++) {
              // Use dumpRect rather than setPixel because we do not want to apply the transformation
              if (x >= metadata.x1() && x < metadata.x2() && y >= metadata.x1() && y < metadata.x2()) {
                makeRoomForAdditionalIntersections(numIntersections + 1);
                xs[numIntersections] = x;
                ys[numIntersections] = y;
                polygonIndexes[numIntersections] = geometryID;
                numIntersections++;
              }
              if (p > 0) {
                x += incx;
                p += dx - dy;
              } else
                p += dx;
            }
          }
          x1 = x2;
          y1 = y2;
        }
        break;
      case GeometryType.PolygonName:
        Polygon polygon = (Polygon) geometry;
        appendIntersectionsLS(geometryID, (polygon.getExteriorRing()), metadata);
        for (int iHole = 0; iHole < polygon.getNumInteriorRing(); iHole++)
          appendIntersectionsLS(geometryID, (polygon.getInteriorRingN(iHole)), metadata);
        break;
      case GeometryType.GeometryCollectionName:
      case GeometryType.MultiPolygonName:
        GeometryCollection gc = (GeometryCollection) geometry;
        for (int i = 0; i < gc.getNumGeometries(); i++) {
          appendIntersectionsLS(geometryID, (gc.getGeometryN(i)), metadata);
        }
        break;
      default:
        LOG.warn(String.format("Unsupported geometry type '%s' for polygon intersection", geometry.getGeometryType()));
    }
  }

  static class TilePixelRange {
    int tileID;
    long geometryID;
    int y;
    int x1;
    int x2;
  }

  @Override
  public Iterator<TilePixelRange> iterator() {
    return new PixelRangeIterator();
  }

  class PixelRangeIterator implements Iterator<TilePixelRange> {
    int i = 0;

    TilePixelRange t = new TilePixelRange();

    @Override
    public boolean hasNext() {
      return i < numIntersections;
    }

    @Override
    public TilePixelRange next() {
      t.tileID = getTileID(i);
      t.y = getY(i);
      t.x1 = getX1(i);
      t.x2 = getX2(i);
      t.geometryID = getFeatureID(i);
      i++;
      return t;
    }
  }
}
