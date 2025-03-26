/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry
import edu.ucr.cs.bdlab.beast.io.shapefilev2.ShapefileGeometryReader.isClockWiseOrder
import edu.ucr.cs.bdlab.beast.util.IOUtil
import org.apache.spark.internal.Logging
import org.locationtech.jts.geom._

import java.io.{DataInputStream, IOException}

/**
 * Reads all geometries in the given .shp file
 * @param in the input stream that points to the .shp file. Initialized at offset zero
 * @param offsets (Optional) the list of geometry offsets from .shx if exists
 */
class ShapefileGeometryReader(in: DataInputStream, srid: Int = 4326, offsets: Array[Int] = null,
                              filterRange: Envelope = null)
  extends Iterator[Geometry] with AutoCloseable with Logging {
  /** Position in the .shp file */
  var pos: Long = 0

  /** The index of the record to be read next */
  var iNextRecord: Int = 0

  /** The index of the record that is currently read */
  var iCurrentRecord: Int = 0

  /** The header of the shapefile */
  val header: ShapefileHeader = ShapefileHeader.readHeader(in)
  pos = 100

  // Check if the entire file can be skipped based on the MBR in the header
  if (filterRange != null && filterRange.disjoint(header)) {
    // Make it look like the file has been read fully
    pos = header.fileLength * 2
  }

  /** The next geometry to be returned or null if reached end of file */
  private var nextRecord: Geometry = _

  /** The geometry factor used to created all geometries */
  val geometryFactory: GeometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), srid)

  /** The record number of the record that will be returned next. */
  var nextRecordNumber: Int = -1

  /** The record number of the record that was last returned */
  var currentRecordNumber: Int = -1

  // Prefetch the first record
  nextRecord = prefetchNext()

  def skipTo(newPos: Long): Unit = {
    assert(pos <= newPos, s"Cannot skip back. Current position $pos > $newPos")
    while (pos < newPos) {
      val n = in.skipBytes((newPos - pos).toInt)
      if (n < 0)
        throw new IOException(s"Reached end of .shp file before reading record #$iNextRecord at position ")
      pos += n
    }
  }

  override def hasNext: Boolean = nextRecord != null

  override def next(): Geometry = {
    val currentRecord = nextRecord
    iCurrentRecord = iNextRecord
    currentRecordNumber = nextRecordNumber
    nextRecord = prefetchNext()
    currentRecord
  }

  def prefetchNext(): Geometry = {
    while (pos < header.fileLength * 2 && (offsets == null || iNextRecord < offsets.length)) {
      if (offsets != null)
        skipTo(offsets(iNextRecord))
      iNextRecord += 1
      val posCurrentRecord: Int = pos.toInt
      nextRecordNumber = in.readInt(); pos += 4
      val currentRecordLength = in.readInt(); pos += 4
      val shapeType = IOUtil.readIntLittleEndian(in); pos += 4
      if (shapeType != header.shapeType)
        logWarning(s"Unexpected change in shape type $shapeType != ${header.shapeType} (header)")
      val hasZValues = shapeType / 10 == 1
      val hasMValues = hasZValues || shapeType / 10 == 2
      var numDimensions = 2
      if (hasZValues) numDimensions += 1
      if (hasMValues) numDimensions += 1
      val geometry: Geometry = (shapeType % 10) match {
        case ShapefileHeader.NullShape =>
          // This indicates a feature without a geometry a shapefile
          // Empty geometries are returned only when no filtering is associated
          if (filterRange == null) EmptyGeometry.instance else null
        case ShapefileHeader.PointShape =>
          val css = geometryFactory.getCoordinateSequenceFactory.create(1, numDimensions, if (hasMValues) 1 else 0)
          // Even with no M coordinates, some factories would produce a coordinate sequence with an additional dimension
          // To fix this issue, we set the M value to NaN to indicate that it's invalid
          if (css.hasM && !hasMValues)
            css.setOrdinate(0, numDimensions, Double.NaN)
          val x = IOUtil.readDoubleLittleEndian(in); pos += 8
          val y = IOUtil.readDoubleLittleEndian(in); pos += 8
          css.setOrdinate(0, 0, x)
          css.setOrdinate(0, 1, y)
          if (shapeType == ShapefileHeader.PointMShape) {
            css.setOrdinate(0, 2, IOUtil.readDoubleLittleEndian(in)); pos += 8;
          } else if (shapeType == ShapefileHeader.PointMZShape) {
            // M
            css.setOrdinate(0, 3, IOUtil.readDoubleLittleEndian(in)); pos += 8;
            // Z
            css.setOrdinate(0, 2, IOUtil.readDoubleLittleEndian(in)); pos += 8;
          }
          if (filterRange == null || filterRange.contains(x, y))
            if (x.isNaN || y.isNaN) geometryFactory.createPoint() else geometryFactory.createPoint(css)
          else
            null
        case ShapefileHeader.MultiPointShape =>
          // Read MBR
          val xmin: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val ymin: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val xmax: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val ymax: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val numPoints: Int = IOUtil.readIntLittleEndian(in); pos += 4
          // Calculate geometry size in bytes in case we want to skip it
          // Coordinate data + range bound for M and Z
          val geometrySizeInBytes = 8 * numPoints * numDimensions + 2 * 8 * (numDimensions - 2)

          if (filterRange != null && filterRange.disjoint(new Envelope(xmin, xmax, ymin, ymax))) {
            // Did not match geometry
            skipTo(posCurrentRecord + geometrySizeInBytes)
            null
          } else {
            val css = geometryFactory.getCoordinateSequenceFactory.create(numPoints, numDimensions, if (hasMValues) 1 else 0)
            // Fill in all (x, y) values
            for (iPoint <- 0 until numPoints) {
              css.setOrdinate(iPoint, 0, IOUtil.readDoubleLittleEndian(in)); pos += 8
              css.setOrdinate(iPoint, 1, IOUtil.readDoubleLittleEndian(in)); pos += 8
            }
            // Fill in remaining dimensions
            for (d <- 2 until numDimensions; iPoint <- 0 until numPoints) {
              // First, read the range even though we don't use it
              val minDimension = IOUtil.readDoubleLittleEndian(in); pos += 8
              val maxDimension = IOUtil.readDoubleLittleEndian(in); pos += 8
              css.setOrdinate(iPoint, d, IOUtil.readDoubleLittleEndian(in)); pos += 8
            }
            geometryFactory.createMultiPoint(css)
          }
        case ShapefileHeader.PolylineShape | ShapefileHeader.PolygonShape =>
          // Polylines and Polygons are very similar to each other so we handle them together
          // Read MBR
          val xmin: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val ymin: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val xmax: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val ymax: Double = IOUtil.readDoubleLittleEndian(in); pos += 8
          val numParts: Int = IOUtil.readIntLittleEndian(in); pos += 4
          val numPoints: Int = IOUtil.readIntLittleEndian(in); pos += 4
          // Total size of geometry in bytes in case we want to skip over it
          val geometrySizeInBytes = 4 * numParts + // Data for parts
            8 * numPoints * numDimensions + // Dimension data for points
            8 * 2 * (numDimensions - 2) // Ranges for extra dimensions, i.e., M and Z
          if (filterRange != null && filterRange.disjoint(new Envelope(xmin, xmax, ymin, ymax))) {
            // Geometry does not match the filter range
            skipTo(pos + geometrySizeInBytes)
            null
          } else {
            val firstPointInPart = new Array[Int](numParts + 1)
            for (iPart <- 0 until numParts) {
              firstPointInPart(iPart) = IOUtil.readIntLittleEndian(in); pos += 4
            }
            firstPointInPart(numParts) = numPoints
            val parts = new Array[CoordinateSequence](numParts)
            // Read (x, y) for all parts
            for (iPart <- parts.indices) {
              val numPointsInPart = firstPointInPart(iPart + 1) - firstPointInPart(iPart)
              parts(iPart) = geometryFactory.getCoordinateSequenceFactory
                .create(numPointsInPart, numDimensions, if (hasMValues) 1 else 0)
              for (iPoint <- 0 until numPointsInPart) {
                parts(iPart).setOrdinate(iPoint, 0, IOUtil.readDoubleLittleEndian(in)); pos += 8
                parts(iPart).setOrdinate(iPoint, 1, IOUtil.readDoubleLittleEndian(in)); pos += 8
              }
              if (shapeType % 10 == ShapefileHeader.PolygonShape) {
                // Some files are not properly written so that the first and last points might not match
                // Override this here by making them equal to make sure the code will not break
                parts(iPart).setOrdinate(parts(iPart).size - 1, 0, parts(iPart).getOrdinate(0, 0))
                parts(iPart).setOrdinate(parts(iPart).size - 1, 1, parts(iPart).getOrdinate(0, 1))
              }
            }
            // Read remaining dimensions
            for (d <- 2 until numDimensions) {
              // First, read the range of this dimension even though we don't use it
              val minDimension = IOUtil.readDoubleLittleEndian(in); pos += 8
              val maxDimension = IOUtil.readDoubleLittleEndian(in); pos += 8
              for (iPart <- parts.indices) {
                val numPointsInPart = firstPointInPart(iPart + 1) - firstPointInPart(iPart)
                for (iPoint <- 0 until numPointsInPart) {
                  parts(iPart).setOrdinate(iPoint, d, IOUtil.readDoubleLittleEndian(in)); pos += 8
                }
              }
            }
            // Now, combine parts into geometries according to the geometry type
            if (shapeType % 10 == ShapefileHeader.PolylineShape) {
              if (numParts == 0) // Empty linestring
                geometryFactory.createLineString()
              else if (numParts == 1) // Line string with one part
                geometryFactory.createLineString(parts(0))
              else // Multiline string
                geometryFactory.createMultiLineString(parts.map(part => geometryFactory.createLineString(part)))
            } else {
              // Polygon or Multipolygon
              assert(shapeType % 10 == ShapefileHeader.PolygonShape, s"Unexpected shape type $shapeType")
              if (numParts == 0) // Empty polygon
                geometryFactory.createPolygon()
              else {
                val polygons = new collection.mutable.ArrayBuffer[Polygon]()
                // First part should always be in clock-wise order because it's an outer shell
                assert(isClockWiseOrder(parts(0)))
                var i1: Int = 0
                while (i1 < numParts) {
                  var i2: Int = i1 + 1
                  while (i2 < numParts && !isClockWiseOrder(parts(i2)))
                    i2 += 1
                  // Create a polygon from all the parts found
                  val shell = geometryFactory.createLinearRing(parts(i1))
                  val holes = parts.slice(i1 + 1, i2).map(cs => geometryFactory.createLinearRing(cs))
                  polygons.append(geometryFactory.createPolygon(shell, holes))
                  i1 = i2
                }
                if (polygons.size == 1) polygons.head else geometryFactory.createMultiPolygon(polygons.toArray)
              }
            }
          }
      }
      assert(currentRecordLength * 2 + 8 == pos - posCurrentRecord,
        s"Expected record length is ${currentRecordLength * 2 + 8} but read ${pos-posCurrentRecord} bytes")
      if (geometry != null)
        return geometry
    }
    // Reached end of file
    null
  }

  override def close(): Unit = in.close()
}

object ShapefileGeometryReader {
  def isClockWiseOrder(cs: CoordinateSequence): Boolean = {
    // See https://stackoverflow.com/questions/1165647/how-to-determine-if-a-list-of-polygon-points-are-in-clockwise-order
    var sum: Double = 0.0
    var x1: Double = cs.getX(0)
    var y1: Double = cs.getY(0)
    for (i <- 1 until cs.size()) {
      val x2: Double = cs.getX(i)
      val y2: Double = cs.getY(i)
      sum += (x2 - x1) * (y2 + y1)
      x1 = x2
      y1 = y2
    }
    sum > 0
  }
}