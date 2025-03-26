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

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND
import edu.ucr.cs.bdlab.beast.util.{FileUtil, IOUtil}
import org.locationtech.jts.geom._

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream, RandomAccessFile}

/**
 * Writes a .shp file that contains the given set of geometries.
 * Since the header of the Shapefile contains the file size, we cannot write it in streaming mode.
 * Thus, this class can only write to the local file system.
 * It also writes two additional files with the same name but in .shx and .prj extensions.
 * @param shapefile the .shp file to write. In the same directory, we will write .shx and .prj file as well.
 */
class ShapefileGeometryWriter(shapefile: File) extends AutoCloseable {
  /** The index file */
  val shxFile: File = new File(shapefile.getParent, FileUtil.replaceExtension(shapefile.getName, ".shx"))

  /** The projection file */
  val prjFile: File = new File(shapefile.getParent, FileUtil.replaceExtension(shapefile.getName, ".prj"))

  /** Output to the Shapefile */
  var shpOut: DataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shapefile)))
  var shpPos: Long = 0

  /** An output stream to the temporary .shx file */
  var shxOut: DataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shxFile)))

  /** The MBR of all data in the input */
  var fileMBR: Envelope = new Envelope()

  /** Type of shapes in the input file */
  var shapeType: Int = 0

  /** Total number of records written to the output so far. Used to generated record numbers. */
  var numRecords: Int = 0

  def initialize(): Unit = {
    // Skip the header for now until we know the information
    shpOut.write(new Array[Byte](100))
    shpPos += 100
    // Create a temporary .shx file with an empty header
    shxOut.write(new Array[Byte](100))
  }

  initialize()

  def write(geometry: Geometry): Unit = {
    if (geometry.getGeometryType == "GeometryCollection") {
      // Geometry collections are not supported in Shapefile. We flatten it to write all its contents.
      for (i <- 0 until geometry.getNumGeometries)
        write(geometry.getGeometryN(i))
      return
    }
    val currentRecordPos: Int = shpPos.toInt
    shxOut.writeInt(currentRecordPos / 2)
    numRecords += 1
    shpOut.writeInt(numRecords); shpPos += 4 // Record number
    if (shapeType == 0) {
      shapeType = geometry.getGeometryType match {
        case "Empty" => ShapefileHeader.NullShape
        case "Point" => ShapefileHeader.PointShape
        case "MultiPoint" => ShapefileHeader.MultiPointShape
        case "LineString" | "LinearRing" | "MultiLineString" => ShapefileHeader.PolylineShape
        case "Envelope" | "Polygon" | "MultiPolygon" => ShapefileHeader.PolygonShape
      }
    }
    // The content length of this record in bytes. Initially, 4 bytes for the shape type
    var contentLength: Int = 4
    fileMBR.expandToInclude(geometry.getEnvelopeInternal)
    // TODO support M and Z coordinates
    if (geometry.isEmpty) {
      // An empty geometry is written according to the standard shape type of the file
      shapeType match {
        case ShapefileHeader.NullShape =>
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, shapeType); shpPos += 4
        case ShapefileHeader.PointShape =>
          contentLength += 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, shapeType); shpPos += 4
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
        case ShapefileHeader.MultiPointShape =>
          // MBR (4 doubles) + num points
          contentLength += 8 * 4 + 4
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, shapeType); shpPos += 4 // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, 0); shpPos += 4 // Number of points
        case ShapefileHeader.PolylineShape | ShapefileHeader.PolygonShape =>
          // MBR (4 doubles) + num parts + num points
          contentLength += 8 * 4 + 4 + 4
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, shapeType); shpPos += 4 // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, Double.NaN); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, 0); shpPos += 4 // Number of parts
          IOUtil.writeIntLittleEndian(shpOut, 0); shpPos += 4 // Number of points
      }
    } else {
      // Non-empty geometry
      val shapeMBR = geometry.getEnvelopeInternal
      geometry.getGeometryType match {
        case "Empty" =>
          contentLength += 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.NullShape); shpPos += 4
        case "Point" =>
          contentLength += 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PointShape); shpPos += 4
          val c: Coordinate = geometry.getCoordinate
          IOUtil.writeDoubleLittleEndian(shpOut, c.getX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, c.getY); shpPos += 8
        case "MultiPoint" =>
          // MBR (4 doubles) + Number of points + point coordinates
          contentLength += 8 * 4 + 4 + 2 * 8 * geometry.getNumGeometries
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.MultiPointShape); shpPos += 4 // Shape type
          // Write MBR
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinY); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxY); shpPos += 8
          // Write number of points
          IOUtil.writeIntLittleEndian(shpOut, geometry.getNumGeometries); shpPos += 4
          for (iPoint <- 0 until geometry.getNumGeometries) {
            val coord: Coordinate = geometry.getGeometryN(iPoint).getCoordinate
            IOUtil.writeDoubleLittleEndian(shpOut, coord.getX); shpPos += 8
            IOUtil.writeDoubleLittleEndian(shpOut, coord.getY); shpPos += 8
          }
        case "Envelope" =>
          val envelope: EnvelopeND = geometry.asInstanceOf[EnvelopeND]
          // Box (4 doubles) + numParts (int) + numPoints (int) + parts (one entry int) + 5 points (2 doubles each)
          contentLength += 8 * 4 + 4 + 4 + 4 + 8 * 5 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PolygonShape); shpPos += 4 // Shape type
          // Write MBR
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(1)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(1)); shpPos += 8
          // Number of parts (1)
          IOUtil.writeIntLittleEndian(shpOut, 1); shpPos += 4
          // Number of points (5)
          IOUtil.writeIntLittleEndian(shpOut, 5); shpPos += 4
          // Only one part and stars at 0
          IOUtil.writeIntLittleEndian(shpOut, 0); shpPos += 4
          // Write the five points in CW order
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(1)); shpPos += 8

          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(1)); shpPos += 8

          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(1)); shpPos += 8

          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMaxCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(1)); shpPos += 8

          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(0)); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, envelope.getMinCoord(1)); shpPos += 8
        case "LineString" =>
          val linestring: LineString = geometry.asInstanceOf[LineString]
          // MBR (4 doubles) + num parts + num points + parts + points
          contentLength += 8 * 4 + 4 + 4 + 4 + linestring.getNumPoints * 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PolylineShape); shpPos += 4 // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinY); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxY); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, 1); shpPos += 4 // Number of parts (always one for a LineString)
          IOUtil.writeIntLittleEndian(shpOut, linestring.getNumPoints); shpPos += 4 // Number of points
          IOUtil.writeIntLittleEndian(shpOut, 0); shpPos += 4 // With one part, the offset is always zero
          for (iPoint <- 0 until linestring.getNumPoints) {
            val c = linestring.getCoordinateN(iPoint)
            IOUtil.writeDoubleLittleEndian(shpOut, c.getX); shpPos += 8
            IOUtil.writeDoubleLittleEndian(shpOut, c.getY); shpPos += 8
          }
        case "MultiLineString" =>
          val multilinestring = geometry.asInstanceOf[MultiLineString]
          // MBR (4 doubles) + num parts + num points + parts + points
          contentLength += 8 * 4 + 4 + 4 + multilinestring.getNumGeometries * 4 + multilinestring.getNumPoints * 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PolylineShape); shpPos += 4 // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinY); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxY); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, multilinestring.getNumGeometries); shpPos += 4 // Number of parts
          IOUtil.writeIntLittleEndian(shpOut, multilinestring.getNumPoints); shpPos += 4 // Number of points
          var firstPointInLineString: Int = 0
          for (iPart <- 0 until multilinestring.getNumGeometries) {
            IOUtil.writeIntLittleEndian(shpOut, firstPointInLineString); shpPos += 4
            firstPointInLineString += multilinestring.getGeometryN(iPart).getNumPoints
          }
          for (iPart <- 0 until multilinestring.getNumGeometries) {
            val lineString = multilinestring.getGeometryN(iPart).asInstanceOf[LineString]
            for (iPoint <- 0 until lineString.getNumPoints) {
              val c = lineString.getCoordinateN(iPoint)
              IOUtil.writeDoubleLittleEndian(shpOut, c.getX); shpPos += 8
              IOUtil.writeDoubleLittleEndian(shpOut, c.getY); shpPos += 8
            }
          }
        case "Polygon" =>
          val polygon = geometry.asInstanceOf[Polygon]
          // MBR (4 doubles) + num parts (integer) + num points (integer) + parts + points
          contentLength += 8 * 4 + 4 + 4 + (polygon.getNumInteriorRing + 1) * 4 + polygon.getNumPoints * 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PolygonShape); shpPos += 4 // Shape type
          // Shape MBR; shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinY); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxY); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, polygon.getNumInteriorRing + 1); shpPos += 4 // Number of parts
          IOUtil.writeIntLittleEndian(shpOut, polygon.getNumPoints); shpPos += 4
          // Index of first point in each ring
          var firstPointInRing: Int = 0
          for (iRing <- 0 to polygon.getNumInteriorRing) {
            IOUtil.writeIntLittleEndian(shpOut, firstPointInRing); shpPos += 4
            val ring = if (iRing == 0) polygon.getExteriorRing else polygon.getInteriorRingN(iRing - 1)
            firstPointInRing += ring.getNumPoints
          }
          // TODO make sure that a hole is written in CCW order while the outer ring is written in CW order
          for (iRing <- 0 to polygon.getNumInteriorRing) {
            val ring: LineString = if (iRing == 0) polygon.getExteriorRing else polygon.getInteriorRingN(iRing - 1)
            for (iPoint <- 0 until ring.getNumPoints) {
              val c = ring.getCoordinateN(iPoint)
              IOUtil.writeDoubleLittleEndian(shpOut, c.getX); shpPos += 8
              IOUtil.writeDoubleLittleEndian(shpOut, c.getY); shpPos += 8
            }
          }
        case "MultiPolygon" =>
          val multipolygon = geometry.asInstanceOf[MultiPolygon]
          var numRings: Int = 0
          for (iPoly <- 0 until multipolygon.getNumGeometries)
            numRings += multipolygon.getGeometryN(iPoly).asInstanceOf[Polygon].getNumInteriorRing + 1
          // MBR (4 doubles) + num parts (integer) + num points (integer) + parts + points
          contentLength += 8 * 4 + 4 + 4 + numRings * 4 + multipolygon.getNumPoints * 8 * 2
          shpOut.writeInt(contentLength / 2); shpPos += 4
          IOUtil.writeIntLittleEndian(shpOut, ShapefileHeader.PolygonShape); shpPos += 4 // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMinY); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxX); shpPos += 8
          IOUtil.writeDoubleLittleEndian(shpOut, shapeMBR.getMaxY); shpPos += 8
          IOUtil.writeIntLittleEndian(shpOut, numRings); shpPos += 4 // Number of parts
          IOUtil.writeIntLittleEndian(shpOut, multipolygon.getNumPoints); shpPos += 4
          // Write indexes of first point in each ring
          var firstPointInRing: Int = 0
          for (iPoly <- 0 until multipolygon.getNumGeometries) {
            val polygon = multipolygon.getGeometryN(iPoly).asInstanceOf[Polygon]
            for (iRing <- 0 to polygon.getNumInteriorRing) {
              IOUtil.writeIntLittleEndian(shpOut, firstPointInRing); shpPos += 4
              val ring: LineString = if (iRing == 0) polygon.getExteriorRing else polygon.getInteriorRingN(iRing - 1)
              firstPointInRing += ring.getNumPoints
            }
          }
          // Now, write polygon coordinates
          for (iPoly <- 0 until multipolygon.getNumGeometries) {
            val polygon = multipolygon.getGeometryN(iPoly).asInstanceOf[Polygon]
            // TODO make sure that a hole is written in CCW order while the outer ring is written in CW order
            for (iRing <- 0 to polygon.getNumInteriorRing) {
              val ring: LineString = if (iRing == 0) polygon.getExteriorRing else polygon.getInteriorRingN(iRing - 1)
              for (iPoint <- 0 until ring.getNumPoints) {
                val c: Coordinate = ring.getCoordinateN(iPoint)
                IOUtil.writeDoubleLittleEndian(shpOut, c.getX); shpPos += 8
                IOUtil.writeDoubleLittleEndian(shpOut, c.getY); shpPos += 8
              }
            }
          }
      }
      assert(shpPos - currentRecordPos == 8 + contentLength)
      shxOut.writeInt(contentLength / 2)
    }
  }

  /**
   * Get the current size of the .shp file
   *
   * @return the current size of the shapefile in bytes including the 100-byte header
   */
  val currentSize: Long = shpPos

  override def close(): Unit = {
    // Close the files
    shpOut.close()
    shxOut.close()
    // Before closing, write the header first
    val shpFileLength: Long = shpPos
    val shpHeader = ShapefileHeader((shpFileLength / 2).toInt, 1000, shapeType, 0, 0, 0, 0)
    shpHeader.init(fileMBR)
    // Reopen the file in random mode to write the header
    val shpOutR = new RandomAccessFile(shapefile, "rw")
    shpOutR.seek(0)
    ShapefileHeader.writeHeader(shpOutR, shpHeader)
    shpOutR.close()

    // Write the header to the index file
    val shxFileLength: Long = 100 + numRecords * 8
    val shxHeader = ShapefileHeader((shxFileLength / 2).toInt, 1000, shapeType, 0, 0, 0, 0)
    shxHeader.init(fileMBR)
    val shxOutR = new RandomAccessFile(shxFile, "rw")
    shxOutR.seek(0)
    ShapefileHeader.writeHeader(shxOutR, shxHeader)
    shxOutR.close()
  }
}
