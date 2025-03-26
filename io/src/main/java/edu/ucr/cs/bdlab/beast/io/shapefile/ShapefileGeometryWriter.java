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
package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A record writer that writes geometries as shapes. The keys are ignored and the values (geometries) are written
 * to the output. This class writes the shapefile (.shp) and the index file (.shx) only. It should be combined with
 * a {@link DBFWriter} that writes the associated .dbf file that completes the minimal required structure
 * for the shapefile.
 */
public class ShapefileGeometryWriter implements AutoCloseable {

  /**The path of the desired shapefile (.shp)*/
  protected Path shpPath;

  /**The configuration*/
  protected Configuration conf;

  /**The temporary shapefile used to write the records locally before writing the final .shp file to HDFS*/
  protected File tempShpFile;

  /**The output stream that writes to the temporary shapefile (.shp)*/
  protected DataOutputStream tempShpOut;

  /**The temporary shape index file used to write the records locally before writing the final .shx file to HDFS*/
  protected File tempShxFile;

  /**The output stream that writes to the temporary shape index file (.shx)*/
  protected DataOutputStream tempShxOut;

  /**The type of the first shape written to the shapefile*/
  protected int shapeType;

  /**Current offset of the shapefile assuming that a 100-byte header has been written*/
  protected int currentOffset;

  /**An auto-increment counter for record number*/
  protected int nextRecordNumber;

  /**The minimum bounding rectangle (MBR) of the input*/
  protected EnvelopeND fileMBR;

  /**
   * Prepares the record writer to write to the given path. The given path is assumed to be for the shapefile (.shp).
   * This class will write the shapefile (.shp) and the corresponding index file (.shx) as well. The .shx file will
   * have the same name and path but a different extension (i.e., .shp is replaced with .shx).
   * @param shpPath the path of the .shp file
   * @param conf the system configuration
   * @throws IOException if an error happens while initializing the output
   */
  public void initialize(Path shpPath, Configuration conf) throws IOException {
    this.shpPath = shpPath;
    this.conf = conf;
    // HDFS does not allow random updates which makes it impossible to write the file header unless we know the total
    // number of records. To overcome this problem, we write the contents to a temporary file and write the actual
    // shapefile only after the file is finalized.
    tempShpFile = File.createTempFile(shpPath.getName(), ".shp.tmp");
    tempShpOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempShpFile)));
    tempShpFile.deleteOnExit();

    tempShxFile = File.createTempFile(shpPath.getName(), ".shx.tmp");
    tempShxOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempShxFile)));
    tempShxFile.deleteOnExit();

    // Initially, invalidate the shape type
    shapeType = -1;
    fileMBR = new EnvelopeND(GeometryReader.DefaultInstance.getGeometryFactory(), 2, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    currentOffset = 100; // Assuming that the 100-byte header will be written
    nextRecordNumber = 1; // Record numbers start at 1
  }

  public void write(Geometry value) throws IOException {
    String geometryType = value.getGeometryType();
    if (geometryType.equals("GeometryCollection")) {
      // Geometry collections are not supported in Shapefile. We flatten it to write all its contents.
      for (int i = 0; i < value.getNumGeometries(); i++)
        write(value.getGeometryN(i));
      return;
    }

    if (shapeType == -1) {
      // First geometry, record the geometry type
      switch (geometryType) {
        case "Empty":
          shapeType = ShapefileGeometryReader.NullShape;
          break;
        case "Point":
          shapeType = ShapefileGeometryReader.PointShape;
          break;
        case "MultiPoint":
          shapeType = ShapefileGeometryReader.MultiPointShape;
        case "Envelope":
        case "Polygon":
        case "MultiPolygon":
          shapeType = ShapefileGeometryReader.PolygonShape;
          break;
        case "LineString":
        case "MultiLineString":
          shapeType = ShapefileGeometryReader.PolylineShape;
          break;
        default:
          throw new RuntimeException("Unsupported geometry type " + value.getGeometryType());
      }
    }

    tempShpOut.writeInt(nextRecordNumber++);
    tempShxOut.writeInt(currentOffset / 2);
    // The content length of this record in bytes. Initially, 4 bytes for the shape type
    int contentLength = 4;

    // Expand the file MBR
    fileMBR.merge(value);
    // TODO support M and Z coordinates

    if (value.isEmpty()) {
      // An empty geometry is written according to the standard shape type of the file
      switch (shapeType) {
        case ShapefileGeometryReader.NullShape:
          contentLength += 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, shapeType);
          break;
        case ShapefileGeometryReader.PointShape:
          contentLength += 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, shapeType);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          break;
        case ShapefileGeometryReader.MultiPointShape:
          // MBR (4 doubles) + num points
          contentLength += 8 * 4 + 4;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, shapeType); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeIntLittleEndian(tempShpOut, 0); // Number of points
          break;
        case ShapefileGeometryReader.PolylineShape:
        case ShapefileGeometryReader.PolygonShape:
          // MBR (4 doubles) + num parts + num points
          contentLength += 8 * 4 + 4 + 4;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, shapeType); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeDoubleLittleEndian(tempShpOut, Double.NaN);
          IOUtil.writeIntLittleEndian(tempShpOut, 0); // Number of parts
          IOUtil.writeIntLittleEndian(tempShpOut, 0); // Number of points
          break;
        default:
          throw new RuntimeException("Unknown geometry type "+shapeType);
      }
    } else {
      // Write the record to the shape file and the index to the index file
      final Envelope shapembr = value.getEnvelopeInternal();
      switch (value.getGeometryType()) {
        case "Empty":
          contentLength += 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.NullShape);
          break;
        case "Point":
          contentLength += 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PointShape);
          Coordinate c = value.getCoordinate();
          IOUtil.writeDoubleLittleEndian(tempShpOut, c.getX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, c.getY());
          break;
        case "MultiPoint":
          // MBR (4 doubles) + Number of points + point coordinates
          contentLength += 8 * 4 + 4 + 2 * 8 * value.getNumGeometries();
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.MultiPointShape); // Shape type
          // Write MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinY());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxY());
          // Write number of points
          IOUtil.writeIntLittleEndian(tempShpOut, value.getNumGeometries());
          for (int iPoint = 0; iPoint < value.getNumGeometries(); iPoint++) {
            Coordinate coord = value.getGeometryN(iPoint).getCoordinate();
            IOUtil.writeDoubleLittleEndian(tempShpOut, coord.getX());
            IOUtil.writeDoubleLittleEndian(tempShpOut, coord.getY());
          }
          break;
        case "Envelope":
          EnvelopeND envelope = (EnvelopeND) value;
          // Box (4 doubles) + numParts (int) + numPoints (int) + parts (one entry int) + 5 points (2 doubles each)
          contentLength += 8 * 4 + 4 + 4 + 4 + 8 * 5 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PolygonShape); // Shape type
          // Write MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(1));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(1));
          // Number of parts (1)
          IOUtil.writeIntLittleEndian(tempShpOut, 1);
          // Number of points (5)
          IOUtil.writeIntLittleEndian(tempShpOut, 5);
          // Only one part and stars at 0
          IOUtil.writeIntLittleEndian(tempShpOut, 0);
          // Write the five points in CW order
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(1));

          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(1));

          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(1));

          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMaxCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(1));

          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(0));
          IOUtil.writeDoubleLittleEndian(tempShpOut, envelope.getMinCoord(1));
          break;
        case "LineString":
          LineString linestring = (LineString) value;
          // MBR (4 doubles) + num parts + num points + parts + points
          contentLength += 8 * 4 + 4 + 4 + 4 + linestring.getNumPoints() * 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PolylineShape); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinY());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxY());
          IOUtil.writeIntLittleEndian(tempShpOut, 1); // Number of parts (always one for a LineString)
          IOUtil.writeIntLittleEndian(tempShpOut, linestring.getNumPoints()); // Number of points
          IOUtil.writeIntLittleEndian(tempShpOut, 0); // With one part, the offset is always zero
          for (int iPoint = 0; iPoint < linestring.getNumPoints(); iPoint++) {
            c = linestring.getCoordinateN(iPoint);
            IOUtil.writeDoubleLittleEndian(tempShpOut, c.getX());
            IOUtil.writeDoubleLittleEndian(tempShpOut, c.getY());
          }
          break;
        case "MultiLineString":
          MultiLineString multilinestring = (MultiLineString) value;
          // MBR (4 doubles) + num parts + num points + parts + points
          contentLength += 8 * 4 + 4 + 4 + multilinestring.getNumGeometries() * 4 + multilinestring.getNumPoints() * 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PolylineShape); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinY());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxY());
          IOUtil.writeIntLittleEndian(tempShpOut, multilinestring.getNumGeometries()); // Number of parts
          IOUtil.writeIntLittleEndian(tempShpOut, multilinestring.getNumPoints()); // Number of points
          int firstPointInLineString = 0;
          for (int iPart = 0; iPart < multilinestring.getNumGeometries(); iPart++) {
            IOUtil.writeIntLittleEndian(tempShpOut, firstPointInLineString);
            firstPointInLineString += multilinestring.getGeometryN(iPart).getNumPoints();
          }
          for (int iPart = 0; iPart < multilinestring.getNumGeometries(); iPart++) {
            LineString lineString = (LineString) multilinestring.getGeometryN(iPart);
            for (int iPoint = 0; iPoint < lineString.getNumPoints(); iPoint++) {
              c = lineString.getCoordinateN(iPoint);
              IOUtil.writeDoubleLittleEndian(tempShpOut, c.getX());
              IOUtil.writeDoubleLittleEndian(tempShpOut, c.getY());
            }
          }
          break;
        case "Polygon":
          Polygon polygon = (Polygon) value;
          // MBR (4 doubles) + num parts (integer) + num points (integer) + parts + points
          contentLength += 8 * 4 + 4 + 4 + (polygon.getNumInteriorRing() + 1) * 4 + polygon.getNumPoints() * 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PolygonShape); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinY());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxY());
          IOUtil.writeIntLittleEndian(tempShpOut, (polygon.getNumInteriorRing() + 1)); // Number of parts
          IOUtil.writeIntLittleEndian(tempShpOut, polygon.getNumPoints());
          // Index of first point in each ring
          int firstPointInRing = 0;
          for (int iRing = 0; iRing < polygon.getNumInteriorRing() + 1; iRing++) {
            IOUtil.writeIntLittleEndian(tempShpOut, firstPointInRing);
            LineString ring = iRing == 0 ? polygon.getExteriorRing() : polygon.getInteriorRingN(iRing - 1);
            firstPointInRing += ring.getNumPoints();
          }
          // TODO make sure that a hole is written in CCW order while the outer ring is written in CW order
          for (int iRing = 0; iRing < polygon.getNumInteriorRing() + 1; iRing++) {
            LineString ring = iRing == 0 ? polygon.getExteriorRing() : polygon.getInteriorRingN(iRing - 1);
            for (int iPoint = 0; iPoint < ring.getNumPoints(); iPoint++) {
              c = ring.getCoordinateN(iPoint);
              IOUtil.writeDoubleLittleEndian(tempShpOut, c.getX());
              IOUtil.writeDoubleLittleEndian(tempShpOut, c.getY());
            }
          }
          break;
        case "MultiPolygon":
          MultiPolygon multipolygon = (MultiPolygon) value;
          int numRings = 0;
          for (int iPoly = 0; iPoly < multipolygon.getNumGeometries(); iPoly++)
            numRings += ((Polygon)multipolygon.getGeometryN(iPoly)).getNumInteriorRing() + 1;
          // MBR (4 doubles) + num parts (integer) + num points (integer) + parts + points
          contentLength += 8 * 4 + 4 + 4 + numRings * 4 + multipolygon.getNumPoints() * 8 * 2;
          tempShpOut.writeInt(contentLength / 2);
          IOUtil.writeIntLittleEndian(tempShpOut, ShapefileGeometryReader.PolygonShape); // Shape type
          // Shape MBR
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMinY());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxX());
          IOUtil.writeDoubleLittleEndian(tempShpOut, shapembr.getMaxY());
          IOUtil.writeIntLittleEndian(tempShpOut, numRings); // Number of parts
          IOUtil.writeIntLittleEndian(tempShpOut, multipolygon.getNumPoints());
          // Write indexes of first point in each ring
          firstPointInRing = 0;
          for (int iPoly = 0; iPoly < multipolygon.getNumGeometries(); iPoly++) {
            polygon = (Polygon) multipolygon.getGeometryN(iPoly);
            for (int iRing = 0; iRing < polygon.getNumInteriorRing() + 1; iRing++) {
              IOUtil.writeIntLittleEndian(tempShpOut, firstPointInRing);
              LineString ring = iRing == 0 ? polygon.getExteriorRing() : polygon.getInteriorRingN(iRing - 1);
              firstPointInRing += ring.getNumPoints();
            }
          }
          // Now, write polygon coordinates
          for (int iPoly = 0; iPoly < multipolygon.getNumGeometries(); iPoly++) {
            polygon = (Polygon) multipolygon.getGeometryN(iPoly);
            // TODO make sure that a hole is written in CCW order while the outer ring is written in CW order
            for (int iRing = 0; iRing < polygon.getNumInteriorRing() + 1; iRing++) {
              LineString ring = iRing == 0 ? polygon.getExteriorRing() : polygon.getInteriorRingN(iRing - 1);
              for (int iPoint = 0; iPoint < ring.getNumPoints(); iPoint++) {
                c = ring.getCoordinateN(iPoint);
                IOUtil.writeDoubleLittleEndian(tempShpOut, c.getX());
                IOUtil.writeDoubleLittleEndian(tempShpOut, c.getY());
              }
            }
          }
          break;
        default:
          throw new RuntimeException("Unsupported shape type :" + value.getGeometryType());
      }
    }
    currentOffset += 8 + contentLength;
    tempShxOut.writeInt(contentLength / 2);
  }

  /**
   * Get the current size of the .shp file
   * @return the current size of the shapefile in bytes including the 100-byte header
   */
  public long getCurrentSize() {
    return currentOffset;
  }

  @Override
  public void close() throws IOException {
    // Close the temporary file
    tempShpOut.close();
    tempShxOut.close();
    // Write the final shape and index files
    FileSystem fs = shpPath.getFileSystem(conf);
    FSDataOutputStream shpOut = fs.create(shpPath);
    ShapefileHeader header = new ShapefileHeader();
    header.fileLength = (int) ((100 + tempShpFile.length()) / 2);
    header.shapeType = shapeType;
    header.version = 1000;
    header.xmin = fileMBR.getMinCoord(0);
    header.ymin = fileMBR.getMinCoord(1);
    header.xmax = fileMBR.getMaxCoord(0);
    header.ymax = fileMBR.getMaxCoord(1);
    header.write(shpOut);
    // Write the file contents
    InputStream tempShpIn = new FileInputStream(tempShpFile);
    IOUtils.copyBytes(tempShpIn, shpOut, 16 * 1024);
    tempShpIn.close();
    shpOut.close();


    // Write the shape index file
    Path shxPath = new Path(shpPath.getParent(), FileUtil.replaceExtension(shpPath.getName(), ".shx"));
    FSDataOutputStream shxOut = fs.create(shxPath);
    header.fileLength = (int) ((100 + tempShxFile.length()) / 2);
    header.write(shxOut);
    // Write the file contents
    InputStream tempShxIn = new FileInputStream(tempShxFile);
    IOUtils.copyBytes(tempShxIn, shxOut, 16 * 1024);
    tempShxIn.close();
    shxOut.close();

    // Delete the temporary file even though we set them up to be deleted on exit for early clean up
    tempShpFile.delete();
    tempShxFile.delete();
  }
}
