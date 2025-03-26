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

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.GeometryType;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ShapefileGeometryReader {
  private static final Log LOG = LogFactory.getLog(ShapefileGeometryReader.class);

  /**Marker for empty shapes in Shapefile*/
  public static final int NullShape = 0;

  /**Marker for point shapes in Shapefile with x and y coordinates*/
  public static final int PointShape = 1;

  /**Marker for multi-point shapes in Shapefile*/
  public static final int MultiPointShape = 8;

  /**Marker for polyline (linestring) shapes in Shapefile*/
  public static final int PolylineShape = 3;

  /**Marker for polygon shapes in Shapefile*/
  public static final int PolygonShape = 5;

  /**Marker for point shapes in Shapefile with x, y, and m attributes*/
  public static final int PointMShape = 21;

  /**Marker for multi-point shapes in Shapefile with x, y, and m attributes*/
  public static final int MultiPointMShape = 28;

  /**Marker for polyline (linestring) shapes in Shapefile with x, y, and m attributes*/
  public static final int PolylineMShape = 23;

  /**Marker for polygon shapes in Shapefile with x, y, and m attributes*/
  public static final int PolygonMShape = 25;

  /**Marker for point shapes in Shapefile with x, y, z, and m attributes*/
  public static final int PointMZShape = 11;

  /**Marker for multi-point shapes in Shapefile with x, y, z, and m attributes*/
  public static final int MultiPointMZShape = 18;

  /**Marker for polyline (linestring) shapes in Shapefile with x, y, z, and m attributes*/
  public static final int PolylineMZShape = 13;

  /**Marker for polygon shapes in Shapefile with x, y, z, and m attributes*/
  public static final int PolygonMZShape = 15;

  /**Marker for multipatch shapes in Shapefile with x, y, z, and m attributes*/
  public static final int MultiPatchMZShape = 31;

  /**The filename*/
  protected String filename;

  /**If the input is a ZIP file, this object stores that zip file*/
  protected ZipFile zipFile;

  /**The input stream to the shapefile*/
  private DataInputStream in;

  /**Header of the file being read*/
  private ShapefileHeader header;

  /**An optional MBB that can be used to filter records*/
  private EnvelopeND filterMBR;

  /**The MBB of the current record*/
  private EnvelopeND mbr;

  /**A mutable geometry used to iterate over the input file*/
  private Geometry geometry;

  /**The length of the record (in shapefile) currently being read*/
  private int currentRecordLength;

  /**The position of the reader in the current record starting at zero*/
  private long offsetOfCurrentRecord;

  /**The position in the shapefile*/
  protected int pos;

  /**The index of the current shape in the file one-based*/
  protected int iShape;

  /**A temporary buffer to read and parse a record*/
  protected ByteBuffer readBuffers;

  /**Header size for a polyline. Four 64-bit double for MBR, and two 32-bit int for number of parts and number of points*/
  protected static final int PolylineHeaderSize = 8 * 4 + 4 * 2;

  /**Header size for a multipoint. Four 64-bit double for MBR, and one 32-bit int for number of points*/
  protected static final int MultiPointHeaderSize = 8 * 4 + 4;

  /**Offsets of all records in bytes as they appear in the .shx file*/
  protected int[] recordOffsets;

  protected GeometryFactory factory = FeatureReader.DefaultGeometryFactory;

  public void initialize(Path path, BeastOptions conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf.loadIntoHadoopConf(null));
    recordOffsets = null;
    this.filename = path.getName();
    int i = filename.lastIndexOf('.');
    String extension = filename.substring(i).toLowerCase();
    if (extension.equals(".shp")) {
      // A path to a shapefile, open directly
      initialize(fs.open(path), conf);
      // Read the .shx file
      String shxFileName = this.filename.substring(0, i) + ".shx";
      Path shxFilePath = new Path(path.getParent(), shxFileName);
      if (fs.exists(shxFilePath)) {
        FSDataInputStream shxIn = fs.open(shxFilePath);
        try {
          readIndexFile(shxIn);
        } finally {
          shxIn.close();
        }
      }
    } else if (extension.equals(".zip")) {
      // Open the first shapefile encountered in the ZIP file
      if (fs instanceof LocalFileSystem) {
        // The ZIP file is stored locally, open it directly
        String fullPath = path.toUri().getPath();
        zipFile = new ZipFile(fullPath);
      } else {
        // The file is stored remotely. We have to copy it locally
        File tempZipFile = File.createTempFile(filename, ".shp");
        fs.copyToLocalFile(path, new Path(tempZipFile.toString()));
        zipFile = new ZipFile(tempZipFile);
      }
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      boolean shpFileFound = false;
      boolean shxFileFound = false;
      while (entries.hasMoreElements() && (!shpFileFound || !shxFileFound)) {
        ZipEntry entry = entries.nextElement();
        String entryName = entry.getName();
        if (entryName.toLowerCase().endsWith(".shp")) {
          // Found the shape file
          DataInputStream in = new DataInputStream(new BufferedInputStream(zipFile.getInputStream(entry)));
          initialize(in, conf);
          shpFileFound = true;
        } else if (entryName.toLowerCase().endsWith(".shx")) {
          // Found the index file
          shxFileFound = true;
          DataInputStream shxIn = new DataInputStream(new BufferedInputStream(zipFile.getInputStream(entry)));
          try {
            readIndexFile(shxIn);
          } finally {
            shxIn.close();
          }
        }
      }
      // Could not find any shapefile entries in the zip file
      if (!shpFileFound)
        throw new RuntimeException("Could not find any .shp files in the file "+path);
      // Could not find the index file in the zip file
      if (!shxFileFound)
        LOG.warn("Could not find any .shx files in the file "+path+". Assuming consecutive records in the .shp file");
    } else {
      throw new RuntimeException(String.format("Unsupported file extension '%s'", extension));
    }
  }

  /**
   * Fully read the entire index (.shx) file and load it into memory to iterate over the file.
   * @param shxIn the input stream to the .shx file
   * @throws IOException if an error happens while reading the file
   */
  protected void readIndexFile(DataInputStream shxIn) throws IOException {
    ShapefileHeader shxHeader = new ShapefileHeader();
    shxHeader.readFields(shxIn);
    // Number of records is the total file size - header size divided by 8-bytes per record
    int numRecords = (shxHeader.fileLength * 2 - 100) / 8;
    this.recordOffsets = new int[numRecords];
    for (int $i = 0; $i < numRecords; $i++) {
      int offset = shxIn.readInt();
      int length = shxIn.readInt();
      this.recordOffsets[$i] = offset * 2;
    }
  }

  protected void initialize(DataInputStream in, BeastOptions conf) throws IOException {
    this.in = in;
    header = new ShapefileHeader();
    header.readFields(this.in);
    pos = 100;
    mbr = new EnvelopeND(factory);
    String filterMBRStr = conf.getString(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] coords = new double[parts.length];
      for (int i = 0; i < coords.length; i++)
        coords[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(factory, coords.length / 2, coords);
    } else {
      this.filterMBR = null;
    }
    iShape = 0;
    readBuffers = ByteBuffer.allocate(PolylineHeaderSize);
    readBuffers.order(ByteOrder.LITTLE_ENDIAN);
    fetchNextRecord();
  }

  protected void setSRID(int srid) {
    this.factory = GeometryReader.getGeometryFactory(srid);
  }

  /**
   * Fetches the next record to start reading shapes
   * @return {@code true} if a record was fetched. {@code false} if EOF is reached.
   * @throws IOException
   */
  private boolean fetchNextRecord() throws IOException {
    if (pos >= header.fileLength * 2 || (recordOffsets != null && iShape >= recordOffsets.length))
      return false;
    offsetOfCurrentRecord = recordOffsets == null? pos : recordOffsets[iShape];
    iShape++;
    if (pos < offsetOfCurrentRecord) {
      // Need to skip some bytes to reach the next record
      in.skipBytes((int) (offsetOfCurrentRecord - pos));
      pos = (int) offsetOfCurrentRecord;
    }
    /**The record number (in shapefile) for the current record being read*/
    int currentRecordNumber = in.readInt();
    pos += 4;
    currentRecordLength = in.readInt(); pos += 4;
    return true;
  }

  public boolean nextKeyValue() throws IOException {
    while (true) {
      // Check if the current record has ended
      if (pos >= offsetOfCurrentRecord + currentRecordLength * 2) {
        if (!fetchNextRecord())
          return false;
      }
      // Fetch next shape from the current record
      int shapeType = IOUtil.readIntLittleEndian(in); pos += 4;
      GeometryType geometryType;
      switch (shapeType % 10) {
        case NullShape: geometryType = GeometryType.EMPTY; break;
        case PointShape: geometryType = GeometryType.POINT; break;
        case PolylineShape: geometryType = GeometryType.MULTILINESTRING; break;
        case PolygonShape: geometryType = GeometryType.MULTIPOLYGON; break;
        case MultiPointShape: geometryType = GeometryType.MULTIPOINT; break;
        default: throw new RuntimeException(String.format("Unsupported shape type '%s' in file '%s'", shapeType, filename));
      }
      if (shapeType != header.shapeType)
        LOG.warn(String.format("Unexpected change in shape type in file '%s'", filename));
      boolean hasZValues = shapeType / 10 == 1;
      boolean hasMValues = hasZValues || shapeType / 10 == 2;
      int numDimensions = 2;
      if (hasZValues) numDimensions++;
      if (hasMValues) numDimensions++;
      CoordinateSequence css;
      double x, y, z, m;
      double xmin, ymin, xmax, ymax;
      int numParts, numPoints, geometrySizeInBytes;
      int firstPointPosition, firstZValuePosition, firstMValuePosition;
      switch (geometryType) {
        case EMPTY:
          // This indicates a feature without a geometry a shapefile
          geometry = EmptyGeometry.instance;
          // Empty geometries are returned only when no filtering is associated
          if (filterMBR == null)
            return true;
          break;
        case POINT:
          css = factory.getCoordinateSequenceFactory().create(1, numDimensions, hasMValues? 0 : 1);
          // Even with no M coordinates, some factories would produce a coordinate sequence with an additional dimension
          // To fix this issue, we set the M value to NaN to indicate that it's invalid
          if (css.hasM() && !hasMValues)
            css.setOrdinate(0, numDimensions, Double.NaN);
          css.setOrdinate(0, 0, x = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in))); pos += 8;
          css.setOrdinate(0, 1, y = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in))); pos += 8;
          mbr.set(css.getCoordinate(0));
          if (shapeType == PointMShape) {
            css.setOrdinate(0, 2, m = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in))); pos += 8;
          } else if (shapeType == PointMZShape) {
            // M
            css.setOrdinate(0, 3, m = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in))); pos += 8;
            // Z
            css.setOrdinate(0, 2, z = Double.longBitsToDouble(IOUtil.readLongLittleEndian(in))); pos += 8;
          }
          assert offsetOfCurrentRecord + currentRecordLength * 2 + 8 == pos;
          if (filterMBR == null || filterMBR.intersectsEnvelope(mbr)) {
            this.geometry = Double.isNaN(x) && Double.isNaN(y)? factory.createPoint() : factory.createPoint(css);
            return true;
          }
          break;
        case MULTIPOINT:
          // Read header size (fixed size regardless of the shape size)
          in.readFully(readBuffers.array(), 0, MultiPointHeaderSize);
          ((java.nio.Buffer)readBuffers).limit(MultiPointHeaderSize); // To ensure we do not parse beyond the limit
          pos += MultiPointHeaderSize;
          // Read MBR
          xmin = readBuffers.getDouble(0);
          ymin = readBuffers.getDouble(8);
          xmax = readBuffers.getDouble(16);
          ymax = readBuffers.getDouble(24);
          mbr.set(new double[] {xmin, ymin}, new double[] {xmax, ymax});
          numPoints = readBuffers.getInt(32);
          // Calculate geometry size in bytes to read it fully
          // Coordinate data + bound data for M and Z
          geometrySizeInBytes = 8 * numPoints * numDimensions + 2 * 8 * (numDimensions - 2);

          // Verify the size is similar to the record header (geometry type: int + Header + points)
          assert 4 + MultiPointHeaderSize + geometrySizeInBytes == currentRecordLength * 2 :
              String.format("Expected size %d != actual size %d", currentRecordLength * 2, 4 + MultiPointHeaderSize + geometrySizeInBytes);

          // Adjust the size of the readBuffer to ensure that we parse the input correctly
          if (((java.nio.Buffer)readBuffers).capacity() < geometrySizeInBytes) {
            readBuffers = ByteBuffer.allocate(geometrySizeInBytes);
            readBuffers.order(ByteOrder.LITTLE_ENDIAN);
          } else
            ((java.nio.Buffer)readBuffers).limit(geometrySizeInBytes); // Ensures that we do not parse beyond the limit
          in.readFully(readBuffers.array(), 0, geometrySizeInBytes);
          pos += geometrySizeInBytes;
          assert offsetOfCurrentRecord + currentRecordLength * 2 + 8 == pos;

          firstZValuePosition = hasZValues? 2 * 8 * numPoints : 0;
          firstMValuePosition = firstZValuePosition + (hasMValues? 8 * (2 + numPoints) : 0);

          if (filterMBR == null || filterMBR.intersects((mbr))) {
            css = this.factory.getCoordinateSequenceFactory().create(numPoints, numDimensions, hasMValues? 0 : 1);
            // Even with no M coordinates, some factories would produce a coordinate sequence with an additional dimension
            // To fix this issue, we set the M value to NaN to indicate that it's invalid
            if (css.hasM() && !hasMValues)
              css.setOrdinate(0, numDimensions, Double.NaN);
            for (int $i = 0; $i < numPoints; $i++) {
              // X
              css.setOrdinate($i, 0, readBuffers.getDouble(8 * 2 * $i));
              // Y
              css.setOrdinate($i, 1, readBuffers.getDouble(8 * 2 * $i + 8));
              if (hasZValues && hasMValues) {
                // M
                css.setOrdinate($i, 3, readBuffers.getDouble(firstZValuePosition + 8 * $i));
                // Z
                css.setOrdinate($i, 2, readBuffers.getDouble(firstMValuePosition + 8 * $i));
              } else if (hasMValues)
                // M
                css.setOrdinate($i, 2, readBuffers.getDouble(firstMValuePosition + 8 * $i));
            }
            this.geometry = factory.createMultiPoint(css);
            return true;
          }
          break;
        case MULTILINESTRING:
        case MULTIPOLYGON:
          // Read header size (fixed size regardless of the shape size)
          in.readFully(readBuffers.array(), 0, PolylineHeaderSize);
          ((java.nio.Buffer)readBuffers).limit(PolylineHeaderSize); // To ensure we do not parse beyond the limit
          pos += PolylineHeaderSize;
          xmin = readBuffers.getDouble(0);
          ymin = readBuffers.getDouble(8);
          xmax = readBuffers.getDouble(16);
          ymax = readBuffers.getDouble(24);
          mbr.set(new double[] {xmin, ymin}, new double[] {xmax, ymax});
          numParts = readBuffers.getInt(32);
          numPoints = readBuffers.getInt(32 + 4);
          // x, y coordinates
          geometrySizeInBytes = 4 * numParts + 2 * 8 * numPoints;
          // Add measured values
          if (hasMValues)
            geometrySizeInBytes += 2 * 8 + numPoints * 8;
          if (hasZValues)
            geometrySizeInBytes += 2 * 8 + numPoints * 8;

          // Verify the size is similar to the record header (geometry type: int + Header + points)
          assert 4 + PolylineHeaderSize + geometrySizeInBytes == currentRecordLength * 2 :
            String.format("Incorrect size of record #%d. Expected size %d actual size in file is %d",
                iShape, 4 + PolylineHeaderSize + geometrySizeInBytes, currentRecordLength * 2);

          firstPointPosition = numParts * 4;
          firstZValuePosition = firstPointPosition + 2 * 8 * numPoints + 8 * 2;
          firstMValuePosition = firstZValuePosition + (hasZValues? 8 * numPoints + 8 * 2 : 0);

          // Adjust the size of the readBuffer to ensure that we parse the input correctly
          if (((java.nio.Buffer)readBuffers).capacity() < geometrySizeInBytes) {
            readBuffers = ByteBuffer.allocate(geometrySizeInBytes);
            readBuffers.order(ByteOrder.LITTLE_ENDIAN);
          } else
            ((java.nio.Buffer)readBuffers).limit(geometrySizeInBytes); // Ensures that we do not parse beyond the limit
          in.readFully(readBuffers.array(), 0, geometrySizeInBytes);
          pos += geometrySizeInBytes;

          assert offsetOfCurrentRecord + currentRecordLength * 2 + 8 == pos;

          List<Geometry> parts = new ArrayList<>();
          List<CoordinateSequence> subparts = new ArrayList<>();
          if (filterMBR == null || filterMBR.intersectsEnvelope(mbr)) {
            // The shape matches the filterMBR
            for (int iPart = 0; iPart < numParts; iPart++) {
              int firstPointInCurrentPart = readBuffers.getInt(iPart * 4);
              int lastPointInCurrentPart = (iPart + 1) < numParts ? readBuffers.getInt(iPart * 4 + 4) : numPoints;
              css = factory.getCoordinateSequenceFactory().create(lastPointInCurrentPart - firstPointInCurrentPart,
                  numDimensions + (hasMValues? 0 : 1), 1);
              for (int iPoint = firstPointInCurrentPart; iPoint < lastPointInCurrentPart; iPoint++) {
                // Fill in the coordinates of the current part
                x = readBuffers.getDouble(firstPointPosition + 8 * 2 * iPoint);
                y = readBuffers.getDouble(firstPointPosition + 8 * 2 * iPoint + 8);
                css.setOrdinate(iPoint - firstPointInCurrentPart, 0, x);
                css.setOrdinate(iPoint - firstPointInCurrentPart, 1, y);
                if (hasMValues) {
                  m = readBuffers.getDouble(firstMValuePosition + 8 * iPoint);
                  css.setOrdinate(iPoint - firstPointInCurrentPart, hasZValues? 3 : 2, m);
                }
                if (hasZValues) {
                  z = readBuffers.getDouble(firstZValuePosition + 8 * iPoint);
                  css.setOrdinate(iPoint - firstPointInCurrentPart, 2, z);
                }
              }
              // Now, decide what to do with the coordinate sequence based on the geometry type
              if (geometryType == GeometryType.MULTILINESTRING) {
                parts.add(factory.createLineString(css));
              } else if (geometryType == GeometryType.MULTIPOLYGON) {
                // Some files are not properly written so that the first and last points might not match
                // Override this here by making them equal to make sure the code will not break
                css.setOrdinate(css.size() - 1, 0, css.getOrdinate(0, 0));
                css.setOrdinate(css.size() - 1, 1, css.getOrdinate(0, 1));
                if (hasZValues || hasMValues)
                  css.setOrdinate(css.size() - 1, 2, css.getOrdinate(0, 2));
                if (hasZValues && hasMValues)
                  css.setOrdinate(css.size() - 1, 3, css.getOrdinate(0, 3));

                // Only for polygons, check if this ring is in CW order. If so, it indicates an outer shell which
                // is modeled as a new polygon
                if (isClockWiseOrder(css)) {
                  // Clockwise order implies outer ring (shell)
                  // This indicates that the previous polygon is now complete
                  if (!subparts.isEmpty()) {
                    parts.add(createPolygon(subparts));
                    subparts.clear();
                  }
                }
                // Now, add this coordinate sequence for the next ones
                subparts.add(css);
              }
            } // For iPart in parts
            // All parts have been added, create the final geometry
            Geometry finalGeometry;
            if (geometryType == GeometryType.MULTILINESTRING) {
              if (parts.size() == 1)
                finalGeometry = parts.get(0);
              else
                finalGeometry = factory.createMultiLineString(parts.toArray(new LineString[0]));
            } else {
              assert geometryType == GeometryType.MULTIPOLYGON;
              // Add last part
              parts.add(createPolygon(subparts));
              if (parts.size() == 1)
                finalGeometry = parts.get(0);
              else
                finalGeometry = factory.createMultiPolygon(parts.toArray(new Polygon[0]));
            }
            geometry = finalGeometry;
            return true;
          }
          break;
        default:
          throw new RuntimeException(String.format("Unsupported shape type '%s' in file '%s'", shapeType, filename));
      }
    }
  }

  private Geometry createPolygon(List<CoordinateSequence> rings) {
    if (rings.isEmpty())
      return factory.createPolygon();
    LinearRing shell = factory.createLinearRing(rings.remove(0));
    LinearRing[] holes = new LinearRing[rings.size()];
    for (int i = 0; i < rings.size(); i++)
      holes[i] = factory.createLinearRing(rings.get(i));
    return factory.createPolygon(shell, holes);
  }

  private static boolean isClockWiseOrder(CoordinateSequence cs) {
    // See https://stackoverflow.com/questions/1165647/how-to-determine-if-a-list-of-polygon-points-are-in-clockwise-order
    double sum = 0.0;
    int $i = 0;
    double x1 = cs.getX($i);
    double y1 = cs.getY($i);
    while (++$i < cs.size()) {
      double x2 = cs.getX($i);
      double y2 = cs.getY($i);
      sum += (x2 - x1) * (y2 + y1);
      x1 = x2;
      y1 = y2;
    }
    boolean cwOrder = sum > 0;
    return cwOrder;
  }

  public Geometry getCurrentValue() {
    return geometry;
  }

  public float getProgress() {
    return (float) pos / header.fileLength / 2.0f;
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
    if (zipFile != null) {
      zipFile.close();
      zipFile = null;
    }
  }
}
