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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes {@link IFeature} values in KML format as explained in
 * <a href="http://www.opengis.net/kml/2.2">http://www.opengis.net/kml/2.2</a> and
 * <a href="https://developers.google.com/kml">https://developers.google.com/kml</a>.
 * The output is a file with one object that contains a label "Document" that has all the
 * features under it.
 */
@FeatureWriter.Metadata(extension = ".kml", shortName = "kml")
public class KMLFeatureWriter extends FeatureWriter {
  /**
   * The output stream
   */
  protected OutputStream out;

  /**
   * The KML output stream writer
   */
  protected XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
  protected XMLStreamWriter sw;

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    try {
      this.out = out;
      sw = xmlOutputFactory.createXMLStreamWriter(out, "UTF-8");
    } catch (XMLStreamException e1) {
      e1.printStackTrace();
    }
    try {
      writeHeader(sw);
    } catch (XMLStreamException e) {
      e.printStackTrace();
    }
  }

  /**
   * Writes the header of the KML file before any features are written
   *
   * @param sw the XML writer
   * @throws XMLStreamException if an error happens while writing the XML output
   */
  protected void writeHeader(XMLStreamWriter sw) throws XMLStreamException {
    sw.writeStartDocument("UTF-8", "1.0");
    sw.writeStartElement("kml");
    sw.writeAttribute("xmlns", "http://www.opengis.net/kml/2.2");
    sw.writeStartElement("Document");
  }

  @Override
  public void write(IFeature f) throws IOException {
    writeFeature(sw, f);
  }

  public static void writeFeature(XMLStreamWriter sw, IFeature feature) {
    try {
      sw.writeStartElement("Placemark");
      sw.writeStartElement("ExtendedData");
      if (feature.length() > 1) {
        for (int iAttr : feature.iNonGeomJ()) {
          Object value = feature.get(iAttr);
          if (value != null) {
            String name = feature.getName(iAttr);
            if (name == null || name.length() == 0)
              name = String.format("attr%d", iAttr);
            // TODO write numeric data as numeric not string
            sw.writeStartElement("Data");
            sw.writeAttribute("name", name);
            sw.writeStartElement("value");
            sw.writeCharacters(String.valueOf(value));
            sw.writeEndElement();
            sw.writeEndElement();
          }
        }
      }
      sw.writeEndElement();
      // Write the geometry
      Geometry geom = feature.getGeometry();
      if (geom != null && !geom.isEmpty()) {
        writeGeometryValue(sw, geom);
        sw.writeEndElement();
      }
    } catch (XMLStreamException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Writes a single geometry value in KML format using the given XMLStreamWriter (writer).
   *
   * @param sw the XML writer to use
   * @param geom the geometry to write in KML
   * @throws XMLStreamException if an error happens while writing the XML output
   * @see <a href="http://www.opengis.net/doc/IS/kml/2.3">http://www.opengis.net/doc/IS/kml/2.3</a>
   */
  public static void writeGeometryValue(XMLStreamWriter sw, Geometry geom) throws XMLStreamException {
    // Write field value
    switch (geom.getGeometryType()) {
      case "Point":
        //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#446
        sw.writeStartElement("Point");
        sw.writeStartElement("coordinates");
        writeCoordinate(sw, geom.getCoordinate());
        sw.writeEndElement();
        sw.writeEndElement();
        break;
      case "LineString":
        writeLineString(sw, (LineString) geom);
        break;
      case "Envelope":
        writeEnvelope(sw, (EnvelopeND) geom);
        break;
      case "Polygon":
        writePolygon(sw, (Polygon) geom);
        break;
      case "MultiPoint":
      case "MultiLineString":
      case "MultiPolygon":
      case "GeometryCollection":
        writeGeometryCollection(sw, (GeometryCollection) geom);
        break;
      default:
        throw new RuntimeException(String.format("Geometry type '%s' is not yet supported in KML", geom.getGeometryType()));
    }
  }

  /**
   * Write one coordinate
   * @param sw the xml writer
   * @param coordinate the coordinate
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writeCoordinate(XMLStreamWriter sw, Coordinate coordinate) throws XMLStreamException {
    sw.writeCharacters(Double.toString(coordinate.getX()));
    sw.writeCharacters(",");
    sw.writeCharacters(Double.toString(coordinate.getY()));
  }

  /**
   * Write a coordinate sequence
   * @param sw the xml writer
   * @param cs the coordinate sequence
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writeCoordinateSequence(XMLStreamWriter sw, CoordinateSequence cs) throws XMLStreamException {
    sw.writeStartElement("coordinates");
    for (int $i = 0; $i < cs.size(); $i++) {
      sw.writeCharacters(Double.toString(cs.getX($i)));
      sw.writeCharacters(",");
      sw.writeCharacters(Double.toString(cs.getY($i)));
      sw.writeCharacters(" ");
    }
    sw.writeEndElement(); // coordinates
  }

  /**
   * Write a geometry collection
   * @param sw the xml writer
   * @param geometryCollection the geometry colleciton
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writeGeometryCollection(XMLStreamWriter sw, GeometryCollection geometryCollection) throws XMLStreamException {
    sw.writeStartElement("MultiGeometry");// Start of the geometry collection
    for (int $iGeom = 0; $iGeom < geometryCollection.getNumGeometries(); $iGeom++)
      writeGeometryValue(sw, (geometryCollection.getGeometryN($iGeom)));
    sw.writeEndElement();  // End of the geometry collection
  }

  /**
   * Write a polygon
   * @param sw the xml writer
   * @param polygon the polygon
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writePolygon(XMLStreamWriter sw, Polygon polygon) throws XMLStreamException {
    //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#505
    sw.writeStartElement("Polygon");
    sw.writeStartElement("outerBoundaryIs");
    writeLineString(sw, polygon.getExteriorRing());
    sw.writeEndElement(); // outerBoundaryIs
    for (int $iRing = 0; $iRing < polygon.getNumInteriorRing(); $iRing++) {
      sw.writeStartElement("innerBoundaryIs");
      writeLineString(sw, polygon.getInteriorRingN($iRing));
      sw.writeEndElement(); // innerBoundaryIs
    }
    sw.writeEndElement();// Polygon
  }

  /**
   * Write an envelope as KML
   * @param sw the xml writer
   * @param geom the envelope to write
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writeEnvelope(XMLStreamWriter sw, EnvelopeND geom) throws XMLStreamException {
    // KML does not support envelopes as a separate geometry. So, we write it as a polygon
    sw.writeStartElement("Polygon");
    EnvelopeND envelope = geom;
    sw.writeStartElement("outerBoundaryIs");
    sw.writeStartElement("LinearRing");
    sw.writeStartElement("coordinates");

    // first point
    sw.writeCharacters(Double.toString(envelope.getMinCoord(0)) + ",");
    sw.writeCharacters(Double.toString(envelope.getMinCoord(1)) + " ");

    // second point
    sw.writeCharacters(Double.toString(envelope.getMaxCoord(0)) + ",");
    sw.writeCharacters(Double.toString(envelope.getMinCoord(1)) + " ");

    // third point
    sw.writeCharacters(Double.toString(envelope.getMaxCoord(0)) + ",");
    sw.writeCharacters(Double.toString(envelope.getMaxCoord(1)) + " ");

    // fourth point
    sw.writeCharacters(Double.toString(envelope.getMinCoord(0)) + ",");
    sw.writeCharacters(Double.toString(envelope.getMaxCoord(1)) + " ");

    // fifth point (= first point)
    sw.writeCharacters(Double.toString(envelope.getMinCoord(0)) + ",");
    sw.writeCharacters(Double.toString(envelope.getMinCoord(1)) + " ");

    sw.writeEndElement(); // End of coordinates
    sw.writeEndElement(); // End of the linear ring
    sw.writeEndElement(); // End of outerBoundaryIs
    sw.writeEndElement();// End of the polygon
  }

  /**
   * Write line string as KML
   * @param sw the xml writer
   * @param linestring the line string to write
   * @throws XMLStreamException if an error happens during the write
   */
  private static void writeLineString(XMLStreamWriter sw, LineString linestring) throws XMLStreamException {
    //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#488
    sw.writeStartElement(linestring.getGeometryType()); // LineString or LinearRing
    writeCoordinateSequence(sw, linestring.getCoordinateSequence());
    sw.writeEndElement();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    try {
      //close the main object
      sw.writeEndDocument();
      //close the XMLStreamWriter and OutputStream
      sw.close();
      out.close();
    } catch (XMLStreamException e2) {
      e2.printStackTrace();
    }
  }
}