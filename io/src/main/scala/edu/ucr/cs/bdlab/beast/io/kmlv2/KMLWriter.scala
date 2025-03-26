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
package edu.ucr.cs.bdlab.beast.io.kmlv2

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryCollection, LineString, Polygon}

import java.io.OutputStream
import javax.xml.stream.{XMLOutputFactory, XMLStreamWriter}

/**
 * Writes spatial features to KML format
 * @param out the output to write the KML to
 * @param schema the schema of records
 */
class KMLWriter(out: OutputStream, schema: StructType) extends AutoCloseable {

  /** The XML stream writer to write to */
  val sw: XMLStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(out, "UTF-8")

  /** The index of the geometry attribute */
  val iGeom: Int = schema.indexWhere(_.dataType == GeometryDataType)

  /** The index of all non-geometry attribute */
  val iNonGeom: Array[Int] = (0 until schema.length).toArray.filter(_ != iGeom)

  // Write the header of the file
  writeHeader(sw)

  /**
   * Writes the header of the KML file before any features are written
   *
   * @param sw the XML writer
   * @throws XMLStreamException if an error happens while writing the XML output
   */
  protected def writeHeader(sw: XMLStreamWriter): Unit = {
    sw.writeStartDocument("UTF-8", "1.0")
    sw.writeStartElement("kml")
    sw.writeAttribute("xmlns", "http://www.opengis.net/kml/2.2")
    sw.writeStartElement("Document")
  }

  def write(row: InternalRow): Unit = {
    sw.writeStartElement("Placemark") // <Placemark>
    sw.writeStartElement("ExtendedData") // <ExtendedData>
    for (iAttr <- iNonGeom) {
      val value = row.get(iAttr, schema(iAttr).dataType)
      if (value != null) {
        val name = schema(iAttr).name
        // TODO write numeric data as numeric not string
        sw.writeStartElement("Data")
        sw.writeAttribute("name", name)
        sw.writeStartElement("value")
        sw.writeCharacters(String.valueOf(value))
        sw.writeEndElement()
        sw.writeEndElement()
      }
    }
    sw.writeEndElement() // </ExtendedData>
    // Write the geometry
    val geom = GeometryDataType.getGeometryFromRow(row, iGeom)
    if (geom != null && !geom.isEmpty)
      writeGeometry(sw, geom)
    sw.writeEndElement() // </Placemark>
  }

  /**
   * Writes a single geometry value in KML format using the given XMLStreamWriter (writer).
   *
   * @param sw   the XML writer to use
   * @param geom the geometry to write in KML
   * @see <a href="http://www.opengis.net/doc/IS/kml/2.3">http://www.opengis.net/doc/IS/kml/2.3</a>
   */
  def writeGeometry(sw: XMLStreamWriter, geom: Geometry): Unit = { // Write field value
    geom.getGeometryType match {
      case "Point" =>
        //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#446
        sw.writeStartElement("Point")
        sw.writeStartElement("coordinates")
        writeCoordinate(sw, geom.getCoordinate)
        sw.writeEndElement()
        sw.writeEndElement()
      case "LineString" => writeLineString(sw, geom.asInstanceOf[LineString])
      case "Envelope" => writeEnvelope(sw, geom.asInstanceOf[EnvelopeND])
      case "Polygon" => writePolygon(sw, geom.asInstanceOf[Polygon])
      case "MultiPoint" | "MultiLineString" | "MultiPolygon" | "GeometryCollection" =>
        writeGeometryCollection(sw, geom.asInstanceOf[GeometryCollection])
      case _ =>
        throw new RuntimeException(s"Geometry type '${geom.getGeometryType}' is not yet supported in KML")
    }
  }

  /**
   * Write one coordinate
   *
   * @param sw         the xml writer
   * @param coordinate the coordinate
   */
  private def writeCoordinate(sw: XMLStreamWriter, coordinate: Coordinate): Unit = {
    sw.writeCharacters(coordinate.getX.toString)
    sw.writeCharacters(",")
    sw.writeCharacters(coordinate.getY.toString)
  }

  /**
   * Write a coordinate sequence
   *
   * @param sw the xml writer
   * @param cs the coordinate sequence
   */
  private def writeCoordinateSequence(sw: XMLStreamWriter, cs: CoordinateSequence): Unit = {
    sw.writeStartElement("coordinates")
    for (i <- 0 until cs.size) {
      sw.writeCharacters(cs.getX(i).toString)
      sw.writeCharacters(",")
      sw.writeCharacters(cs.getY(i).toString)
      sw.writeCharacters(" ")
    }
    sw.writeEndElement() // coordinates
  }

  /**
   * Write a geometry collection
   *
   * @param sw                 the xml writer
   * @param geometryCollection the geometry colleciton
   */
  private def writeGeometryCollection(sw: XMLStreamWriter, geometryCollection: GeometryCollection): Unit = {
    sw.writeStartElement("MultiGeometry") // Start of the geometry collection
    for (iGeom <- 0 until geometryCollection.getNumGeometries)
      writeGeometry(sw, geometryCollection.getGeometryN(iGeom))
    sw.writeEndElement() // End of the geometry collection
  }

  /**
   * Write a polygon
   *
   * @param sw      the xml writer
   * @param polygon the polygon
   */
  private def writePolygon(sw: XMLStreamWriter, polygon: Polygon): Unit = { //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#505
    sw.writeStartElement("Polygon")
    sw.writeStartElement("outerBoundaryIs")
    writeLineString(sw, polygon.getExteriorRing)
    sw.writeEndElement() // outerBoundaryIs

    for (iRing <- 0 until polygon.getNumInteriorRing) {
      sw.writeStartElement("innerBoundaryIs")
      writeLineString(sw, polygon.getInteriorRingN(iRing))
      sw.writeEndElement() // innerBoundaryIs

    }
    sw.writeEndElement() // Polygon
  }

  /**
   * Write an envelope as KML
   *
   * @param sw   the xml writer
   * @param geom the envelope to write
   */
  private def writeEnvelope(sw: XMLStreamWriter, geom: EnvelopeND): Unit = { // KML does not support envelopes as a separate geometry. So, we write it as a polygon
    sw.writeStartElement("Polygon")
    val envelope = geom
    sw.writeStartElement("outerBoundaryIs")
    sw.writeStartElement("LinearRing")
    sw.writeStartElement("coordinates")
    // first point
    sw.writeCharacters(envelope.getMinCoord(0).toString + ",")
    sw.writeCharacters(envelope.getMinCoord(1).toString + " ")
    // second point
    sw.writeCharacters(envelope.getMaxCoord(0).toString + ",")
    sw.writeCharacters(envelope.getMinCoord(1).toString + " ")
    // third point
    sw.writeCharacters(envelope.getMaxCoord(0).toString + ",")
    sw.writeCharacters(envelope.getMaxCoord(1).toString + " ")
    // fourth point
    sw.writeCharacters(envelope.getMinCoord(0).toString + ",")
    sw.writeCharacters(envelope.getMaxCoord(1).toString + " ")
    // fifth point (= first point)
    sw.writeCharacters(envelope.getMinCoord(0).toString + ",")
    sw.writeCharacters(envelope.getMinCoord(1).toString + " ")
    sw.writeEndElement() // End of coordinates
    sw.writeEndElement() // End of the linear ring
    sw.writeEndElement() // End of outerBoundaryIs
    sw.writeEndElement() // End of the polygon
  }

  /**
   * Write line string as KML
   *
   * @param sw         the xml writer
   * @param linestring the line string to write
   */
  private def writeLineString(sw: XMLStreamWriter, linestring: LineString): Unit = { //http://docs.opengeospatial.org/is/12-007r2/12-007r2.html#488
    sw.writeStartElement(linestring.getGeometryType) // LineString or LinearRing
    writeCoordinateSequence(sw, linestring.getCoordinateSequence)
    sw.writeEndElement()
  }

  override def close(): Unit = {
    // Close the main object
    sw.writeEndDocument()
    // Close the XMLStreamWriter and OutputStream
    sw.close()
    out.close()
  }
}
