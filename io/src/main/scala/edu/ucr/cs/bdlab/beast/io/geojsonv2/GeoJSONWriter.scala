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
package edu.ucr.cs.bdlab.beast.io.geojsonv2

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ByteType, DataType, FloatType, IntegerType, ShortType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryCollection, LineString, LinearRing, MultiLineString, MultiPolygon, Polygon}

import java.io.OutputStream

/**
 * A class that writes GeoJSON files from a stream of geometries.
 * @param out the output stream to write to
 * @param schema the schema that defines the names and types of attributes
 * @param jsonLineFormat whether to write JSON Line format where each record is in a separate line
 * @param prettyPrint whether to write the output JSON in a pretty indented format. Not compatible with jsonLineFormat
 * @param writeEmptyProperties whether to write an empty properties attribute even if all properties are null
 */
class GeoJSONWriter(out: OutputStream, schema: StructType, jsonLineFormat: Boolean = false, prettyPrint: Boolean = true,
                    writeEmptyProperties: Boolean = false) extends AutoCloseable {

  require(!prettyPrint || !jsonLineFormat, "Cannot enable both jsonLineFormat and prettyPrint")

  /** The generator used to create the output JSON file */
  val generator: JsonGenerator = new JsonFactory().createGenerator(out)
  if (prettyPrint)
    generator.setPrettyPrinter(new DefaultPrettyPrinter())

  /** The index of the geometry attributes or -1 if no geometry attribute is presented */
  val iGeom: Int = schema.indexWhere(_.dataType == GeometryDataType)

  /** The index of the ID attribute or -1 if no ID field exists */
  private val iID: Int = schema.indexWhere(f => f.name == "id" || f.name == "_id")

  /** The index of all remaining attributes that go into the "properties" field */
  private val iProperties: Array[Int] = (0 until schema.length).toArray.filter(i => i != iGeom && i != iID)

  // Write the header
  if (!jsonLineFormat)
    writeHeader(generator)

  /**
   * Write the header of the GeoJSON file
   * @param jsonGenerator the JSON generator to write to
   */
  private def writeHeader(jsonGenerator: JsonGenerator): Unit = {
    jsonGenerator.writeStartObject()
    jsonGenerator.writeStringField("type", "FeatureCollection")
    jsonGenerator.writeFieldName("features")
    jsonGenerator.writeStartArray()
  }

  /**
   * Write the footer of the GeoJSON file
   * @param jsonGenerator the JSON generator to write to
   */
  private def writeFooter(jsonGenerator: JsonGenerator): Unit = {
    jsonGenerator.writeEndArray()
    jsonGenerator.writeEndObject()
  }

  /**
   * Writes a single row to the output
   * @param row the row to write to the output as a GeoJSON file
   */
  def write(row: InternalRow): Unit = {
    // Write one feature
    generator.writeStartObject()
    generator.writeStringField("type", "Feature")
    // First, write the ID if exists
    if (iID != -1 && !row.isNullAt(iID)) {
      generator.writeFieldName(schema(iID).name)
      val id = row.get(iID, schema(iID).dataType)
      writeValue(id)
    }
    // Second, write the geometry field
    if (iGeom != -1) {
      val geometry = GeometryDataType.getGeometryFromRow(row, iGeom)
      generator.writeFieldName("geometry")
      writeGeometry(generator, geometry)
    }
    // Third, write all additional properties
    if (iProperties.nonEmpty || writeEmptyProperties) {
      generator.writeFieldName("properties")
      generator.writeStartObject()
      for (iProperty <- iProperties) {
        val value = row.get(iProperty, schema(iProperty).dataType)
        if (value != null) {
          val name = schema(iProperty).name
          generator.writeFieldName(name)
          writeValue(value)
        }
      }
      generator.writeEndObject() // End the properties object
    }
    generator.writeEndObject() // End the feature object
    if (jsonLineFormat) {
      generator.flush()
      out.write('\n')
    }
  }

  /**
   * Writes a single value of a specific type to the output
   * @param value the value to write
   */
  @inline private def writeValue(value: Any): Unit = {
    // TODO provide a better way of writing certain fields such as Date or DateTime
    value match {
      case x: Byte => generator.writeNumber(x)
      case x: Short => generator.writeNumber(x)
      case x: Int => generator.writeNumber(x)
      case x: Long => generator.writeNumber(x)
      case x: Float => generator.writeNumber(x)
      case x: Double => generator.writeNumber(x)
      case x: Boolean => generator.writeBoolean(x)
      case xx: Seq[_] =>
        generator.writeStartArray()
        for (x <- xx)
          writeValue(x)
        generator.writeEndArray()
      case yy: Map[String, _] =>
        generator.writeStartObject()
        for (kv <- yy) {
          generator.writeFieldName(kv._1)
          writeValue(kv._2)
        }
        generator.writeEndObject()
      case _ => generator.writeString(value.toString)
    }
  }

  /**
   * Write a geometry in the standard GeoJSON format
   * @param jsonGenerator the generator to write to
   * @param geometry the geometry to write
   */
  private def writeGeometry(jsonGenerator: JsonGenerator, geometry: Geometry): Unit = {
    jsonGenerator.writeStartObject()
    // Write field type
    val strType = geometry.getGeometryType match {
      case "Envelope" => "Polygon" // Treat envelopes as polygon for GeoJSON
      case other => other
    }
    jsonGenerator.writeStringField("type", strType)
    // Write field value
    geometry.getGeometryType match {
      case "Point" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Point
        jsonGenerator.writeFieldName("coordinates")
        writePoint(jsonGenerator, geometry.getCoordinate)
      case "LineString" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#LineString
        val linestring = geometry.asInstanceOf[LineString]
        jsonGenerator.writeFieldName("coordinates")
        jsonGenerator.writeStartArray()
        for (i <- 0 until linestring.getNumPoints)
          writePoint(jsonGenerator, linestring.getCoordinateN(i))
        jsonGenerator.writeEndArray()
      case "Envelope" =>
        // GeoJSON does not support envelopes as a separate geometry. So, we write it as a polygon
        val envelope = geometry.asInstanceOf[EnvelopeND]
        jsonGenerator.writeFieldName("coordinates")
        jsonGenerator.writeStartArray() // Start of polygon

        jsonGenerator.writeStartArray() // Start of the single linear ring inside the polygon

        // first point
        jsonGenerator.writeStartArray()
        jsonGenerator.writeNumber(envelope.getMinCoord(0))
        jsonGenerator.writeNumber(envelope.getMinCoord(1))
        jsonGenerator.writeEndArray()
        // second point
        jsonGenerator.writeStartArray()
        jsonGenerator.writeNumber(envelope.getMaxCoord(0))
        jsonGenerator.writeNumber(envelope.getMinCoord(1))
        jsonGenerator.writeEndArray()
        // third point
        jsonGenerator.writeStartArray()
        jsonGenerator.writeNumber(envelope.getMaxCoord(0))
        jsonGenerator.writeNumber(envelope.getMaxCoord(1))
        jsonGenerator.writeEndArray()
        // fourth point
        jsonGenerator.writeStartArray()
        jsonGenerator.writeNumber(envelope.getMinCoord(0))
        jsonGenerator.writeNumber(envelope.getMaxCoord(1))
        jsonGenerator.writeEndArray()
        // fifth point (= first point)
        jsonGenerator.writeStartArray()
        jsonGenerator.writeNumber(envelope.getMinCoord(0))
        jsonGenerator.writeNumber(envelope.getMinCoord(1))
        jsonGenerator.writeEndArray()
        jsonGenerator.writeEndArray() // End of the linear ring

        jsonGenerator.writeEndArray() // End of the polygon
      case "Polygon" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
        val polygon = geometry.asInstanceOf[Polygon]
        jsonGenerator.writeFieldName("coordinates")
        // Start the array of rings
        jsonGenerator.writeStartArray()
        for ($iRing <- 0 until polygon.getNumInteriorRing + 1) { // String the array of points in this ring
          jsonGenerator.writeStartArray()
          val ring = if ($iRing == 0) polygon.getExteriorRing
          else polygon.getInteriorRingN($iRing - 1)
          for (iPoint <- 0 until ring.getNumPoints)
            writePoint(jsonGenerator, ring.getCoordinateN(iPoint))
          // Close the array of points in this ring
          jsonGenerator.writeEndArray()
        }
        // Close the array of rings
        jsonGenerator.writeEndArray()
      case "MultiPoint" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPoint
        jsonGenerator.writeFieldName("coordinates")
        jsonGenerator.writeStartArray()
        for (iPoint <- 0 until geometry.getNumGeometries)
          writePoint(jsonGenerator, geometry.getGeometryN(iPoint).getCoordinate)
        jsonGenerator.writeEndArray() // End coordinates array
      case "MultiLineString" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiLineString
        val multiLineString = geometry.asInstanceOf[MultiLineString]
        jsonGenerator.writeFieldName("coordinates")
        jsonGenerator.writeStartArray()
        for (iLineString <- 0 until multiLineString.getNumGeometries) {
          jsonGenerator.writeStartArray()
          val subls = multiLineString.getGeometryN(iLineString).asInstanceOf[LineString]
          for (iPoint <- 0 until subls.getNumPoints)
            writePoint(jsonGenerator, subls.getCoordinateN(iPoint))
          jsonGenerator.writeEndArray() // End sub-linestring
        }
        jsonGenerator.writeEndArray()
      case "MultiPolygon" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPolygon
        val multiPolygon = geometry.asInstanceOf[MultiPolygon]
        jsonGenerator.writeFieldName("coordinates")
        jsonGenerator.writeStartArray() // Start of the multipolygon

        for (iPoly <- 0 until multiPolygon.getNumGeometries) {
          jsonGenerator.writeStartArray() // Start of the polygon

          val subpoly = multiPolygon.getGeometryN(iPoly).asInstanceOf[Polygon]
          // Write exterior ring
          for ($iRing <- 0 until subpoly.getNumInteriorRing + 1) {
            jsonGenerator.writeStartArray() // Start of the ring

            val ring = (if ($iRing == 0) subpoly.getExteriorRing
            else subpoly.getInteriorRingN($iRing - 1))
            for ($iPoint <- 0 until ring.getNumPoints) { // Write the point
              writePoint(jsonGenerator, ring.getCoordinateN($iPoint))
            }
            // Close the array of points in the current ring
            jsonGenerator.writeEndArray() // End of the current ring
          }
          jsonGenerator.writeEndArray() // End of the current polygon
        }
        jsonGenerator.writeEndArray() // End of the multipolygon
      case "GeometryCollection" =>
        // http://wiki.geojson.org/GeoJSON_draft_version_6#GeometryCollection
        val geometryCollection = geometry.asInstanceOf[GeometryCollection]
        jsonGenerator.writeFieldName("geometries")
        jsonGenerator.writeStartArray() // Start of the geometry collection

        for (iGeom <- 0 until geometryCollection.getNumGeometries)
          writeGeometry(jsonGenerator, geometryCollection.getGeometryN(iGeom))
        jsonGenerator.writeEndArray() // End of the geometry collection
      case _ =>
        throw new RuntimeException(s"Geometry type '${geometry.getGeometryType}' is not yet supported in GeoJSON")
    }
    jsonGenerator.writeEndObject()
  }

  private def writePoint(jsonGenerator: JsonGenerator, p: Coordinate): Unit = {
    jsonGenerator.writeStartArray()
    jsonGenerator.writeNumber(p.getX)
    jsonGenerator.writeNumber(p.getY)
    jsonGenerator.writeEndArray()
  }

  override def close(): Unit = {
    if (!jsonLineFormat)
      writeFooter(generator)
    generator.close()
  }
}
