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

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import edu.ucr.cs.bdlab.beast.io.SilentJsonParser
import edu.ucr.cs.bdlab.beast.io.geojsonv2.GeoJSONReader.{consumeAndCheckFieldName, consumeAndCheckToken, consumeNumber}
import org.apache.hadoop.fs.Seekable
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom._

import java.io.InputStream

/**
 * Reads a part of GeoJSON file.
 * @param in the input stream to read from
 * @param schema the schema that contains all required attributes
 * @param pos an object that tells the position to read from. If null, we use the bytes parsed by the JSON parser.
 * @param end the last position (according to [[pos]]) to consider in the input.
 * @param srid the spatial reference identifier to use for all created geometries
 */
class GeoJSONReader(in: InputStream, schema: StructType, pos: Seekable = null, end: Long = Long.MaxValue, srid: Int = 4326)
  extends Iterator[InternalRow] with AutoCloseable with Logging {

  /** The factory used to create all parsed geometries */
  val geometryFactory: GeometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), srid)

  /** The JSON parser for the underlying input */
  val jsonParser: JsonParser = new SilentJsonParser(new JsonFactory().createParser(in))

  /** End-of-stream has been reached for the input */
  var eos: Boolean = false

  /** The record that will be returned next or null if the end of input has been reached */
  var nextRecord: InternalRow = prefetchNext

  /**
   * The current position in the input. If [[pos]] is not null, we use it.
   * Otherwise, we use the position of the current token of the underlying JSON parser
   * @return the current position in the input
   */
  def inputPosition: Long = if (pos != null) pos.getPos else jsonParser.getTokenLocation.getByteOffset

  /**
   * Prefetch and return the next value from the input
   * @return the next value as an [[InternalRow]]
   */
  private def prefetchNext: InternalRow = {
    // This function reads the next feature record in the file. A correct spatial feature is an object that contains:
    // - A "type": "Feature" attribute.
    // - A nested object with {"type": "geometry", "coordinates": []} schema.
    // - An optional "properties": {} nested object
    // Once the object is found, it is returned
    if (eos)
      return null
    val values = new Array[Any](schema.length)
    var featureMarkerFound = false
    var posOfLastStartObject = inputPosition
    var featureComplete = false
    while (!featureComplete && jsonParser.nextToken() != null && posOfLastStartObject < end && !eos) {
      val token = jsonParser.currentToken
      token match {
        case JsonToken.START_OBJECT =>
          posOfLastStartObject = inputPosition
        case JsonToken.END_OBJECT =>
          if (featureMarkerFound) {
            featureComplete = true
          } else {
            // Clear all values and continue to find the next record
            for (i <- values.indices)
              values(i) = null
          }
        case JsonToken.NOT_AVAILABLE => eos = true // End of stream has been reached
        case JsonToken.FIELD_NAME =>
          val fieldName = jsonParser.currentName
          fieldName.toLowerCase match {
            case "type" =>
              val fieldValue = jsonParser.nextTextValue
              if (fieldValue.equalsIgnoreCase("feature"))
                featureMarkerFound = true
            case "geometry" =>
              consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT)
              val geometry = readGeometry(jsonParser)
              val geometryData = GeometryDataType.setGeometryInRow(geometry)
              values(schema.indexWhere(_.dataType == GeometryDataType)) = geometryData
            case "properties" =>
              // Read properties
              consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT)
              while (jsonParser.nextToken != JsonToken.END_OBJECT) {
                val name = jsonParser.currentName
                jsonParser.nextToken
                try {
                  val fieldIndex = schema.fieldIndex(name)
                  val fieldType = schema(fieldIndex).dataType
                  val value = parseCurrentValue(jsonParser, fieldType)
                  values(fieldIndex) = value
                } catch {
                  case e: IllegalArgumentException =>
                    logWarning(s"Unexpected field '$name' in schema: ${schema.fieldNames.mkString(",")}")
                }
              }
            case "features" => // Feature collection. Just ignore and continue
            case _ =>
              // Any unrecognized attribute is treated as an additional field. Works for "id" under features
              val name = jsonParser.currentName
              jsonParser.nextToken
              if (schema.exists(_.name == name)) {
                val fieldIndex: Int = schema.fieldIndex(name)
                val fieldType = schema(fieldIndex).dataType
                val value = parseCurrentValue(jsonParser, fieldType)
                values(fieldIndex) = value
              } else {
                // Skip until the end of this value
                skipCurrentValue(jsonParser)
              }
          }
        case _ => // Ignore any other token
      }
    }
    if (!featureMarkerFound) {
      eos = true
      return null
    }
    InternalRow.apply(values:_*)
  }

  /**
   * Read the current geometry object from the input. The assumption is that the START_OBJECT
   * token has already been consumed and this function will read and consume until the corresponding
   * END_OBJECT
   * @param jsonParser the parser to read from
   * @return the parsed geometry
   */
  private def readGeometry(jsonParser: JsonParser): Geometry = {
    // Clear the existing geometry to prepare it for possible reuse
    consumeAndCheckFieldName(jsonParser, "type")
    val geometryType = jsonParser.nextTextValue.toLowerCase
    try {
      geometryType match {
        case "point" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#Point
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          consumeNumber(jsonParser)
          val x = jsonParser.getDoubleValue
          consumeNumber(jsonParser)
          val y = jsonParser.getDoubleValue
          consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
          geometryFactory.createPoint(new CoordinateXY(x, y))
        case "linestring" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#LineString
          val coordinates = new collection.mutable.ArrayBuffer[Coordinate]()
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          while (jsonParser.nextToken != JsonToken.END_ARRAY) {
            consumeNumber(jsonParser)
            val x = jsonParser.getDoubleValue
            consumeNumber(jsonParser)
            val y = jsonParser.getDoubleValue
            coordinates.append(new CoordinateXY(x, y))
            consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
          }
          geometryFactory.createLineString(coordinates.toArray)
        case "polygon" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
          val parts = new collection.mutable.ArrayBuffer[LinearRing]
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          while (jsonParser.nextToken != JsonToken.END_ARRAY) {
            val coordinates = new collection.mutable.ArrayBuffer[Coordinate]
            // Read one linear ring
            while (jsonParser.nextToken ne JsonToken.END_ARRAY) {
              consumeNumber(jsonParser)
              val x = jsonParser.getDoubleValue
              consumeNumber(jsonParser)
              val y = jsonParser.getDoubleValue
              coordinates.append(new CoordinateXY(x, y))
              consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
            }
            parts.append(geometryFactory.createLinearRing(coordinates.toArray))
            coordinates.clear()
          }
          val shell = parts.remove(0)
          val holes = parts.toArray
          geometryFactory.createPolygon(shell, holes)
        case "multipoint" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPoint
          val points = new collection.mutable.ArrayBuffer[Point]
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          while (jsonParser.nextToken != JsonToken.END_ARRAY) {
            consumeNumber(jsonParser)
            val x = jsonParser.getDoubleValue
            consumeNumber(jsonParser)
            val y = jsonParser.getDoubleValue
            points.append(geometryFactory.createPoint(new Coordinate(x, y)))
            consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
          }
          geometryFactory.createMultiPoint(points.toArray)
        case "multilinestring" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiLineString
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          val parts = new collection.mutable.ArrayBuffer[LineString]
          while (jsonParser.nextToken != JsonToken.END_ARRAY) {
            val coordinates = new collection.mutable.ArrayBuffer[Coordinate]
            // Read one line string
            while (jsonParser.nextToken != JsonToken.END_ARRAY) {
              consumeNumber(jsonParser)
              val x = jsonParser.getDoubleValue
              consumeNumber(jsonParser)
              val y = jsonParser.getDoubleValue
              coordinates.append(new CoordinateXY(x, y))
              consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
            }
            parts.append(geometryFactory.createLineString(coordinates.toArray))
            coordinates.clear()
          }
          geometryFactory.createMultiLineString(parts.toArray)
        case "multipolygon" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPolygon
          consumeAndCheckFieldName(jsonParser, "coordinates")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          val polygons = new collection.mutable.ArrayBuffer[Polygon]
          while (jsonParser.nextToken != JsonToken.END_ARRAY) { // Read one polygon
            val parts = new collection.mutable.ArrayBuffer[LinearRing]
            while (jsonParser.nextToken != JsonToken.END_ARRAY) {
              val coordinates = new collection.mutable.ArrayBuffer[Coordinate]
              while (jsonParser.nextToken != JsonToken.END_ARRAY) {
                consumeNumber(jsonParser)
                val x = jsonParser.getDoubleValue
                consumeNumber(jsonParser)
                val y = jsonParser.getDoubleValue
                coordinates.append(new CoordinateXY(x, y))
                consumeAndCheckToken(jsonParser, JsonToken.END_ARRAY)
              }
              // Done with one linear ring
              parts.append(geometryFactory.createLinearRing(coordinates.toArray))
              coordinates.size
            }
            // Done with one polygon
            val shell = parts.remove(0)
            val holes = parts.toArray
            polygons.append(geometryFactory.createPolygon(shell, holes))
            parts.clear()
          }
          // Done with the multipolygon
          geometryFactory.createMultiPolygon(polygons.toArray)
        case "geometrycollection" =>
          // http://wiki.geojson.org/GeoJSON_draft_version_6#GeometryCollection
          val geoms = new collection.mutable.ArrayBuffer[Geometry]
          consumeAndCheckFieldName(jsonParser, "geometries")
          consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY)
          while (jsonParser.nextToken != JsonToken.END_ARRAY)
            geoms.append(readGeometry(jsonParser))
          geometryFactory.createGeometryCollection(geoms.toArray)
        case _ =>
          throw new RuntimeException(String.format("Unexpected geometry type '%s'", geometryType))
      }
    } finally {
      // We put this last required step in a finally block to avoid affecting the return value
      consumeAndCheckToken(jsonParser, JsonToken.END_OBJECT)
    }
  }

  /**
   * Read and discard all token until the end of the value currently pointed by the parser
   * @param parser the parser to skip token from
   */
  private def skipCurrentValue(parser: JsonParser): Unit = {
    var depth = 0
    var currentToken = parser.currentToken()
    do {
      currentToken match {
        case JsonToken.START_OBJECT | JsonToken.START_ARRAY => depth += 1
        case JsonToken.END_OBJECT | JsonToken.END_ARRAY => depth -= 1
        case _ => // Keep depth as-is
      }
      if (depth > 0)
        currentToken = parser.nextToken()
    } while (depth > 0 && currentToken != JsonToken.NOT_AVAILABLE)
  }

  /**
   * Parse the input value starting with the token currently pointed at by the input.
   * This function might read additional tokens to read the entire value.
   * For example, if the input is an array or an object, it will read and consume until the end of it.
   * @param parser the parser to read from
   * @param dataType the expected data type of this value.
   * @return the parsed value pointed at and should match the given dataType
   */
  private def parseCurrentValue(parser: JsonParser, dataType: DataType): Any = dataType match {
    case NullType => null
    case BooleanType if parser.currentToken == JsonToken.VALUE_TRUE => true
    case BooleanType if parser.currentToken == JsonToken.VALUE_FALSE => false
    case StringType => UTF8String.fromString(parser.getValueAsString)
    case ByteType => parser.getByteValue
    case ShortType => parser.getShortValue
    case IntegerType => parser.getIntValue
    case LongType => parser.getLongValue
    case FloatType => parser.getFloatValue
    case DoubleType => parser.getDoubleValue
    case ArrayType(elementType, _) =>
      assert(parser.currentToken == JsonToken.START_ARRAY)
      val arrayValues = new collection.mutable.ArrayBuffer[Any]
      while (parser.nextToken != JsonToken.END_ARRAY)
        arrayValues.append(parseCurrentValue(parser, elementType))
      arrayValues
    case MapType(StringType, valueType, _) =>
      assert(parser.currentToken == JsonToken.START_OBJECT)
      val names = new collection.mutable.ArrayBuffer[String]
      val values = new collection.mutable.ArrayBuffer[Any]
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        names.append(parser.getValueAsString)
        parser.nextToken
        values.append(parseCurrentValue(parser, valueType))
      }
      names.zip(values).toMap
  }

  override def hasNext: Boolean = nextRecord != null

  override def next(): InternalRow = {
    val currentRecord = nextRecord
    nextRecord = prefetchNext
    currentRecord
  }

  override def close(): Unit = in.close()
}

object GeoJSONReader extends Logging {
  /**
   * Infers the schema of a GeoJSON file by reading the first part of the file
   * @param in the input stream to read from
   * @param size how many bytes to read from from the input
   * @return the schema that contains all records in the first part of the file
   */
  def inferSchema(in: InputStream, size: Long): StructType = {
    val jsonFactory = new JsonFactory()
    val jsonParser: JsonParser = new SilentJsonParser(jsonFactory.createParser(in))
    // Start parsing from the beginning to search for the following:
    // "id" attribute inside a Feature object which indicate an ID attribute
    // "properties" attribute which indicates additional attributes

    // The object level at which an ID field was found or -1 if not found
    var idLevel: Int = -1
    // The type of value for the ID field, if found
    var idType: DataType = NullType
    // The object level at which a {"type": "feature"} attribute was found
    var typeFeatureLevel: Int = -1
    // The object level that corresponds to a geometry object
    var geometryLevel: Int = -1
    var currentLevel: Int = 0
    var fields = Map[String, DataType]()
    var eos = false
    while (!eos && jsonParser.getCurrentLocation.getByteOffset < size && jsonParser.nextToken() != JsonToken.NOT_AVAILABLE) {
      val token: JsonToken = jsonParser.currentToken()
      token match {
        case JsonToken.START_ARRAY | JsonToken.END_ARRAY => // Ignore. Mostly for array of features
        case JsonToken.START_OBJECT => currentLevel += 1
        case JsonToken.NOT_AVAILABLE | null => eos = true
        case JsonToken.END_OBJECT =>
          // Reset markers if the object ended
          if (typeFeatureLevel == currentLevel)
            typeFeatureLevel = -1
          if (idLevel == currentLevel) {
            val finalIDType = if (fields.contains("id"))
              promoteType(idType, fields("id"))
            else idType
            fields = fields + (("id", finalIDType))
            idLevel = -1
          }
          if (geometryLevel > currentLevel)
            geometryLevel = -1
          currentLevel -= 1
        case JsonToken.FIELD_NAME =>
          val name = jsonParser.getCurrentName
          name.toLowerCase match {
            case "features" => // Nothing. an object that contains all features
            case "type" =>
              val value = jsonParser.nextTextValue().toLowerCase
              if (value == "feature")
                typeFeatureLevel = currentLevel
              else if (value == "featurecollection") {
                // Nothing. Just ignore
              }
            case "geometry" =>
              geometryLevel = currentLevel
              // Skip until the end of this geometry
              val level = currentLevel
              consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT)
              currentLevel += 1
              while (currentLevel > level) {
                jsonParser.nextToken() match {
                  case JsonToken.START_OBJECT => currentLevel += 1
                  case JsonToken.END_OBJECT => currentLevel -= 1
                  case _ => // Ignore all other types
                }
              }
            case "properties" =>
              // Read and merge all attributes under properties
              consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT)
               while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                 val attributeName: String = jsonParser.currentName
                 val attributeType: DataType = inferValueType(jsonParser)
                 // TODO should we provide an option for case sensitivity in attribute names
                 if (fields.contains(attributeName)) {
                   val finalType = promoteType(attributeType, fields(attributeName))
                   fields = fields + ((attributeName, finalType))
                 } else {
                   fields = fields + ((attributeName, attributeType))
                 }
               }
            case "id" =>
              idLevel = currentLevel
              idType = inferValueType(jsonParser)
          }
      }
    }
    StructType(Seq(StructField("geometry", GeometryDataType)) ++ fields.toSeq.map(f => StructField(f._1, f._2)))
  }

  /**
   * Infers the [[DataType]] of the next value in the given parser
   * @param parser
   * @return
   */
  private def inferValueType(parser: JsonParser): DataType = {
    val token = parser.nextToken()
    token match {
      case JsonToken.VALUE_NULL => NullType
      case JsonToken.VALUE_FALSE | JsonToken.VALUE_TRUE => BooleanType
      case JsonToken.VALUE_STRING => StringType
      case JsonToken.VALUE_NUMBER_INT =>
        val valueLength = if (parser.getNumberValue.longValue() >= 0)
          parser.getNumberValue.toString.length
        else
          parser.getNumberValue.toString.length - 1
        if (valueLength < 9)
          IntegerType
        else
          LongType
      case JsonToken.VALUE_NUMBER_FLOAT => DoubleType
      case JsonToken.START_ARRAY =>
        // Array. Find a type that covers all elements of this array
        var allElementsType: DataType = NullType
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          val elementType: DataType = inferValueType(parser)
          allElementsType = promoteType(allElementsType, elementType)
        }
        ArrayType.apply(allElementsType)
      case JsonToken.START_OBJECT =>
        // A set of key-value pairs. Keys are always strings. Determine value type
        val keyType = StringType
        var allValuesType: DataType = NullType
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          assert(parser.currentToken == JsonToken.FIELD_NAME)
          val valueType = inferValueType(parser)
          allValuesType = promoteType(valueType, allValuesType)
        }
        MapType.apply(keyType, allValuesType)
      case other =>
        throw new RuntimeException(s"Cannot determine type for token ${other}")
    }
  }

  /**
   * Returns a data type that can support both given types, i.e., more general.
   * @param type1 first data type
   * @param type2 second data type
   * @return a data type that can support values of both types
   */
  private def promoteType(type1: DataType, type2: DataType): DataType = {
    if (type1 == type2)
      return type1
    (type1, type2) match {
      case (NullType, other) => other
      case (other, NullType) => other
      case (IntegerType, LongType) | (LongType, IntegerType) => LongType
      case (IntegerType, FloatType) | (FloatType, IntegerType) |
           (LongType, FloatType) | (FloatType, LongType) => FloatType
      case (IntegerType, DoubleType) | (DoubleType, IntegerType) |
           (LongType, DoubleType) | (DoubleType, LongType) |
           (FloatType, DoubleType) | (DoubleType, FloatType) => DoubleType
      case (StringType, _) | (_, StringType) => StringType
      case (DateType, TimestampType) | (TimestampType, DateType) => TimestampType
      case (ArrayType(t1, _), ArrayType(t2, _)) =>
        val combinedType = promoteType(t1, t2)
        ArrayType(combinedType)
      case (MapType(k1, v1, _), MapType(k2, v2, _)) =>
        val combinedKeyType = promoteType(k1, k2)
        val combinedValueType = promoteType(v1, v2)
        MapType.apply(combinedKeyType, combinedValueType)
      case _ => BinaryType
    }
  }

  private def consumeAndCheckToken(parser: JsonParser, expectedToken: JsonToken): Unit = {
    val token = parser.nextToken()
    if (token != expectedToken) {
      val lineNumber = parser.getTokenLocation.getLineNr
      val characterNumber = parser.getTokenLocation.getColumnNr
      throw new RuntimeException(s"Error parsing GeoJSON file." +
        s" Expected token ${expectedToken} but found ${token} at line ${lineNumber} character ${characterNumber}")
    }
  }

  private def consumeAndCheckFieldName(jsonParser: JsonParser, expected: String): Unit = {
    consumeAndCheckToken(jsonParser, JsonToken.FIELD_NAME)
    val actual = jsonParser.getCurrentName
    if (!expected.equalsIgnoreCase(actual)) { // Throw a parse exception.
      // TODO use a specialized exception(s)
      val lineNumber = jsonParser.getTokenLocation.getLineNr
      val characterNumber = jsonParser.getTokenLocation.getColumnNr
      throw new RuntimeException("Error parsing GeoJSON. Expected field '%s' but found '%s' at line %d character %d"
        .format(expected, actual, lineNumber, characterNumber))
    }
  }

  /**
   * Read the next token and ensure it is a numeric token. Either Integer or Float
   */
  private def consumeNumber(jsonParser: JsonParser): Unit = {
    val actual = jsonParser.nextToken
    if ((actual ne JsonToken.VALUE_NUMBER_FLOAT) && (actual ne JsonToken.VALUE_NUMBER_INT)) { // Throw a parse exception.
      // TODO use a specialized exception(s)
      val lineNumber = jsonParser.getTokenLocation.getLineNr
      val characterNumber = jsonParser.getTokenLocation.getColumnNr
      throw new RuntimeException("Error parsing GeoJSON. Expected numeric value but found %s at line %d character %d"
        .format(actual, lineNumber, characterNumber))
    }
  }
}
