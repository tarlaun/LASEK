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
package edu.ucr.cs.bdlab.beast.io.gpxv2

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{CoordinateXY, GeometryFactory}

import java.io.InputStream
import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

/**
 * Reads a GPX file. Currently, this GPX reader needs to read the entire file from beginning to end.
 * It does not yet support splitting big files.
 * @param in an input initialized at the beginning of the GPX file
 * @param filename the name of the GPX file to add to each row
 */
class GPXReader2(in: InputStream, filename: String)
  extends Iterator[InternalRow] with AutoCloseable with Logging {

  /** The geometry factory to use with  */
  private val geometryFactory: GeometryFactory = new GeometryFactory()

  /** The XML reader that reads the GPX file */
  private val xmlReader: XMLStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(in)

  /** A flag that is raised when the end-of-split is reached */
  private var eos: Boolean = _

  /** The filename converted to UTF8 to be able to write it in the Row */
  private val filenameUTF8: UTF8String = UTF8String.fromString(filename)

  /**The track name currently being read*/
  private var currentTrackName: UTF8String = _

  /**The track number currently being read*/
  private var currentTrackNumber: Int = -1

  /**The index of the track within the GPS file*/
  private var currentTrackIndex: Int = -1

  /**The segment number currently being read (starts at zero)*/
  private var currentSegmentNumber: Int = -1

  /** Stores the record to be returned next or null if reached end of file */
  var nextRecord: InternalRow = prefetchNextValue

  def prefetchNextValue: InternalRow = {
    if (eos) return null
    try {
      while (xmlReader.hasNext) {
        val token = xmlReader.next()
        token match {
          case XMLStreamConstants.START_ELEMENT =>
            val elementName = xmlReader.getName
            elementName.getLocalPart match {
              case "name" => this.currentTrackName = UTF8String.fromString(xmlReader.getElementText)
              case "number" => this.currentTrackNumber = xmlReader.getElementText.toInt
              case "trkseg" => this.currentSegmentNumber += 1
              case "trk" => this.currentTrackIndex += 1
              case "trkpt" =>
                return readTrkPoint
              case _ => // Do nothing
            }
          case XMLStreamConstants.END_DOCUMENT =>
            eos = true
            return null
          case _ => // Skip anything else
        }
      }
    } catch {
      case e: javax.xml.stream.XMLStreamException =>
        throw new RuntimeException(s"Error parsing GPX file '$filename' " +
          s"at line #${xmlReader.getLocation.getLineNumber} @${xmlReader.getLocation.getColumnNumber}", e)
    }
    // No more tokens left
    eos = true
    null
  }

  /**
   * Read one point in the track
   * @return
   */
  private def readTrkPoint: InternalRow = {
    // Found a point, retrieve Latitude and Longitude from the element attribute
    val longitude: Double = xmlReader.getAttributeValue(null, "lon").toDouble
    val latitude: Double = xmlReader.getAttributeValue(null, "lat").toDouble
    var elevation: Double = -1
    var time: java.sql.Timestamp = null
    // Retrieve the elevation and time from the contained elements
    while (xmlReader.next() != XMLStreamConstants.END_ELEMENT) {
      if (xmlReader.hasName && xmlReader.getName.getLocalPart == "ele") {
        // Retrieve elevation
        elevation = xmlReader.getElementText.toDouble
        // Retrieve and skip the END_ELEMENT token
        xmlReader.next
      } else if (xmlReader.hasName && xmlReader.getName.getLocalPart == "time") {
        // Retrieve time
        val timeStr = xmlReader.getElementText
        try {
          time = new Timestamp(GPXReader2.dateFormat1.parse(timeStr).getTime)
        } catch {
          case _: ParseException =>
            try {
              time = new Timestamp(GPXReader2.dateFormat2.parse(timeStr).getTime)
            } catch {
              case _: ParseException =>
                logWarning(s"Could not parse time '$timeStr'")
                time = null
            }
        }
        // Retrieve and skip the END_ELEMENT token
        xmlReader.next
      }
    }
    // Create and return the row
    InternalRow.apply(
      GeometryDataType.setGeometryInRow(geometryFactory.createPoint(new CoordinateXY(longitude, latitude))),
      if (elevation != -1) elevation else null,
      time,
      filenameUTF8,
      if (currentTrackIndex != -1) currentTrackIndex else null,
      if (currentTrackNumber != -1) currentTrackNumber else null,
      if (currentTrackName != null) currentTrackName else null,
      if (currentSegmentNumber != -1) currentSegmentNumber else null
    )
  }

  override def hasNext: Boolean = nextRecord != null

  override def next(): InternalRow = {
    val currentRecord = nextRecord
    nextRecord = prefetchNextValue
    currentRecord
  }

  override def close(): Unit = xmlReader.close()
}

object GPXReader2 {
  val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val dateFormat2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")

  val schema: StructType = StructType(Seq(
    StructField("point", GeometryDataType), // The point location (x,y)
    StructField("elevation", DoubleType), // The elevation
    StructField("time", TimestampType), // The date-time of the point
    StructField("filename", StringType), // The name of the file that contains the point
    StructField("trackindex", IntegerType), // The index of the track within the file
    StructField("tracknumber", IntegerType), // The track number as mentioned in the file
    StructField("trackname", StringType), // The track name as mentioned in the file
    StructField("segmentnumber", IntegerType), // The segment number based on where it appears in the file

  ))
}