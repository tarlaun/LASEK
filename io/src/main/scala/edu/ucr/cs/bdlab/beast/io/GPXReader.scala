/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataTypes
import org.locationtech.jts.geom.CoordinateXY

import java.text.{ParseException, SimpleDateFormat}
import java.util.Calendar
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

@SpatialReaderMetadata(description = "Parse GPX files", shortName = "gpx", extension = ".gpx", filter = "*.gpx",
  noSplit = true)
class GPXReader extends FeatureReader with Logging {

  /**The underlying XML parser*/
  var xmlReader: XMLStreamReader = _

  /** The returned feature */
  var feature: IFeature = _

  /** Cache the file path to report errors */
  var filePath: Path = _

  /**The start offset of the split*/
  var start: Long = 0

  /**The end offset of the split*/
  var splitEnd: Long = 0

  /**A flag that is raised when the end-of-split is reached*/
  var eos: Boolean = _

  /**The track name currently being read*/
  var currentTrackName: String = _

  /**The track number currently being read*/
  var currentTrackNumber: Int = _

  /**The index of the track wihin the GPS file*/
  var currentTrackIndex: Int = _

  /**The segment number currently being read (starts at zero)*/
  var currentSegmentNumber: Int = _

  override def initialize(inputSplit: InputSplit, conf: BeastOptions): Unit = {
    val fsplit: FileSplit = inputSplit.asInstanceOf[FileSplit]
    this.filePath = fsplit.getPath
    this.start = fsplit.getStart
    this.splitEnd = fsplit.getStart + fsplit.getLength
    val xmlFactory = XMLInputFactory.newInstance()
    xmlFactory.setProperty(XMLInputFactory.IS_VALIDATING, false)
    val filesystem: FileSystem = fsplit.getPath.getFileSystem(conf.loadIntoHadoopConf())
    val inputStream: FSDataInputStream = filesystem.open(fsplit.getPath)
    inputStream.seek(fsplit.getStart)
    xmlReader = xmlFactory.createXMLStreamReader(inputStream)
    eos = false
    this.currentTrackName = null
    this.currentTrackNumber = -1
    this.currentSegmentNumber = -1
    this.currentTrackIndex = -1
  }

  /**
   * Returns the current file position
   * @return the current position of the file
   */
  def getFilePosition: Long = start + xmlReader.getLocation.getCharacterOffset

  val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val dateFormat2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")

  override def nextKeyValue(): Boolean = {
    if (eos) return false

    try {
      while (xmlReader.hasNext) {
        val token = xmlReader.next()
        token match {
          case XMLStreamConstants.START_ELEMENT =>
            val elementName = xmlReader.getName
            elementName.getLocalPart match {
              case "name" => this.currentTrackName = xmlReader.getElementText
              case "number" => this.currentTrackNumber = xmlReader.getElementText.toInt
              case "trkseg" => this.currentSegmentNumber += 1
              case "trk" => this.currentTrackIndex += 1
              case "trkpt" =>
                this.feature = readTrkPoint
                return true
              case _ => // Do nothing
            }
          case XMLStreamConstants.END_DOCUMENT => eos = true; return false
          case _ => None
        }
      }
    } catch {
      case e: javax.xml.stream.XMLStreamException =>
        throw new RuntimeException(s"Error parsing file '$filePath' at location $getFilePosition", e)
    }
    // No more tokens left
    eos = true
    false
  }

  private def readTrkPoint: IFeature = {
    // Found a point, retrieve Latitude and Longitude from the element attribute
    val longitude: Double = xmlReader.getAttributeValue(null, "lon").toDouble
    val latitude: Double = xmlReader.getAttributeValue(null, "lat").toDouble
    var elevation: Double = -1
    var time: Calendar = null
    // Retrieve the elevation and time from the contained elements
    while (xmlReader.next() != XMLStreamConstants.END_ELEMENT) {
      if (xmlReader.hasName && xmlReader.getName.getLocalPart == "ele") {
        // Retrieve elevation
        elevation = xmlReader.getElementText.toDouble
        // Retrieve and skip the END_ELEMENT token
        xmlReader.next
      } else if (xmlReader.hasName && xmlReader.getName.getLocalPart == "time") {
        // Retrieve time
        time = Calendar.getInstance()
        val timeStr = xmlReader.getElementText
        try {
          time.setTime(dateFormat1.parse(timeStr))
        } catch {
          case _: ParseException =>
            try {
              time.setTime(dateFormat2.parse(timeStr))
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
    val fieldValues = Array(
      if (elevation != -1) elevation else null,
      if (time != null) time else null,
      filePath.getName,
      if (currentTrackIndex != -1) currentTrackIndex else null,
      if (currentTrackNumber != -1) currentTrackNumber else null,
      if (currentTrackName != null) currentTrackName else null,
      if (currentSegmentNumber != -1) currentSegmentNumber else null
    )
    Feature.create(
      FeatureReader.DefaultGeometryFactory.createPoint(new CoordinateXY(longitude, latitude)),
      GPXReader.fieldNames,
      GPXReader.fieldTypes,
      fieldValues
    )
  }

  override def getCurrentValue: IFeature = feature

  override def getProgress: Float = Math.min(1.0f, (getFilePosition - start) / (splitEnd - start).toFloat);

  override def close(): Unit = xmlReader.close()
}

object GPXReader {
  val fieldNames = Array("ele", "time", "filename", "trackindex", "tracknumber", "trackname", "segmentnumber")

  val fieldTypes = Array(DataTypes.DoubleType, DataTypes.TimestampType, DataTypes.StringType, DataTypes.IntegerType,
    DataTypes.IntegerType, DataTypes.StringType, DataTypes.IntegerType
  )
}