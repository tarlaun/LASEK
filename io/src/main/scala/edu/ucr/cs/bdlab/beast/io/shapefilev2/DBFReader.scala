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

import edu.ucr.cs.bdlab.beast.util.IOUtil
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.io.DataInputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.GregorianCalendar

/**
 * Reads DBF entries
 * @param in an input stream to a DBF file initialized at the beginning
 */
class DBFReader(in: DataInputStream) extends Iterator[Array[Any]] with AutoCloseable {
  /** The header of the DBF file */
  val header: DBFHeader = DBFHelper.readHeader(in)

  /** A temporary array to read a single record */
  private val recordBytes: Array[Byte] = new Array[Byte](header.recordSize - 1)

  /** A flag that is raised when the end of file is reached */
  private var eofReached: Boolean = false

  /** The index of the record that will be returned next */
  var iCurrentRecord: Int = 0

  /** The index of the record to be returned next */
  var iNextRecord: Int = 0

  /** The record that will be returned next. null if end of file is reached */
  private var nextRecord: Array[Any] = prefetchNext

  lazy val schema: StructType = {
    StructType(header.fieldDescriptors.map(f => {
      val dataType = f.fieldType match {
        case DBFHelper.TypeString => StringType
        case DBFHelper.TypeNumeric | DBFHelper.TypeFloat if f.decimalCount == 0 && f.fieldLength < 9 => IntegerType
        case DBFHelper.TypeNumeric | DBFHelper.TypeFloat if f.decimalCount == 0 && f.fieldLength >= 9 => LongType
        case DBFHelper.TypeNumeric | DBFHelper.TypeFloat if f.decimalCount > 0 => DoubleType
        case DBFHelper.TypeBoolean => BooleanType
        case DBFHelper.TypeDate => DateType
        case DBFHelper.TypeDatetime => TimestampType
      }
      StructField(f.fieldName, dataType)
    }))
  }

  override def hasNext: Boolean = nextRecord != null

  private def prefetchNext: Array[Any] = {
    while (!eofReached) {
      iNextRecord += 1
      val marker = in.readByte()
      if (marker == DBFHelper.EOFMarker) {
        eofReached = true
      } else if (marker == DBFHelper.ValidRecordMarker) {
        // A valid record. Read it
        in.readFully(recordBytes)
        return parseRecord(recordBytes)
      } else if (marker == DBFHelper.DeletedRecordMarker) {
        // A deleted record. Skip over it
        in.skipBytes(recordBytes.length)
      } else {
        throw new RuntimeException(s"Error parsing DBF file. Invalid marker ${marker}")
      }
    }
    // EOF reached
    null
  }

  override def next(): Array[Any] = {
    iCurrentRecord = iNextRecord
    val currentRecord = nextRecord
    nextRecord = prefetchNext
    currentRecord
  }

  /**
   * Parse bytes of a record into an array of values
   * @param bytes data read from the file
   * @return an array of parsed values according to the header
   */
  private def parseRecord(bytes: Array[Byte]): Array[Any] = {
    val values = new Array[Any](header.fieldDescriptors.length)
    var startOffset: Int = 0
    for (iField <- header.fieldDescriptors.indices) {
      val field = header.fieldDescriptors(iField)
      var endOffsetInclusive: Int = (bytes.length - 1) min (startOffset + field.fieldLength - 1)
      values(iField) = field.fieldType match {
        case DBFHelper.TypeString =>
          // String. Skip leading spaces or null values
          while (endOffsetInclusive >= startOffset &&
            (bytes(endOffsetInclusive) == ' ' || bytes(endOffsetInclusive) == '\0'))
            endOffsetInclusive -= 1
          if (startOffset > endOffsetInclusive)
            null
          else
            new String(bytes, startOffset, endOffsetInclusive - startOffset + 1, StandardCharsets.UTF_8)
        case DBFHelper.TypeNumeric | DBFHelper.TypeFloat =>
          // Skip any leading blanks (or non-digits)
          var adjustedStartOffset = startOffset
          while (adjustedStartOffset <= endOffsetInclusive &&
              !(bytes(adjustedStartOffset) >= '0' && bytes(adjustedStartOffset) <= '9') &&
              bytes(adjustedStartOffset) != '.' && bytes(adjustedStartOffset) != '-')
            adjustedStartOffset += 1
          // Check if it is an empty value
          if (adjustedStartOffset > endOffsetInclusive)
            null
          else {
            val strValue = new String(bytes, adjustedStartOffset,
              endOffsetInclusive - adjustedStartOffset + 1, StandardCharsets.US_ASCII)
            if (field.decimalCount == 0 && field.fieldLength < 10) {
              // Return value as integer
              strValue.toInt
            } else if (field.decimalCount == 0 && field.fieldLength < 19) {
              // Return value as long
              strValue.toLong
            } else {
              strValue.toDouble
            }
          }
        case DBFHelper.TypeDouble =>
          val v = IOUtil.readDoubleLittleEndian(bytes, startOffset)
          if (v.isNaN) null else v
        case DBFHelper.TypeDate =>
          val yyyy = (bytes(startOffset) - '0') * 1000 + (bytes(startOffset + 1) - '0') * 100 +
            (bytes(startOffset + 2) - '0') * 10 + (bytes(startOffset + 3) - '0')
          val mm = (bytes(startOffset + 4) - '0') * 10 + (bytes(startOffset + 5) - '0')
          val dd = (bytes(startOffset + 6) - '0') * 10 + (bytes(startOffset + 7) - '0')
          val date = new GregorianCalendar(DBFHelper.UTC)
          date.clear() // Important to clear the time
          date.set(yyyy, mm - 1, dd)
          new java.sql.Date(date.getTimeInMillis)
        case DBFHelper.TypeDatetime =>
          // 8 bytes - two longs, first for date, second for time.
          // The date is the number of days since  01/01/4713 BC.
          // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
          val datepart = IOUtil.readIntLittleEndian(bytes, startOffset)
          val timepart = IOUtil.readIntLittleEndian(bytes, startOffset + 4)
          if (datepart == 0 && timepart == 0)
            null
          else
            new Timestamp(datepart * DBFHelper.MillisInOneDay + DBFHelper.DBFEpoch.getTimeInMillis + timepart)
        case DBFHelper.TypeBlockNumber =>
          // 10 digits representing a .DBT block number. The number is stored as a string,
          // right justified and padded with blanks.
          // Skip leading spaces
          while (startOffset <= endOffsetInclusive && bytes(startOffset) == ' ')
            startOffset += 1
          if (startOffset == endOffsetInclusive)
            null
          else
            new String(bytes, startOffset, endOffsetInclusive - startOffset + 1, StandardCharsets.UTF_8)
        case DBFHelper.TypeBoolean =>
          // Logical (Boolean) type
          // initialized to 0x20 (space) otherwise T or F
          bytes(startOffset) match {
            case ' ' => null
            case 'T' => true
            case 'F' => false
            case _ => null
          }
        case other => throw new RuntimeException(s"Field type '$other' not supported")
      }
      startOffset += field.fieldLength
    }
    values
  }

  override def close(): Unit = in.close()
}
