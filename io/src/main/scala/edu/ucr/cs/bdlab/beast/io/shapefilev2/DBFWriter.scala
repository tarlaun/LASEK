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

import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.hsqldb.lib.DataOutputStream

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Calendar

/**
 * Writes a DBF file. Since the format of the contents depend on the aggregates, e.g., string lengths,
 * we have to write all the contents to a temporary file, and then to the output.
 * @param out the output stream to write to
 */
class DBFWriter(out: OutputStream, schema: StructType) extends AutoCloseable {

  /** A temporary file to write the data to */
  val tempFile: File = File.createTempFile("temp-dbf", ".dbf.tmp")

  val tempOut: ObjectOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))

  /** Number of records written so far to the output */
  var numRecords: Int = 0

  /** Number of attributes in the file */
  def numFields: Int = schema.length

  /** Field attributes */
  val fieldTypes: Array[Char] = schema.map(field => field.dataType match {
    case ByteType | ShortType | IntegerType | LongType => DBFHelper.TypeNumeric
    case FloatType | DoubleType => DBFHelper.TypeFloat
    case DateType => DBFHelper.TypeDate
    case TimestampType => DBFHelper.TypeDatetime
    case StringType => DBFHelper.TypeString
    case BooleanType => DBFHelper.TypeBoolean
    case _ => DBFHelper.TypeString
  }).toArray
  val fieldLengths: Array[Short] = schema.map(field => field.dataType match {
    case DateType => 8.toShort
    case TimestampType => 8.toShort
    case BooleanType => 1.toShort
    case _ => 0.toShort
  }).toArray
  val fieldDecimalCounts: Array[Byte] = new Array[Byte](numFields)

  def write(value: Array[Any]): Unit = {
    // Write the value to the temporary file as-is. No formatting at this point
    numRecords += 1
    tempOut.writeObject(value)
    for (i <- value.indices) {
      // Update field length and decimal count
      if (fieldTypes(i) == DBFHelper.TypeNumeric || fieldTypes(i) == DBFHelper.TypeFloat ||
        fieldTypes(i) == DBFHelper.TypeString) {
        value(i) match {
          case null => // Do nothing
          case _: Byte | _: Short | _: Int | _:Long =>
            val length = value(i).toString.length
            fieldLengths(i) = fieldLengths(i) max length.toShort
          case x: Float => updateFieldLengthDouble(i, x)
          case x: Double => updateFieldLengthDouble(i, x)
          case s: String => fieldLengths(i) = fieldLengths(i) max s.getBytes(StandardCharsets.UTF_8).length.toShort
          case x => fieldLengths(i) = fieldLengths(i) max x.toString.getBytes(StandardCharsets.UTF_8).length.toShort
        }
      }
    }
  }

  def updateFieldLengthDouble(iField: Int, x: Double): Unit = {
    val str = "%.19f".format(x)
    var lastNonZero = str.length - 1
    while (lastNonZero > 0 && str(lastNonZero) == '0')
      lastNonZero -= 1
    val decimalPoint: Int = str.indexOf('.')
    val numFractionPoints = if (decimalPoint == -1) 0 else (lastNonZero - decimalPoint)
    fieldDecimalCounts(iField) = fieldDecimalCounts(iField) max numFractionPoints.toByte
    fieldLengths(iField) = fieldLengths(iField) max (lastNonZero + 1 + (if (x < 0) 1 else 0)).toShort
  }

  private def createHeader: DBFHeader = {
    // Calculate record size
    val recordSize = fieldLengths.sum + 1
    val headerSize = 32 /*Main header*/ +
      32 * numFields +
      1 /* Header record terminator */
    val fieldDescriptors = new Array[DBFFieldDescriptor](numFields)
    for (iField <- fieldDescriptors.indices)
      fieldDescriptors(iField) = DBFFieldDescriptor(schema.fieldNames(iField),
        fieldTypes(iField).toShort, fieldLengths(iField), fieldDecimalCounts(iField), 0, 0)
    DBFHeader(3, new java.util.Date(), numRecords, headerSize, recordSize, 0, 0, 0, 0, "UCRBeast", fieldDescriptors, null)
  }

  override def close(): Unit = {
    tempOut.close()
    val dout = new DataOutputStream(out)
    // Create and write the header
    val header = createHeader
    DBFHelper.writeHeader(dout, header)
    dout.writeByte(DBFHelper.EndOfHeader)
    // Write the data output
    val formatStrings: Array[String] = fieldDecimalCounts.map(d => s"%.${d}f")
    val tempIn = new ObjectInputStream(new FileInputStream(tempFile))
    val bout = ByteBuffer.allocate(header.recordSize)
    bout.order(ByteOrder.LITTLE_ENDIAN)
    for (iRecord <- 0 until numRecords) {
      bout.position(0)
      bout.put(DBFHelper.ValidRecordMarker)
      val record = tempIn.readObject().asInstanceOf[Array[Any]]
      for (iField <- record.indices) {
        val startFieldPos = bout.position()
        val value = record(iField)
        if (value == null) {
          // Write a default value that will be parsed back as null
          fieldTypes(iField) match {
            case DBFHelper.TypeString =>
              for (_ <- 0 until fieldLengths(iField))
                bout.put('\0'.toByte)
            case DBFHelper.TypeNumeric | DBFHelper.TypeFloat =>
              for (_ <- 0 until fieldLengths(iField))
                bout.put(' '.toByte)
            case DBFHelper.TypeBoolean =>
              bout.put(' '.toByte)
            case DBFHelper.TypeDatetime =>
              bout.putLong(0)
            case DBFHelper.TypeDate =>
              for (_ <- 0 until fieldLengths(iField))
                bout.put('0'.toByte)
          }
        } else {
          // Non-null value
          fieldTypes(iField) match {
            case DBFHelper.TypeString =>
              val strBytes = value.toString.getBytes(StandardCharsets.UTF_8)
              bout.put(strBytes)
              val diff = fieldLengths(iField) - strBytes.length
              assert(diff >= 0, "Value longer than expected")
              for (_ <- 0 until diff)
                bout.put(' '.toByte)
            case DBFHelper.TypeNumeric | DBFHelper.TypeFloat =>
              val strBytes = (value match {
                case _: Float | _: java.lang.Float | _: Double | _: java.lang.Double => formatStrings(iField).format(value)
                case _ => value.toString
              }).getBytes(StandardCharsets.US_ASCII)
              // Remove any trailing zeros
              var valueLength = strBytes.length
              while (valueLength > 0 && strBytes(valueLength - 1) == '0')
                valueLength -= 1
              // If the last character is a decimal point, remove it as well
              if (valueLength > 0 && strBytes(valueLength - 1) == '.')
                valueLength -= 1
              val diff = header.fieldDescriptors(iField).fieldLength - valueLength
              // Prepend spaces
              for (_ <- 0 until diff)
                bout.put(' '.toByte)
              bout.put(strBytes, 0, valueLength)
            case DBFHelper.TypeBoolean =>
              bout.put((if (value.asInstanceOf[Boolean]) 'T' else 'F').toByte)
            case DBFHelper.TypeDate =>
              val c = java.util.Calendar.getInstance(DBFHelper.UTC)
              c.setTime(value.asInstanceOf[java.sql.Date])
              // String representation in the format YYYYMMDD
              val strValue = "%04d%02d%02d".format(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH))
              assert(strValue.length == 8, s"String representation '${strValue}' does not have a length of eight")
              bout.put(strValue.getBytes)
            case DBFHelper.TypeDatetime =>
              // two (32-bit) longs, first for date, second for time.
              // The date is the number of days since  01/01/4713 BC.
              // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
              val c = java.util.Calendar.getInstance(DBFHelper.UTC)
              c.setTime(value.asInstanceOf[java.sql.Timestamp])

              val datepart = ((c.getTimeInMillis - DBFHelper.DBFEpoch.getTimeInMillis) / DBFHelper.MillisInOneDay).toInt
              val timepart = c.get(Calendar.MILLISECOND)
              assert(timepart == c.get(Calendar.HOUR_OF_DAY) * 3600000 + c.get(Calendar.MINUTE) * 60000 + c.get(Calendar.SECOND) * 1000)
              bout.putInt(datepart)
              bout.putInt(timepart)
          }
        }
        assert(bout.position() - startFieldPos == fieldLengths(iField),
            s"Wrote ${bout.position() - startFieldPos} bytes instead of ${fieldLengths(iField)} for field #$iField with string value '$value'")
      }
      assert(bout.position() == header.recordSize, s"Wrote ${bout.position()} bytes instead of ${header.recordSize} bytes for record #$iRecord")
      dout.write(bout.array(), 0, header.recordSize)
    }
    tempIn.close()
    dout.write(DBFHelper.EOFMarker)
    dout.close()
    tempFile.delete()
  }
}
