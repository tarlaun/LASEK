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
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.io.{DataInput, DataInputStream, DataOutput, InputStream}
import java.util.{Calendar, GregorianCalendar, SimpleTimeZone}

/**
 * A helper class for reading DBF files.
 */
object DBFHelper {
  val TypeString = 'C'
  val TypeBlockNumber = 'B'
  val TypeNumeric = 'N'
  val TypeFloat = 'F'
  val TypeDouble = '0'
  val TypeBoolean = 'L'
  val TypeDate = 'D'
  val TypeDatetime = '@'
  /** Number of milliseconds in one day */
  val MillisInOneDay: Long = 1000L * 60 * 60 * 24
  /** UTC time zone */
  val UTC = new SimpleTimeZone(0, "UTC")
  /** The reference date for DBF dates */
  val DBFEpoch: GregorianCalendar = {
    val epoch = new GregorianCalendar(UTC)
    epoch.clear()
    epoch.set(-4713, 0, 1)
    epoch
  }
  /** Marks the end of the header after field descriptors */
  val EndOfHeader: Byte = 0x0D

  /** Marks the end of the file after all records */
  val EOFMarker: Byte = 0x1A
  /** Marks the beginning of a valid record */
  val ValidRecordMarker: Byte = ' '
  /** Marks the beginning of a deleted record */
  val DeletedRecordMarker: Byte = '*'


  /**
   * Infer the schema of a DBF file from the given input stream pointed at the beginning of the file
   * @param in input stream to the DBF file pointed at its beginning
   * @return the schema of the DBF file
   */
  def inferSchema(in: InputStream): Seq[StructField] = {
    val din: DataInput = in match {
      case input: DataInput => input
      case _ => new DataInputStream(in)
    }
    val version: Byte = din.readByte()
    // Skip the rest of the header until field descriptor
    val skipSize = if ((version & 0x7) == 4) 36 + 31 else 31
    din.skipBytes(skipSize)
    // Now read the field descriptors
    var fields = Seq[StructField]()
    var terminator = din.readByte()
    while (terminator != EndOfHeader) {
      // Read next field descriptor. The terminator should be the first character of the name
      var nameBytes: Array[Byte] = null
      var fieldType: Byte = 0
      var fieldLength: Int = 0
      var decimalCount: Int = 0
      if ((version & 0x7) == 4) {
        // dBase version 7
        nameBytes = new Array[Byte](32)
        nameBytes(0) = terminator
        din.readFully(nameBytes, 1, nameBytes.length - 1)
        fieldType = din.readByte()
        fieldLength = din.readUnsignedByte()
        decimalCount = din.readUnsignedByte()
        // Ignore the rest of the header
        din.skipBytes(13)
      } else {
        // dBase older than version 7
        nameBytes = new Array[Byte](11)
        nameBytes(0) = terminator
        din.readFully(nameBytes, 1, nameBytes.length - 1)
        fieldType = din.readByte()
        din.skipBytes(4)
        fieldLength = din.readUnsignedByte()
        decimalCount = din.readUnsignedByte()
        din.skipBytes(14)
      }
      val nameLength = nameBytes.indexOf(0)
      val name = new String(nameBytes, 0, if (nameLength == -1) nameBytes.length else nameLength)
      val fieldTypeSql: DataType = fieldType match {
        case TypeString | TypeBlockNumber => StringType
        case TypeNumeric | TypeFloat if decimalCount > 0 => DoubleType
        case TypeNumeric | TypeFloat if decimalCount == 0 && fieldLength <= 9 => IntegerType
        case TypeNumeric | TypeFloat if decimalCount == 0 && fieldLength > 9 => LongType
        case TypeDouble => DoubleType
        case TypeDate => DateType
        case TypeDatetime => TimestampType
        case TypeBoolean => BooleanType
      }
      fields = fields :+ StructField(name, fieldTypeSql)
      terminator = din.readByte()
    }
    fields
  }

  /**
   * Reads the header from the given file
   * @param in an input stream initialized at the beginning of the file
   * @return the header of the DBF file
   */
  def readHeader(in: DataInputStream): DBFHeader = {
    val version: Byte = in.readByte()
    val dateLastUpdatedYY = in.readUnsignedByte()
    val dateLastUpdatedMM = in.readUnsignedByte()
    val dateLastUpdatedDD = in.readUnsignedByte()
    val numRecords = IOUtil.readIntLittleEndian(in)
    val headerSize = IOUtil.readShortLittleEndian(in)
    val recordSize = IOUtil.readShortLittleEndian(in)
    in.skipBytes(2)
    val incomplete = in.readByte()
    val encryption = in.readByte()
    in.skipBytes(12)
    val mdxFlag: Byte = in.readByte()
    val driverID = in.readByte()
    in.skipBytes(2)
    var driverName: String = null
    if ((version & 0x7) == 4) {
      // dBase version 7
      val driverNameBytes = new Array[Byte](32)
      in.readFully(driverNameBytes)
      val nullTerminator = driverNameBytes.indexOf(0)
      driverName = if (nullTerminator == -1) new String(driverNameBytes)
        else new String(driverNameBytes, 0, nullTerminator)
      in.skipBytes(4) // Reserved
    }
    // Next, read field descriptor
    var fieldDescriptors = new Array[DBFFieldDescriptor](0)
    var terminator: Byte = in.readByte()
    while (terminator != EndOfHeader) {
      // dBase version 7
      // Read next field descriptor. The terminator should be the first character of the name
      var nameBytes: Array[Byte] = null
      var fieldType: Byte = 0
      var fieldLength: Short = 0
      var decimalCount: Byte = 0
      var mdxFlag: Byte = 0
      var nextAutoIncrementValue: Int = 0
      if ((version & 0x7) == 4) {
        // dBase version 7
        nameBytes = new Array[Byte](32)
        nameBytes(0) = terminator
        in.readFully(nameBytes, 1, nameBytes.length - 1)
        fieldType = in.readByte()
        fieldLength = in.readUnsignedByte().toShort
        decimalCount = in.readUnsignedByte().toByte
        in.skipBytes(2) // Reserved
        mdxFlag = in.readByte()
        in.skipBytes(2) // Reserved
        nextAutoIncrementValue = IOUtil.readIntLittleEndian(in)
        in.skipBytes(4)
      } else {
        // dBase older than version 7
        nameBytes = new Array[Byte](11)
        nameBytes(0) = terminator
        in.readFully(nameBytes, 1, nameBytes.length - 1)
        fieldType = in.readByte()
        in.skipBytes(4) // Field displacement
        fieldLength = in.readUnsignedByte().toShort
        decimalCount = in.readUnsignedByte().toByte
        in.skipBytes(13)
        mdxFlag = in.readByte()
      }
      val nameLength = nameBytes.indexOf(0)
      val name = new String(nameBytes, 0, if (nameLength == -1) nameBytes.length else nameLength)
      val fieldDescriptor = DBFFieldDescriptor(name, fieldType, fieldLength, decimalCount,
        mdxFlag, nextAutoIncrementValue)
      fieldDescriptors = fieldDescriptors :+ fieldDescriptor
      terminator = in.readByte()
    }
    assert(fieldDescriptors.map(_.fieldLength).sum + 1 == recordSize,
      s"Incompatible record size. In header = $recordSize in fields ${fieldDescriptors.map(_.fieldLength).sum + 1}")
    val cal = new GregorianCalendar(UTC)
    cal.set(Calendar.YEAR, 1900 + dateLastUpdatedYY)
    cal.set(Calendar.MONTH, dateLastUpdatedMM - 1)
    cal.set(Calendar.DAY_OF_MONTH, dateLastUpdatedDD)
    DBFHeader(version, cal.getTime, numRecords, headerSize, recordSize, incomplete,
      encryption, mdxFlag, driverID, driverName, fieldDescriptors, Array[DBFFieldProperties]())
  }

  def writeHeader(out: DataOutput, header: DBFHeader): Unit = {
    out.writeByte(header.version)
    val cal = new GregorianCalendar()
    cal.clear()
    cal.setTime(header.dateLastUpdated)
    out.writeByte(cal.get(Calendar.YEAR) - 1900)
    out.writeByte(cal.get(Calendar.MONTH) + 1)
    out.writeByte(cal.get(Calendar.DAY_OF_MONTH))
    IOUtil.writeIntLittleEndian(out, header.numRecords)
    IOUtil.writeShortLittleEndian(out, header.headerSize.toShort)
    IOUtil.writeShortLittleEndian(out, header.recordSize.toShort)
    // Skip 16 bytes
    out.writeLong(0)
    out.writeLong(0)
    out.writeByte(0) // No special flags
    out.writeByte(0) // Code page mark
    out.writeShort(0) // Reserved. Filled with zeros
    // Write field descriptors
    var fieldDisplacement: Int = 0
    for (descriptor <- header.fieldDescriptors) {
      val fieldName: Array[Byte] = new Array[Byte](11)
      if (descriptor.fieldName.length < 11) {
        // Left-align the name
        System.arraycopy(descriptor.fieldName.getBytes(), 0, fieldName, 0, descriptor.fieldName.length)
      } else {
        // Truncate at 11 characters
        System.arraycopy(descriptor.fieldName.getBytes(), 0, fieldName, 0, 11)
      }
      out.write(fieldName)
      out.writeByte(descriptor.fieldType)
      out.writeInt(fieldDisplacement) // Field displacement in record
      out.writeByte(descriptor.fieldLength)
      out.writeByte(descriptor.decimalCount)
      out.writeByte(0) // Field flags
      out.writeInt(0) // Value of autoincrement Next value
      out.writeByte(0) // Value of autoincrement Step value
      out.writeLong(0) // Reserved (8-bytes)
      fieldDisplacement += descriptor.fieldLength
    }
  }
}
