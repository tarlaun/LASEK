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
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Reads a DBase file as a sequence of features.
 * This class follows the file specifications at http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
 * and http://www.dbf2002.com/dbf-file-format.html
 */
public class DBFReader implements AutoCloseable {

  /**The header of the current file*/
  protected DBFHeader header;

  /**Total size of a record in bytes*/
  protected int recordSize;

  /**An array to hold the values of all fields of one record*/
  protected byte[] recordValue;

  /**The value currently pointed by this reader*/
  protected Feature feature;

  /**The input stream to the DBF file*/
  protected DataInputStream in;

  /**This flag is raised when the EOF marker is reached*/
  protected boolean eofReached;

  /**The index of the current record one-based. Used to calculate the progress in an easy way.*/
  protected int iRecord;

  /**Cache the path to the input file to report the error*/
  protected Path dbfPath;

  /**Field names and types as appear in the header*/
  protected StructField[] fields;

  /**The schema of the created object*/
  protected StructType schema;

  public void initialize(Path dbfPath, BeastOptions conf) throws IOException {
    FileSystem fs = dbfPath.getFileSystem(conf.loadIntoHadoopConf(null));
    initialize(fs.open(dbfPath), conf);
    // Cache the file path to use in reporting errors
    this.dbfPath = dbfPath;
  }

  public void initialize(DataInputStream in, BeastOptions conf) throws IOException {
    this.dbfPath = null; // Reset file name if it is initialized from a stream
    this.in = in;
    this.header = new DBFHeader();
    readHeader(this.in, header);
    readFieldDescriptors(this.in, header);
    if ((header.version & 0x7) == 4) {
      readFieldProperties(this.in, header);
    }
    iRecord = 0;
    eofReached = false;
    this.recordValue = new byte[this.recordSize];
    // Initialize field names and types to use for creating features
    this.fields = new StructField[1 + header.fieldDescriptors.length];
    this.fields[0] = new StructField("g", GeometryHelper.GeometryType, true, Metadata.empty());
    for (int iField = 0; iField < header.fieldDescriptors.length; iField++) {
      FieldDescriptor fieldDescriptor = header.fieldDescriptors[iField];
      String fieldName = fieldDescriptor.getFieldName();
      DataType fieldType;
      switch (fieldDescriptor.fieldType) {
        case DBFConstants.TypeString:
        case DBFConstants.TypeBlockNumber:
          fieldType = DataTypes.StringType;
          break;
        case DBFConstants.TypeNumeric:
        case DBFConstants.TypeFloat:
          if (fieldDescriptor.decimalCount > 0)
            fieldType = DataTypes.DoubleType;
          else if (fieldDescriptor.fieldLength - fieldDescriptor.decimalCount <= 9)
            fieldType = DataTypes.IntegerType;
          else
            fieldType = DataTypes.LongType;
          break;
        case DBFConstants.TypeDouble:
          fieldType = DataTypes.DoubleType;
          break;
        case DBFConstants.TypeDate:
          fieldType = DataTypes.DateType;
          break;
        case DBFConstants.TypeDatetime:
          fieldType = DataTypes.TimestampType;
          break;
        case DBFConstants.TypeBoolean:
          fieldType = DataTypes.BooleanType;
          break;
        default:
          throw new RuntimeException("Unrecognized type "+fieldDescriptor.fieldType);
      }
      fields[1 + iField] = new StructField(fieldName, fieldType, true, Metadata.empty());
    }
    this.schema = new StructType(fields);
  }

  private void readFieldProperties(DataInputStream in, DBFHeader header) {
    throw new RuntimeException("Not supported yet");
  }

  /**
   * Read the first part of the header before the field descriptors
   * @param in the input stream to read from
   * @param header the header of the DBF file
   * @throws IOException if an error happens while reading the header
   */
  protected void readHeader(DataInputStream in, DBFHeader header) throws IOException {
    header.version = in.readByte();
    header.dateLastUpdatedYY = (short) in.readUnsignedByte();
    header.dateLastUpdatedMM = (short) in.readUnsignedByte();
    header.dateLastUpdatedDD = (short) in.readUnsignedByte();
    header.numRecords = IOUtil.readIntLittleEndian(in);
    header.headerSize = IOUtil.readShortLittleEndian(in);
    header.recordSize = IOUtil.readShortLittleEndian(in);
    in.skipBytes(2); // Reserved; filled with zeros.
    header.incomplete = in.readByte();
    header.encryption = in.readByte();
    in.skipBytes(12); // Reserved for multi-user processing.
    header.mdxFlag = in.readByte();
    header.driverID = in.readByte();
    in.skipBytes(2); // Reserved; filled with zeros.
    if ((header.version & 0x7) == 4) {
      // dBase version 7
      in.read(header.driverName);
      in.skipBytes(4); // Reserved
    }
  }

  protected void readFieldDescriptors(DataInputStream in, DBFHeader header) throws IOException {
    byte terminator = in.readByte();
    List<FieldDescriptor> descriptors = new ArrayList<>();
    recordSize = 0;
    while (terminator != 0x0D) {
      // The terminator is not actually a terminator; it is the first byte in the first attribute
      FieldDescriptor descriptor = new FieldDescriptor();
      if ((header.version & 0x7) == 4) {
        // dBase version 7
        descriptor.fieldName = new byte[32];
        descriptor.fieldName[0] = terminator;
        in.read(descriptor.fieldName, 1, descriptor.fieldName.length - 1);
        descriptor.fieldType = in.readByte();
        descriptor.fieldLength = (short) in.readUnsignedByte();
        descriptor.decimalCount = (byte) in.readUnsignedByte();
        in.skipBytes(2); // Reserved.
        descriptor.mdxFlag = in.readByte();
        in.skipBytes(2); // Reserved.
        descriptor.nextAutoIncrementValue = IOUtil.readIntLittleEndian(in);
        in.skipBytes(4); // Reserved.
      } else {
        // dBase older than version 7
        descriptor.fieldName = new byte[11];
        descriptor.fieldName[0] = terminator;
        in.read(descriptor.fieldName, 1, descriptor.fieldName.length - 1);
        descriptor.fieldType = in.readByte();
        in.skipBytes(4);
        descriptor.fieldLength = (short) in.readUnsignedByte();
        descriptor.decimalCount = (byte) in.readUnsignedByte();
        in.skipBytes(13);
        descriptor.mdxFlag = in.readByte();
      }

      recordSize += descriptor.fieldLength;
      descriptors.add(descriptor);
      terminator = in.readByte();
    }
    header.fieldDescriptors = descriptors.toArray(new FieldDescriptor[descriptors.size()]);
  }

  public static final byte EOFMarker = 0x1A;
  public static final byte ValidRecordMarker = ' ';
  public static final byte DeletedRecordMarker = '*';

  public boolean nextKeyValue() throws IOException {
    if (eofReached)
      return false;
    while (true) {
      byte marker = in.readByte();
      iRecord++;
      if (marker == EOFMarker) {
        eofReached = true;
        return false;
      } else if (marker == ValidRecordMarker) {
        // A valid record. Read it.
        in.readFully(this.recordValue);
        this.feature = readFeatureFields(this.recordValue, this.header);
        return true;
      } else if (marker == DeletedRecordMarker) {
        // A deleted record. Skip over it
        in.readFully(this.recordValue);
      } else {
        String filename = dbfPath == null? "DBF" : "'" + dbfPath + "'";
        throw new RuntimeException(String.format("Error parsing %s file. Invalid marker %d", filename, marker));
      }
    }
  }

  private Feature readFeatureFields(byte[] fieldValues, DBFHeader header) {
    int startOffset = 0;
    Object[] values = new Object[1 + header.fieldDescriptors.length];
    values[0] = null; // Set the geometry component to null
    for (int iField = 0; iField < header.fieldDescriptors.length; iField++) {
      FieldDescriptor fieldDescriptor = header.fieldDescriptors[iField];
      int endOffsetInclusive = Math.min(fieldValues.length - 1, startOffset + fieldDescriptor.fieldLength - 1);
      Object fieldValue;
      switch (fieldDescriptor.fieldType) {
        case DBFConstants.TypeString:
          // String
          while (endOffsetInclusive >= startOffset &&
              (fieldValues[endOffsetInclusive] == ' ' || fieldValues[endOffsetInclusive] == 0))
            endOffsetInclusive--;
          fieldValue = startOffset > endOffsetInclusive? null :
              new String(fieldValues, startOffset, endOffsetInclusive - startOffset + 1, StandardCharsets.UTF_8);
          break;
        case DBFConstants.TypeNumeric:
        case DBFConstants.TypeFloat:
          // Skip any leading blanks (or non-digits)
          int adjustedStartOffset = startOffset;
          while (adjustedStartOffset <= endOffsetInclusive &&
              !(fieldValues[adjustedStartOffset] >= '0' && fieldValues[adjustedStartOffset] <= '9') &&
              fieldValues[adjustedStartOffset] != '.' && fieldValues[adjustedStartOffset] != '-')
            adjustedStartOffset++;
          // Check if it is an empty value
          if (adjustedStartOffset > endOffsetInclusive)
            fieldValue = null;
          else {
            String strValue = new String(fieldValues, adjustedStartOffset, endOffsetInclusive - adjustedStartOffset + 1, StandardCharsets.US_ASCII);
            if (fieldDescriptor.decimalCount == 0 && fieldDescriptor.fieldLength < 10) {
              // Return the value as integer
              fieldValue = Integer.parseInt(strValue);
            } else if (fieldDescriptor.decimalCount == 0 && fieldDescriptor.fieldLength < 19) {
              // Return value as long
              fieldValue = Long.parseLong(strValue);
            } else {
//              strValue = strValue.substring(0, strValue.length() - fieldDescriptor.decimalCount) + "."
//                    + strValue.substring(strValue.length() - fieldDescriptor.decimalCount);
              fieldValue = Double.parseDouble(strValue);
            }
          }
          break;
        case DBFConstants.TypeDouble:
          // Double
          double v = IOUtil.readDoubleLittleEndian(fieldValues, startOffset);
          fieldValue = Double.isNaN(v) ? null : v;
          break;
        case DBFConstants.TypeDate:
          // Date - 8 bytes - date stored as a string in the format YYYYMMDD.
          int yyyy = (fieldValues[startOffset] - '0') * 1000 + (fieldValues[startOffset + 1] - '0') * 100 +
              (fieldValues[startOffset + 2] - '0') * 10 + (fieldValues[startOffset + 3] - '0');
          int mm = (fieldValues[startOffset + 4] - '0') * 10 + (fieldValues[startOffset + 5] - '0');
          int dd = (fieldValues[startOffset + 6] - '0') * 10 + (fieldValues[startOffset + 7] - '0');
          GregorianCalendar date = new GregorianCalendar(DBFConstants.UTC);
          date.clear();
          date.set(yyyy, mm - 1, dd);
          fieldValue = new java.sql.Date(date.getTimeInMillis());
          break;
        case DBFConstants.TypeDatetime:
          // 8 bytes - two longs, first for date, second for time.
          // The date is the number of days since  01/01/4713 BC.
          // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
          int datepart = IOUtil.readIntLittleEndian(fieldValues, startOffset);
          int timepart = IOUtil.readIntLittleEndian(fieldValues, startOffset + 4);
          if (datepart == 0 && timepart == 0)
            fieldValue = null;
          else {
            GregorianCalendar datetime = new GregorianCalendar(DBFConstants.UTC);
            datetime.clear();
            datetime.setTimeInMillis(datepart * DBFConstants.MillisInOneDay + DBFConstants.DBFEpoch.getTimeInMillis() + timepart);
            fieldValue = new java.sql.Timestamp(datetime.getTimeInMillis());
          }
          break;
        case DBFConstants.TypeBlockNumber:
          // 10 digits representing a .DBT block number. The number is stored as a string,
          // right justified and padded with blanks.
          // Skip leading spaces
          while (startOffset <= endOffsetInclusive && fieldValues[startOffset] == ' ')
            startOffset++;
          fieldValue = startOffset == endOffsetInclusive ? null :
              new String(fieldValues, startOffset, endOffsetInclusive - startOffset + 1, StandardCharsets.UTF_8);
          break;
        case DBFConstants.TypeBoolean:
          // Logical (Boolean) type
          // initialized to 0x20 (space) otherwise T or F
          switch (fieldValues[startOffset]) {
            case ' ': fieldValue = null; break;
            case 'T': fieldValue = true; break;
            case 'F': fieldValue = false; break;
            default: fieldValue = null; break;
          }
          break;
        default:
          throw new RuntimeException(String.format("Field type '%c' not supported", fieldDescriptor.fieldType));
      }
      values[iField + 1] = fieldValue;
      startOffset += fieldDescriptor.fieldLength;
    }
    return new Feature(values, schema);
  }

  public IFeature getCurrentValue() {
    return feature;
  }

  public float getProgress() {
    return (float)iRecord / header.numRecords;
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
  }
}
