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

import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * A record writer that writes features into DBF file format. It should be accompanied with a {@link ShapefileGeometryWriter}
 * to write a correct shapefile.
 */
public class DBFWriter implements AutoCloseable {

  /**Path to the desired DBF file*/
  private Path dbfPath;

  /**Configuration of the job*/
  private Configuration conf;

  /**A temporary file for writing the records until all records have been written*/
  protected File tempDbfFile;

  /**The output stream that writes to the temporary DBF file*/
  protected ObjectOutputStream tempDbfOut;

  /**File header is updated while the features are written and is flushed to disk at the very end*/
  protected DBFHeader header;

  /**
   * Initializes the record writer to write to the given DBF file.
   * @param dbfPath the path to the output file
   * @param conf the system configuration
   * @throws IOException if an error happens while initializing the output
   */
  public void initialize(Path dbfPath, Configuration conf) throws IOException {
    this.dbfPath = dbfPath;
    this.conf = conf;

    // We cannot write the final DBF file directly due to unknown header information, e.g., number of records
    // This class first writes a temporary file with the feature data and write the final file upon closure
    tempDbfFile = File.createTempFile(dbfPath.getName(), ".dbf.tmp");
    tempDbfOut = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tempDbfFile)));
    tempDbfFile.deleteOnExit();

    header = new DBFHeader();
  }

  public void write(IFeature value) throws IOException {
    header.numRecords++;
    if (header.fieldDescriptors == null) {
      // First feature, get the header from it
      header.fieldDescriptors = new FieldDescriptor[value.length() - 1];
      for (int iAttr : value.iNonGeomJ()) {
        int iField = iAttr < value.iGeom() ? iAttr : iAttr - 1;
        FieldDescriptor attr = header.fieldDescriptors[iField] = new FieldDescriptor();
        attr.fieldName = new byte[11]; // Field name with a maximum of 11 characters, initially filled with zeros
        if (value.getName(iAttr) != null) {
          // Crop the name to 11 bytes
          byte[] fullName = value.getName(iAttr).getBytes(StandardCharsets.UTF_8);
          System.arraycopy(fullName, 0, attr.fieldName, 0, Math.min(11, fullName.length));
        }
        Object fieldValue = value.get(iAttr);
        DataType type = value.getDataType(iAttr);
        if (type == DataTypes.StringType) {
          attr.fieldType = DBFConstants.TypeString;
        } else if (type == DataTypes.IntegerType) {
          attr.fieldType = DBFConstants.TypeNumeric;
        } else if (type == DataTypes.LongType) {
          attr.fieldType = DBFConstants.TypeNumeric;
        } else if (type == DataTypes.DoubleType) {
          attr.fieldType = DBFConstants.TypeFloat;
        } else if (type == DataTypes.BooleanType) {
          // Logical
          attr.fieldType = DBFConstants.TypeBoolean;
          attr.fieldLength = 1; // One byte which is initialized to 0x20 (space) otherwise, 'T' or 'F'
        } else if (type == DataTypes.TimestampType) {
          // Date time field
          attr.fieldType = DBFConstants.TypeDatetime;
          attr.fieldLength = 8; // 8 bytes - two longs, first for date, second for time.
          // The date is the number of days since  01/01/4713 BC.
          // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
        } else if (type == DataTypes.DateType) {
          // Date time field
          attr.fieldType = DBFConstants.TypeDate;
          attr.fieldLength = 8; // String representation in the format YYYYMMDD
        } else if (type instanceof MapType) {
          // DBF does not support nested types so we convert it to string
          attr.fieldType = DBFConstants.TypeString;
          attr.fieldLength = fieldValue == null ? 0 : (short) fieldValue.toString().length();
        } else {
          throw new RuntimeException("Unsupported attribute value type: " + type);
        }

      }
    }
    // Write the feature attribute values to the temporary file
    tempDbfOut.writeObject(value);
    // Update field lengths
    for (int iAttr : value.iNonGeomJ()) {
      int iField = iAttr < value.iGeom() ? iAttr : iAttr - 1;
      Object fieldValue = value.get(iAttr);
      DataType type = value.getDataType(iAttr);
      if (fieldValue != null) {
        if (type == DataTypes.StringType ||
            type == DataTypes.IntegerType ||
            type == DataTypes.LongType ||
            type instanceof MapType)
          header.fieldDescriptors[iField].fieldLength = (short) Math.max(header.fieldDescriptors[iField].fieldLength,
              String.valueOf(fieldValue).getBytes(StandardCharsets.UTF_8).length);
        else if (type == DataTypes.DoubleType)
          updateFieldLengthDouble(header.fieldDescriptors[iField], ((Number) fieldValue).doubleValue());
      }
    }
  }

  protected static void updateFieldLengthDouble(FieldDescriptor attr, double value) {
    String str = String.format("%.19f", value);
    // Remove any trailing zeros
    int lastNonZero = str.length() - 1;
    while (lastNonZero > 0 && str.charAt(lastNonZero) == '0')
      lastNonZero--;
    int decimalPoint = str.indexOf('.');
    int numFractionPoints = decimalPoint == -1? 0 : (lastNonZero - decimalPoint);
    // Number of decimal places
    attr.decimalCount = (byte) Math.max(attr.decimalCount, numFractionPoints);
    attr.fieldLength = (short) Math.max(attr.fieldLength, lastNonZero + 1 + (value < 0? 1 : 0));
  }

  @Override
  public void close() throws IOException {
    tempDbfOut.close();
    try {
      LocalDateTime now = LocalDateTime.now();
      header.version = 3;
      header.dateLastUpdatedYY = (short) (now.getYear() - 1900);
      header.dateLastUpdatedMM = (short) now.getMonthValue();
      header.dateLastUpdatedDD = (short) now.getDayOfMonth();

      // Calculate header size and record size
      header.headerSize = 32 /*Main header*/ +
          32 * header.fieldDescriptors.length /*field descriptors*/ +
          1 /*Header record terminator*/;
      header.recordSize = 1; // Terminator
      for (FieldDescriptor field : header.fieldDescriptors)
        header.recordSize += field.fieldLength;

      FileSystem fileSystem = dbfPath.getFileSystem(conf);
      FSDataOutputStream dbfOut = fileSystem.create(dbfPath);

      // Write header
      writeHeader(dbfOut, header);
      // Write records
      // Create a new feature to make sure the geometry in it is not reused outside this class.
      // For efficiency, we compute the format string for fixed-point real numbers once and reuse them
      String[] formatString = new String[header.fieldDescriptors.length];
      for (int i = 0; i < formatString.length; i++) {
        if (header.fieldDescriptors[i].fieldType == DBFConstants.TypeNumeric ||
            header.fieldDescriptors[i].fieldType == DBFConstants.TypeFloat)
          formatString[i] = String.format("%%.%df", header.fieldDescriptors[i].decimalCount);
      }
      try (ObjectInputStream tempDbfIn = new ObjectInputStream(new BufferedInputStream(new FileInputStream(tempDbfFile)))) {
        ByteBuffer tempRecordBuffer = ByteBuffer.allocate(header.recordSize);
        tempRecordBuffer.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < header.numRecords; i++) {
          IFeature f = (IFeature) tempDbfIn.readObject();
          ((java.nio.Buffer)tempRecordBuffer).position(0);
          writeRecord(tempRecordBuffer, formatString, f);
          dbfOut.write(tempRecordBuffer.array(), 0, header.recordSize);
          assert ((java.nio.Buffer)tempRecordBuffer).remaining() == 0: "Record did not follow the record size";
        }
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException("Error reading features back", e);
      }
      // Write record terminator
      dbfOut.write(DBFReader.EOFMarker);
      dbfOut.close();
    } finally {
      // Delete the temp DBF file
      tempDbfFile.delete();
    }
  }

  protected void writeHeader(DataOutputStream out, DBFHeader header) throws IOException {
    out.write(header.version);
    out.write(header.dateLastUpdatedYY);
    out.write(header.dateLastUpdatedMM);
    out.write(header.dateLastUpdatedDD);
    IOUtil.writeIntLittleEndian(out, header.numRecords);
    IOUtil.writeShortLittleEndian(out, (short) header.headerSize);
    IOUtil.writeShortLittleEndian(out, (short) header.recordSize);
    // Skip 16 bytes
    out.writeLong(0);
    out.writeLong(0);
    out.write(0); // No special flags
    out.write(0); // Code page mark
    out.writeShort(0); // Reserved. Filled with zeros
    // Write field descriptors
    int fieldDisplacement = 0;
    for (FieldDescriptor descriptor : header.fieldDescriptors) {
      assert descriptor.fieldName.length == 11;
      out.write(descriptor.fieldName);
      out.write(descriptor.fieldType);
      out.writeInt(fieldDisplacement); // Field displacement in record
      out.write(descriptor.fieldLength);
      out.write(descriptor.decimalCount);
      out.write(0); // Field flags
      out.writeInt(0); // Value of autoincrement Next value
      out.write(0); // Value of autoincrement Step value
      out.writeLong(0); // Reserved (8-bytes)
      fieldDisplacement += descriptor.fieldLength;
    }
    out.write(0x0D); // Header record terminator
  }

  /**
   * Fills in the given ByteBuffer with the record data. The ByteBuffer should have a capacity equals to record
   * size and should be reset to position zero.
   * The ByteBuffer should also be set to little endian.
   * @param out the ByteBuffer to write the record to.
   * @param formatStrings the format strings for numeric fields
   * @param feature the feature to write to the output
   */
  protected void writeRecord(ByteBuffer out, String[] formatStrings, IFeature feature) {
    assert out.position() == 0 : "Buffer should be reset to one";
    assert out.order() == ByteOrder.LITTLE_ENDIAN : "Buffer should be set to little endian";
    out.put(DBFReader.ValidRecordMarker);
    for (int iAttr : feature.iNonGeomJ()) {
      int start = out.position();
      int iField = iAttr < feature.iGeom() ? iAttr : iAttr - 1;
      Object value = feature.get(iAttr);
      if (value == null) {
        // Write a default value that will be parsed back as null
        DataType type = feature.getDataType(iAttr);
        if (type == DataTypes.StringType) {
          for (int i = 0; i < header.fieldDescriptors[iField].fieldLength; i++)
            out.put((byte) 0);
        } else if (type == DataTypes.IntegerType || type == DataTypes.LongType || type == DataTypes.DoubleType) {
          for (int i = 0; i < header.fieldDescriptors[iField].fieldLength; i++)
            out.put((byte) 0x20); // White space
        } else if (type == DataTypes.BooleanType) {
          out.put((byte) 0x20); // White space
        } else if (type == DataTypes.TimestampType) {
          out.putLong(0);
        } else {
          throw new RuntimeException("Unsupported type " + type + " for null values");
        }
      } else {
        byte[] valBytes;
        int diff;
        DataType type = feature.getDataType(iAttr);
        if (type == DataTypes.StringType) {
          valBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
          out.put(valBytes);
          diff = header.fieldDescriptors[iField].fieldLength - valBytes.length;
          assert diff >= 0 : "Value longer than expected";
          // Append spaces
          while (diff-- > 0)
            out.put((byte) ' ');
        } else if (type == DataTypes.IntegerType || type == DataTypes.LongType) {
          valBytes = String.valueOf(value).getBytes(StandardCharsets.US_ASCII);
          diff = header.fieldDescriptors[iField].fieldLength - valBytes.length;
          assert diff >= 0 : "Value longer than expected";
          // Prepend spaces
          while (diff-- > 0)
            out.put((byte) ' ');
          out.put(valBytes);
        } else if (type == DataTypes.DoubleType) {
          valBytes = String.format(formatStrings[iField], ((Number)value).doubleValue())
              .getBytes(StandardCharsets.US_ASCII);
          // Remove any trailing zeros
          int valueLength = valBytes.length;
          while (valueLength > 0 && valBytes[valueLength - 1] == '0')
            valueLength--;
          // If the last character is a decimal point, remove it as well
          if (valueLength > 0 && valBytes[valueLength - 1] == '.')
            valueLength--;
          diff = header.fieldDescriptors[iField].fieldLength - valueLength;
          // Prepend spaces
          while (diff-- > 0)
            out.put((byte) ' ');
          out.put(valBytes, 0, valueLength);
        } else if (type == DataTypes.BooleanType) {
          out.put((byte)((Boolean) value ? 'T' : 'F'));
        } else if (type == DataTypes.DateType) {
          java.util.Calendar c = java.util.Calendar.getInstance(DBFConstants.UTC);
          c.setTime((java.sql.Date) value);
          // String representation in the format YYYYMMDD
          String strValue = String.format("%04d%02d%02d",
              c.get(Calendar.YEAR), c.get(Calendar.MONTH)+1, c.get(Calendar.DAY_OF_MONTH));
          assert strValue.length() == 8 :
              String.format("String representation '%s' does not have a length of eight", strValue);
          out.put(strValue.getBytes());
        } else if (type == DataTypes.TimestampType) {
          // two (32-bit) longs, first for date, second for time.
          // The date is the number of days since  01/01/4713 BC.
          // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
          java.util.Calendar c = java.util.Calendar.getInstance(DBFConstants.UTC);
          c.setTime((java.sql.Timestamp) value);

          int datepart = (int) ((c.getTimeInMillis() - DBFConstants.DBFEpoch.getTimeInMillis()) / DBFConstants.MillisInOneDay);
          int timepart = c.get(Calendar.MILLISECOND);
          assert timepart == c.get(Calendar.HOUR_OF_DAY) * 3600000 +
              c.get(Calendar.MINUTE) * 60000 +
              c.get(Calendar.SECOND) * 1000;
          out.putInt(datepart);
          out.putInt(timepart);
        } else if (type instanceof MapType) {
          // DBF format does not support nested map data type. Convert to string
          valBytes = (value.toString()).getBytes(StandardCharsets.UTF_8);
          out.put(valBytes);
          diff = header.fieldDescriptors[iField].fieldLength - valBytes.length;
          assert diff >= 0 : "Value longer than expected";
          // Append spaces
          while (diff-- > 0)
            out.put((byte) ' ');
        } else {
          throw new RuntimeException("Unsupported type " + type + " for null values");
        }

      }
      int end = out.position();
      assert end - start == header.fieldDescriptors[iField].fieldLength :
          String.format("Error writing field #%d with string value '%s'", iField, value);
    }
  }
}
