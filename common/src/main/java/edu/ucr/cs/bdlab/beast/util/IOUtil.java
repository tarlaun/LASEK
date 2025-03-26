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
package edu.ucr.cs.bdlab.beast.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class IOUtil {

  /**
   * Read a 32-bit written in little endian
   * @param in the DataInput to read from
   * @return the integer value extracted from the DataInput
   * @throws IOException thrown by the underlying DataInput
   */
  public static int readIntLittleEndian(DataInput in) throws IOException {
    int ch1 = in.readUnsignedByte();
    int ch2 = in.readUnsignedByte();
    int ch3 = in.readUnsignedByte();
    int ch4 = in.readUnsignedByte();
    return (ch4 << 24) | (ch3 << 16) | (ch2 << 8) | (ch1 << 0);
  }

  public static int readIntLittleEndian(byte[] bytes, int offset) {
    int ch1 = bytes[offset + 0] & 0xFF;
    int ch2 = bytes[offset + 1] & 0xFF;
    int ch3 = bytes[offset + 2] & 0xFF;
    int ch4 = bytes[offset + 3] & 0xFF;
    return (ch4 << 24) | (ch3 << 16) | (ch2 << 8) | (ch1 << 0);
  }

  /**
   * Write a 32-bit integer in little endian
   * @param out the DataOutput to write to
   * @param v the value to write
   * @throws IOException thrown by the passed DataOutput
   */
  public static void writeIntLittleEndian(DataOutput out, int v) throws IOException {
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 24) & 0xFF);
  }

  /**
   * Write a 16-bit short integer in little endian
   * @param out the DataOutput to write to
   * @param v the value to write
   * @throws IOException thrown by the underlying DataOutput
   */
  public static void writeShortLittleEndian(DataOutput out, short v) throws IOException {
    out.write((v >>>  0) & 0xFF);
    out.write((v >>>  8) & 0xFF);
  }

  /**
   * Read a 64-bit long written in little endian
   * @param in the DataInput to read from
   * @return the extracted value
   * @throws IOException thrown by the underling DataInput
   */
  public static long readLongLittleEndian(DataInput in) throws IOException {
    long ch1 = in.readUnsignedByte();
    long ch2 = in.readUnsignedByte();
    long ch3 = in.readUnsignedByte();
    long ch4 = in.readUnsignedByte();
    long ch5 = in.readUnsignedByte();
    long ch6 = in.readUnsignedByte();
    long ch7 = in.readUnsignedByte();
    long ch8 = in.readUnsignedByte();
    return (ch8 << 56) | (ch7 << 48) | (ch6 << 40) | (ch5 << 32) |
        (ch4 << 24) | (ch3 << 16) | (ch2 << 8) | (ch1 << 0);
  }

  /**
   * Read a 64-bit long written in Big endian
   * @param in the DataInput to read from
   * @return the extracted long value
   * @throws IOException if throws by the underlying DataInput
   */
  public static long readLongBigEndian(DataInput in) throws IOException {
    long ch1 = in.readUnsignedByte();
    long ch2 = in.readUnsignedByte();
    long ch3 = in.readUnsignedByte();
    long ch4 = in.readUnsignedByte();
    long ch5 = in.readUnsignedByte();
    long ch6 = in.readUnsignedByte();
    long ch7 = in.readUnsignedByte();
    long ch8 = in.readUnsignedByte();
    return (ch1 << 56) | (ch2 << 48) | (ch3 << 40) | (ch4 << 32) |
        (ch5 << 24) | (ch6 << 16) | (ch7 << 8) | (ch8 << 0);
  }

  /**
   * Write a 64-bit long in little endian
   * @param out the data output to write the long value to
   * @param v the long value to write
   * @throws IOException if thrown by the underlying DataOutput
   */
  public static void writeLongLittleEndian(DataOutput out, long v) throws IOException {
    out.write((int) ((v >>>  0) & 0xFF));
    out.write((int) ((v >>>  8) & 0xFF));
    out.write((int) ((v >>> 16) & 0xFF));
    out.write((int) ((v >>> 24) & 0xFF));
    out.write((int) ((v >>> 32) & 0xFF));
    out.write((int) ((v >>> 40) & 0xFF));
    out.write((int) ((v >>> 48) & 0xFF));
    out.write((int) ((v >>> 56) & 0xFF));
  }

  public static void writeDoubleLittleEndian(DataOutput out, double d) throws IOException {
    writeLongLittleEndian(out, Double.doubleToLongBits(d));
  }

  public static double readDoubleLittleEndian(DataInput in) throws IOException {
    return Double.longBitsToDouble(readLongLittleEndian(in));
  }

  public static double readDoubleLittleEndian(byte[] values, int startOffset) {
    long ch1 = values[startOffset + 0] & 0xff;
    long ch2 = values[startOffset + 1] & 0xff;
    long ch3 = values[startOffset + 2] & 0xff;
    long ch4 = values[startOffset + 3] & 0xff;
    long ch5 = values[startOffset + 4] & 0xff;
    long ch6 = values[startOffset + 5] & 0xff;
    long ch7 = values[startOffset + 6] & 0xff;
    long ch8 = values[startOffset + 7] & 0xff;
    long longValue = (ch8 << 56) | (ch7 << 48) | (ch6 << 40) | (ch5 << 32) |
        (ch4 << 24) | (ch3 << 16) | (ch2 << 8) | (ch1 << 0);
    return Double.longBitsToDouble(longValue);
  }

  /**
   * Returns the extension of the given file including the dot.
   * @param filename the name of the file to get its extension
   * @return the extension of the file including the dot or null of the file name does not have a dot.
   */
  public static String getExtension(String filename) {
    int i = filename.lastIndexOf('.');
    return i == -1 ? null : filename.substring(i);
  }

  public static short readShortLittleEndian(InputStream in) throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    return (short) ((ch2 << 8) | (ch1 << 0));
  }

  public static short readShortBigEndian(InputStream in) throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    return (short) ((ch1 << 8) | (ch2 << 0));
  }

  /**
   * Retrieves the floating point value starting at the given offset in big-endian format.
   * @param data the byte array to retrieve the floating point value for
   * @param offset the offset of the first byte in the floating-point value
   * @return the floating-point value
   */
  public static float getFloatBigEndian(byte[] data, int offset) {
    int intBits = 0;
    intBits |= (data[offset++] & 0xff) << 24;
    intBits |= (data[offset++] & 0xff) << 16;
    intBits |= (data[offset++] & 0xff) << 8;
    intBits |= data[offset] & 0xff;
    return Float.intBitsToFloat(intBits);
  }

  /**
   * Retrieves the floating point value starting at the given offset in little-endian format.
   * @param data the byte array to retrieve the floating point value for
   * @param offset the offset of the first byte in the floating-point value
   * @return the floating-point value
   */
  public static float getFloatLittleEndian(byte[] data, int offset) {
    int intBits = 0;
    intBits |= (data[offset++] & 0xff);
    intBits |= (data[offset++] & 0xff) << 8;
    intBits |= (data[offset++] & 0xff) << 16;
    intBits |= (data[offset] & 0xff) << 24;
    return Float.intBitsToFloat(intBits);
  }

  /**
   * Sets the given floating-point value in the give byte array at the specified offset in big-endian format.
   * @param data the byte array to write the floating point value to
   * @param offset the offset to write the floating point value to
   * @param value the value to write
   */
  public static void setFloatBigEndian(byte[] data, int offset, float value) {
    int intBits = Float.floatToIntBits(value);
    data[offset++] = (byte) ((intBits >>> 24) & 0xff);
    data[offset++] = (byte) ((intBits >>> 16) & 0xff);
    data[offset++] = (byte) ((intBits >>> 8) & 0xff);
    data[offset] = (byte) ((intBits) & 0xff);
  }

  /**
   * Sets the given floating-point value in the give byte array at the specified offset in little-endian format.
   * @param data the byte array to write the floating point value to
   * @param offset the offset to write the floating point value to
   * @param value the value to write
   */
  public static void setFloatLittleEndian(byte[] data, int offset, float value) {
    int intBits = Float.floatToIntBits(value);
    data[offset++] = (byte) ((intBits) & 0xff);
    data[offset++] = (byte) ((intBits >>> 8) & 0xff);
    data[offset++] = (byte) ((intBits >>> 16) & 0xff);
    data[offset] = (byte) ((intBits >>> 24) & 0xff);
  }

  /**
   * Copies the contents of a source path to a target path using the Hadoop FileSystem API.
   * This method supports both files and directories. In the case of a directory,
   * it performs a deep copy, including all subdirectories and files.
   *
   * @param sourceFS The file system of the source path.
   * @param sourcePath The source path in the source file system.
   * @param targetFS The file system of the target path.
   * @param targetPath The target path in the target file system.
   * @throws IOException If an I/O error occurs during copying.
   */
  public static void deepCopy(FileSystem sourceFS, Path sourcePath, FileSystem targetFS, Path targetPath) throws IOException {
    // Check if the source is a directory
    if (sourceFS.getFileStatus(sourcePath).isDirectory()) {
      // Create the target directory if it doesn't exist
      targetFS.mkdirs(targetPath);

      // List all entries in the source directory
      FileStatus[] fileStatuses = sourceFS.listStatus(sourcePath);

      for (FileStatus fileStatus : fileStatuses) {
        // Construct the source and target paths for each entry
        Path currentSourcePath = fileStatus.getPath();
        Path currentTargetPath = new Path(targetPath, currentSourcePath.getName());

        // Recursively copy each entry
        deepCopy(sourceFS, currentSourcePath, targetFS, currentTargetPath);
      }
    } else {
      // If the source is a file, copy the file contents
      if (targetFS.exists(targetPath) && targetFS.getFileStatus(targetPath).isDirectory())
        targetPath = new Path(targetPath, sourcePath.getName());
      try (InputStream in = sourceFS.open(sourcePath);
           OutputStream out = targetFS.create(targetPath, true)) {
        IOUtils.copyBytes(in, out, 1024 * 8, false);
      }
    }
  }
}
