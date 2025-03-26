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
package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.beast.util.BufferedFSDataInputStream;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.logging.Log;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads TIFF images.
 */
public class TiffReader implements ITiffReader, Closeable {

  private static final Log LOG = LogFactory.getLog(TiffReader.class);

  /**
   * 8-byte header of the TIFF file. See specs page 13.
   */
  static class Header {

    /**Byte ordering. Either LITTLE_ENDIAN or BIG_ENDIAN*/
    public short order;

    /** An arbitrary but carefully chosen number (42) that further identifies the file as a TIFF file. */
    public short signature;

    /**
     * The offset (in bytes) of the first IFD. The directory may be at any location in the
     * file after the header but must begin on a word boundary. In particular, an Image
     * File Directory may follow the image data it describes. Readers must follow the
     * pointers wherever they may lead.
     * The term byte offset is always used in this document to refer to a location with
     * respect to the beginning of the TIFF file. The first byte of the file has an offset of 0.
     */
    public int offset;

    public Header read(InputStream in) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      int headerSize = 0;
      int readCount;
      do {
        readCount = in.read(buffer.array(), headerSize, 8 - headerSize);
        if (readCount > 0)
          headerSize += readCount;
      } while (readCount > 0 && headerSize < 8);
      if (headerSize != 8)
        throw new RuntimeException("Could not read the eight-byte header of the TIFF file");
      order = buffer.getShort();
      assert order == TiffConstants.LITTLE_ENDIAN || order == TiffConstants.BIG_ENDIAN : String.format("Invalid byte order %d", order);
      buffer.order(order == TiffConstants.LITTLE_ENDIAN? ByteOrder.LITTLE_ENDIAN: ByteOrder.BIG_ENDIAN);
      signature = buffer.getShort();
      assert signature == TiffConstants.SIGNATURE : String.format("Invalid signature %d in TIFF file", signature);
      offset = buffer.getInt();
      return this;
    }
  }

  /**The header of this file*/
  protected Header header;

  /**Input stream to the TIFF file*/
  protected BufferedFSDataInputStream in;

  /**Path to the underlying GeoTIFF file if known.*/
  protected String filePath;

  /**
   * The list of all directory entries in the file. Each entry in this list contains an array of IFD entries
   * that appear on IFD. According to the TIFF specs on page 16:
   * There may be more than one IFD in a TIFF file. Each IFD defines a subfile. One
   * potential use of subfiles is to describe related images, such as the pages of a facsimile
   * transmission. A Baseline TIFF reader is not required to read any IFDs
   * beyond the first one.
   */
  protected List<AbstractIFDEntry[]> directoryEntries;

  /**A temporary buffer to read chunks from underlying file*/
  private transient ByteBuffer buffer;

  public void initialize(FSDataInputStream _in) throws IOException {
    this.initialize(new BufferedFSDataInputStream(_in, 4 * 1024 * 1024));
  }

  protected void initialize(BufferedFSDataInputStream _in) throws IOException {
    this.in = _in;
    header = new Header().read(this.in);
    // Read all IFDs and keep them in memory
    directoryEntries = new ArrayList();
    buffer = ByteBuffer.allocate(1024);
    buffer.order(header.order == TiffConstants.LITTLE_ENDIAN? ByteOrder.LITTLE_ENDIAN: ByteOrder.BIG_ENDIAN);
    int offsetIFD = header.offset;
    while (offsetIFD != 0) {
      in.seek(offsetIFD);
      short numEntries = header.order == TiffConstants.LITTLE_ENDIAN?
          IOUtil.readShortLittleEndian(in) : IOUtil.readShortBigEndian(in);
      assert numEntries > 0 : "Found an empty IFD. Each IFD must have at least one entry.";
      IFDEntry[] entries = new IFDEntry[numEntries];
      int sizeIFD = 12 * numEntries + 4;
      // Expand the buffer if necessary to hold the entire table
      buffer = expandBuffer(buffer, sizeIFD);
      ((java.nio.Buffer)buffer).position(0);
      // Read the entire IFD
      in.readFully(buffer.array(), 0, sizeIFD);
      // Set the limit to ensure that we don't mistakenly go beyond the IFD
      ((java.nio.Buffer)buffer).limit(sizeIFD);

      // Read all the entries and keep them in memory
      for (int $i = 0; $i < numEntries; $i++)
        entries[$i] = new IFDEntry().read(buffer, header.order == TiffConstants.BIG_ENDIAN);

      directoryEntries.add(entries);

      // Locate the next IFD (if any)
      offsetIFD = buffer.getInt();
      assert buffer.remaining() == 0 : String.format("The buffer still has %d remaining bytes", buffer.remaining());
    }
  }

  @Override
  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public String getFilePath() {
    return filePath;
  }

  public int getNumLayers() {
    return directoryEntries.size();
  }

  public ByteBuffer readEntry(AbstractIFDEntry entry, ByteBuffer buffer) throws IOException {
    if (entry == null) {
      if (buffer != null) {
        ((java.nio.Buffer)buffer).position(0);
        ((java.nio.Buffer)buffer).limit(0);
      }
      return buffer;
    }
    int size = entry.getCountAsInt() * TiffConstants.TypeSizes[entry.type];
    if (buffer == null || buffer.capacity() < size)
      buffer = expandBuffer(buffer, size);
    ((java.nio.Buffer)buffer).limit(size);
    ((java.nio.Buffer)buffer).position(0);
    if (size <= 4) {
      // value stored in the offset field
      int val = entry.getOffsetAsInt();
      for (int $i = 0; $i < size; $i++) {
        buffer.put((byte) (val & 0xff));
        val = val >>> 8;
      }
      buffer.order(ByteOrder.LITTLE_ENDIAN);
    } else {
      // Value stored in the input file
      in.seek(entry.getOffsetAsInt());
      in.readFully(buffer.array(), 0, size);
      buffer.order(this.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    }
    ((java.nio.Buffer)buffer).position(0);
    return buffer;
  }

  private ByteBuffer expandBuffer(ByteBuffer buffer, int length) {
    if (buffer == null || buffer.capacity() < length) {
      buffer = ByteBuffer.allocate(length);
      buffer.order(header.order == TiffConstants.LITTLE_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    }
    ((java.nio.Buffer)buffer).limit(length);
    return buffer;
  }

  public TiffRaster getLayer(int i) throws IOException {
    return new TiffRaster(this, i);
  }

  @Override
  public AbstractIFDEntry[] getDirectoryEntries(int iLayer) {
    return directoryEntries.get(iLayer);
  }

  @Override
  public void readRawData(long tileOffset, byte[] bytes) throws IOException {
    if (tileOffset >= in.getPos() && tileOffset - in.getPos() < 1024 * 1024)
      in.skip(tileOffset - in.getPos());
    else {
      in.seek(tileOffset);
      LOG.info(String.format(this+": Seeking tileOffset at %s ", tileOffset));
    }
    in.readFully(bytes);
  }

  @Override
  public boolean isLittleEndian() {
    return header.order == TiffConstants.LITTLE_ENDIAN;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

}
