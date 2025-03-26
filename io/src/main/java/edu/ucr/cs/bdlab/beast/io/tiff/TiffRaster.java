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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raster layer from the TIFF file.
 */
public class TiffRaster {

  /**The associated TIFF file*/
  protected final ITiffReader reader;

  /**
   * A predictor is a mathematical operator that is applied to the image data before an encoding scheme is applied.
   * 1 = No prediction scheme used before coding.
   * 2 = Horizontal differencing.
   */
  protected final int predictor;

  /**IDF entries associated with this layer in the TIFF file*/
  protected AbstractIFDEntry[] entries;

  /**Width of the raster. Number of pixels per row.*/
  protected int width;

  /**Height of the raster. Number of rows (scan lines).*/
  protected int height;

  /**
   * Width of each tile in pixels. If the file is stored in a strip representation, tileWidth = width
   */
  protected int tileWidth;

  /**
   * Height of each tile in pixels. If the file is stored in a strip representation, tileHeight = rowsPerStrip
   */
  protected int tileHeight;

  /**
   * Offsets of tiles if the file is stored in a tile format; or offsets of strips if strip format
   */
  protected long[] tileOffsets;

  /**
   * Sizes of tiles in number of bytes (number of compressed bytes if compressed); if the file is stored in a strip
   * representation, this will be the strip lengths.
   */
  protected int[] tileByteCounts;

  /**Number of bits for each sample. A sample is like a component in the pixel.*/
  protected int[] bitsPerSample;

  /**The sum of the entries in the field {@link #bitsPerSample}*/
  protected transient int bitsPerPixel;

  /** A reusable buffer for reading contents of IFD entries*/
  protected transient ByteBuffer buffer;

  /** The index of the tile that is currently loaded */
  protected int currentTileIndex;

  /** The tile that is currently loaded */
  protected ITiffTile currentTile;

  protected byte[] jpegTableRawData = null;

  /**
   * This field specifies how to interpret each data sample in a pixel. Possible values are:
   * 1 = unsigned integer data
   * 2 = two's complement signed integer data
   * 3 = IEEE floating point data [IEEE]
   * 4 = undefined data format
   */
  protected int[] sampleFormats;

  /**
   * How the components of each pixel are stored.
   * 1 = {@link TiffConstants#ChunkyFormat}
   * 2 = {@link TiffConstants#PlanarFormat}
   */
  protected int planarConfiguration;

  /**
   * Initializes a raster layer for the given TIFF file and the given layer number in it.
   * @param reader the TIFF reader of the file
   * @param iLayer the index of the layer to read
   * @throws IOException if an error happens while reading the file
   */
  public TiffRaster(ITiffReader reader, int iLayer) throws IOException {
    this.entries = reader.getDirectoryEntries(iLayer);
    this.reader = reader;
    this.width = getEntry(TiffConstants.TAG_IMAGE_WIDTH).getOffsetAsInt();
    this.height = getEntry(TiffConstants.TAG_IMAGE_LENGTH).getOffsetAsInt();
    // Retrieve the tiling information depending on the representation type
    AbstractIFDEntry rowsPerStrip = getEntry(TiffConstants.TAG_ROWS_PER_STRIP);
    if (rowsPerStrip != null) {
      // File stored in stripped format
      this.tileWidth = this.width;
      this.tileHeight = rowsPerStrip.getOffsetAsInt();
      // Retrieve offsets of strips
      AbstractIFDEntry ifdStripOffsets = getEntry(TiffConstants.TAG_STRIP_OFFSETS);
      this.tileOffsets = getLongValues(ifdStripOffsets);
      // Retrieve lengths of strips
      AbstractIFDEntry ifdStripLengths = getEntry(TiffConstants.TAG_STRIP_BYTE_COUNTS);
      this.tileByteCounts = getIntValues(ifdStripLengths);
    } else {
      // File stored in grid format
      this.tileWidth = getEntry(TiffConstants.TAG_TILE_WIDTH).getOffsetAsInt();
      this.tileHeight = getEntry(TiffConstants.TAG_TILE_LENGTH).getOffsetAsInt();
      // Retrieve tile offsets
      AbstractIFDEntry ifdTileOffsets = getEntry(TiffConstants.TAG_TILE_OFFSETS);
      this.tileOffsets = getLongValues(ifdTileOffsets);
      // Retrieve tile lengths (sizes in bytes)
      AbstractIFDEntry ifdTileByteCounts = getEntry(TiffConstants.TAG_TILE_BYTE_COUNTS);
      this.tileByteCounts = getIntValues(ifdTileByteCounts);
    }
    int samplesPerPixel = getEntry(TiffConstants.TAG_SAMPLES_PER_PIXEL).getOffsetAsInt();
    this.bitsPerSample = getIntValues(getEntry(TiffConstants.TAG_BITS_PER_SAMPLE));
    AbstractIFDEntry sampleFormatEntry = getEntry(TiffConstants.TAG_SAMPLE_FORMAT);
    if (sampleFormatEntry != null) {
      this.sampleFormats = getIntValues(sampleFormatEntry);
    } else {
      // Use default values
      this.sampleFormats = new int[samplesPerPixel];
      Arrays.fill(this.sampleFormats, 1);
    }
    assert this.bitsPerSample.length == this.sampleFormats.length;
    assert this.bitsPerSample.length == samplesPerPixel;
    for (int x : bitsPerSample)
      bitsPerPixel += x;
    AbstractIFDEntry pcTag = getEntry(TiffConstants.TAG_PLANAR_CONFIGURATION);
    this.planarConfiguration = pcTag != null? pcTag.getOffsetAsInt() : TiffConstants.ChunkyFormat;
    AbstractIFDEntry predictorEntry = getEntry(TiffConstants.TAG_PREDICTOR);
    this.predictor = predictorEntry == null ? 1 : predictorEntry.getOffsetAsInt();
  }

  /**
   * Width of the raster layer in pixels
   * @return the width of the raster in pixels
   */
  public int getWidth() {
    return width;
  }

  /**
   * Height of the raster layer in pixels.
   * @return the height of the raster in pixels
   */
  public int getHeight() {
    return height;
  }

  /**
   * Number of tiles along the x-axis = &lceil;image width / tile width&rceil;
   * @return the number of tiles along the x-axis
   */
  public int getNumTilesX() {
    return (width + tileWidth - 1) / tileWidth;
  }

  /**
   * Number of tiles along the y-axis = &lceil; image height / tile height &rceil;
   * @return the number of tiles along y-axis
   */
  public int getNumTilesY() {
    return (height + tileHeight - 1) / tileHeight;
  }

  /**
   * The total number of tiles in the raster. This is always equal to getNumTilesX() * getNumTilesY()
   * @return
   */
  public int getNumTiles() {
    return getNumTilesX() * getNumTilesY();
  }

  /**
   * Load tile data from disk and return it in the given tile.
   * @param tileID the ID of the tile to load
   * @return the loaded Tile
   * @throws IOException if an error happens while reading the tile
   */
  public synchronized AbstractTiffTile getTile(int tileID) throws IOException {
    int iTile = tileID % getNumTilesX();
    int jTile = tileID / getNumTilesX();
    int compressionScheme = getEntry(TiffConstants.TAG_COMPRESSION).getOffsetAsInt();
    if (!CompressedTiffTile.isCompressionSupported(compressionScheme))
      throw new RuntimeException("Unsupported compression "+compressionScheme+" in file '"+ reader.getFilePath()+"'");
    boolean all8Bits = true;
    for (int b : bitsPerSample)
      if (b != 8)
        all8Bits = false;
    byte[][] rawData;
    switch (planarConfiguration) {
      case TiffConstants.ChunkyFormat:
        rawData = new byte[1][tileByteCounts[tileID]];
        reader.readRawData(tileOffsets[tileID], rawData[0]);
        break;
      case TiffConstants.PlanarFormat:
        rawData = new byte[getNumSamples()][];
        for (int iSample = 0; iSample < rawData.length; iSample++) {
          rawData[iSample] = new byte[tileByteCounts[getNumTiles() * iSample + tileID]];
          reader.readRawData(tileOffsets[getNumTiles() * iSample + tileID], rawData[iSample]);
        }
        break;
      default:
        throw new RuntimeException("Unsupported planar configuration "+planarConfiguration);
    }

    CompressedTiffTile tile;
    if (all8Bits && planarConfiguration == TiffConstants.ChunkyFormat)
      tile = new CompressedTiffTile8Bit(rawData[0], compressionScheme, predictor, bitsPerSample, sampleFormats, bitsPerPixel,
            iTile * tileWidth, jTile * tileHeight, (iTile + 1) * tileWidth - 1, (jTile + 1) * tileHeight - 1,
            planarConfiguration, reader.isLittleEndian());
    else
      tile = new CompressedTiffTile(rawData, compressionScheme, predictor, bitsPerSample, sampleFormats, bitsPerPixel,
        iTile * tileWidth, jTile * tileHeight, (iTile + 1) * tileWidth - 1, (jTile + 1) * tileHeight - 1,
        planarConfiguration, reader.isLittleEndian());
    if (compressionScheme == TiffConstants.COMPRESSION_JPEG2) {
      // Set JPEG decompression table
      tile.setJpegTable(getOrReadJpegTable());
    }
    return tile;
  }

  protected byte[] getOrReadJpegTable() throws IOException {
    if (jpegTableRawData == null) {
      AbstractIFDEntry jpegTableEntry = getEntry(TiffConstants.TAG_JPEG_TABLES);
      if (jpegTableEntry != null) {
        byte[] data = new byte[jpegTableEntry.getLength()];
        reader.readEntry(jpegTableEntry, ByteBuffer.wrap(data));
        jpegTableRawData = data;
      }
    }
    return jpegTableRawData;
  }

  /**
   * Returns the value of the pixel at column i and row j as an integer. If it has more than one component,
   * they are concatenated together into one integer. If the components cannot fit into one long integer
   * (i.e., more than 64 bits), an exception is thrown. The components are ordered so that the first component is stored
   * in the highest order part of the returned value.
   * @param iPixel column-coordinate of the pixel
   * @param jPixel row-coordinate of the pixel
   * @return the pixel value as long
   * @throws IOException if an error happens while reading the file
   */
  public long getPixel(int iPixel, int jPixel) throws IOException {
    if (bitsPerPixel > 64)
      throw new RuntimeException(String.format("The pixel value is too big to fit in a 64-bit integer (%d > 64)", bitsPerPixel));
    loadTileAtPixel(iPixel, jPixel);
    return currentTile.getPixel(iPixel, jPixel);
  }

  /**
   * Loads the data of the tile that contains the given pixel
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @throws IOException if an error happens while reading the file
   */
  public void loadTileAtPixel(int iPixel, int jPixel) throws IOException {
    int iTile = iPixel / tileWidth;
    int jTile = jPixel / tileHeight;
    int requiredTileIndex = jTile * getNumTilesX() + iTile;
    if (currentTile == null || requiredTileIndex != currentTileIndex)
      readTileData(requiredTileIndex);
  }

  public int getTileIDAtPixel(int i, int j) {
    int iTile = i / tileWidth;
    int jTile = j / tileHeight;
    return jTile * getNumTilesX() + iTile;
  }

  /**
   * Returns the value of the given pixel and band as an integer
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample which band (or sample) to read for the given pixel
   * @return the sample value as integer
   * @throws IOException if an error happens while reading the file
   */
  public int getSampleValueAsInt(int iPixel, int jPixel, int iSample) throws IOException {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case TiffConstants.SAMPLE_IEEE_FLOAT:
        return Math.round(Float.intBitsToFloat((int) val));
      case TiffConstants.SAMPLE_SIGNED_INT:
        //diff return value
        return (short) val;
      case TiffConstants.SAMPLE_UNSIGNED_INT:
      case TiffConstants.SAMPLE_UNDEFINED:
      default:
        return (int) val;
    }
  }

  /**
   * Returns the value of the given pixel and band as a single-precision floating point value
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample as float
   * @throws IOException if an error happens while reading the file
   */
  protected float getSampleValueAsFloat(int iPixel, int jPixel, int iSample) throws IOException {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case TiffConstants.SAMPLE_IEEE_FLOAT:
        return Float.intBitsToFloat((int) val);
      case TiffConstants.SAMPLE_SIGNED_INT:
        return (short)val;
      case TiffConstants.SAMPLE_UNSIGNED_INT:
      case TiffConstants.SAMPLE_UNDEFINED:
      default:
        return (float) val;
    }
  }

  /**
   * Returns the value of the given pixel and band as a double-precision floating point value
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample as float
   * @throws IOException if an error happens while reading the file
   */
  protected double getSampleValueAsDouble(int iPixel, int jPixel, int iSample) throws IOException {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case TiffConstants.SAMPLE_IEEE_FLOAT:
        return bitsPerSample[iSample] == 32 ? Float.intBitsToFloat((int) val) : Double.longBitsToDouble(val);
      case TiffConstants.SAMPLE_SIGNED_INT:
        return (short)val;
      case TiffConstants.SAMPLE_UNSIGNED_INT:
      case TiffConstants.SAMPLE_UNDEFINED:
      default:
        return (double) val;
    }
  }

  /**
   * Get the sample value bits as an integer without interpreting the sample format.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample at the given pixel
   * @throws IOException if an error happens while reading the file
   */
  public long getRawSampleValue(int iPixel, int jPixel, int iSample) throws IOException {
    loadTileAtPixel(iPixel, jPixel);
    return currentTile.getRawSampleValue(iPixel, jPixel, iSample);
  }

  /**
   * Reverse a value (Little Endian &lt; = &gt; Big Endian)
   * @param value the value to reverse
   * @param numBits the number of bits of the given value. Has to be a multiple of 8
   * @return the reversed value.
   */
  protected static long reverseValue(long value, int numBits) {
    if (numBits == 64) {
      value = Long.reverseBytes(value);
    } else if (numBits == 48) {
      long valReversed = value >>> 40;
      valReversed |= (value >>> 24) & 0xff00;
      valReversed |= (value >>> 8) & 0xff0000;
      valReversed |= (value << 8) & 0xff000000L;
      valReversed |= (value << 24) & 0xff00000000L;
      valReversed |= (value << 40) & 0xff0000000000L;
      value = valReversed;
    } else if (numBits == 32) {
      value = Integer.reverseBytes((int) value);
    } else if (numBits == 16) {
      value = Short.reverseBytes((short) value);
    } else if (numBits == 8) {
      // Do nothing
    } else {
      throw new RuntimeException("Unsupported number of bits "+numBits);
    }
    return value;
  }

  /**
   * Loads the data of the given tile (or strip) in this raster.
   * If the data is compressed, this function decompresses it. The loaded data is stored in the
   * {@link #currentTile} field. If the given tile is already loaded, this function does nothing.
   * The field {@link #currentTileIndex} is updated to point to the given tile index.
   * on the fly.
   * @param desiredTileIndex the index of the tile to read
   * @throws IOException if an error happens while reading the file
   */
  protected void readTileData(int desiredTileIndex) throws IOException {
    if (currentTile != null && this.currentTileIndex == desiredTileIndex)
      return;
    this.currentTileIndex = desiredTileIndex;
    this.currentTile = getTile(desiredTileIndex);
  }

  public final int getTileWidth() {
    return tileWidth;
  }

  public final int getTileHeight() {
    return tileHeight;
  }

  public final int getNumSamples() {
    return bitsPerSample.length;
  }

  /**
   * Returns all the sample values at the given pixel location as integer. If these samples are stored in other formats,
   * e.g., byte or short, they are converted to integer with the same value. If the samples are represented as
   * floating-point numbers, they are rounded to the nearest integer.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param value the array of values to output to
   * @throws IOException if an error happens while reading the pixel value
   */
  public void getPixelSamplesAsInt(int iPixel, int jPixel, int[] value) throws IOException {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsInt(iPixel, jPixel, iSample);
  }

  /**
   * Returns all the sample values at the given pixel location as double. If these samples are stored in other formats,
   * e.g., byte or short, they are converted to double with the same value. If the samples are represented as
   * floating-point numbers, their value is returned in double format.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param value the array of values to output to
   * @throws IOException if an error happens while reading the pixel value
   */
  public void getPixelSamplesAsDouble(int iPixel, int jPixel, double[] value) throws IOException {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsDouble(iPixel, jPixel, iSample);
  }

  /**
   * Returns the list of IFD entries for this raster.
   * @return the list of all IFD Entries
   */
  public AbstractIFDEntry[] getEntries() {
    return entries;
  }

  /**
   * Returns the IFD Entry for the given tag and associated with this raster.
   * @param tag the tag of the entry to read
   * @return the IFDEntry with the given tag.
   */
  public AbstractIFDEntry getEntry(short tag) {
    // TODO if the list is long, use binary search as the entries are already sorted by tag
    for (AbstractIFDEntry entry : entries) {
      if (entry.tag == tag)
        return entry;
    }
    return null;
  }

  /**
   * Write all entry data to the given print stream; used for debugging purposes.
   * @param out the print stream to dump the entries to, e.g., System.out
   * @throws IOException if an error happens while writing to the PrintStream
   */
  public void dumpEntries(PrintStream out) throws IOException {
    ByteBuffer buffer = null;
    for (AbstractIFDEntry entry : entries) {
      buffer = reader.readEntry(entry, buffer);
      out.printf("#%d ", entry.tag & 0xffff);
      String tagName = TiffConstants.TagNames.get(entry.tag);
      out.print(tagName != null? tagName : "Unknown Tag");
      out.print(" - ");
      if (entry.getCountAsInt() > 1)
        out.print("[");
      switch (entry.type) {
        case TiffConstants.TYPE_BYTE:
        default:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.get() & 0xff);
          }
          break;
        case TiffConstants.TYPE_SBYTE:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.get());
          }
          break;
        case TiffConstants.TYPE_SHORT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getShort() & 0xffff);
          }
          break;
        case TiffConstants.TYPE_SSHORT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getShort());
          }
          break;
        case TiffConstants.TYPE_LONG:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getInt() & 0xffffffffL);
          }
          break;
        case TiffConstants.TYPE_SLONG:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getInt());
          }
          break;
        case TiffConstants.TYPE_ASCII:
          out.print('"');
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            char c = (char) buffer.get();
            if (c == 0) {
              out.print('"');
              if ($i < entry.getCountAsInt() - 1) {
                out.print(", \"");
              }
            } else {
              out.print(c);
            }
          }
          break;
        case TiffConstants.TYPE_FLOAT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%f", Float.intBitsToFloat(buffer.getInt()));
          }
          break;
        case TiffConstants.TYPE_DOUBLE:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%f", Double.longBitsToDouble(buffer.getLong()));
          }
          break;
      }
      if (entry.getCountAsInt() > 1)
        out.print("]");
      out.println();
    }
  }

  /**
   * Returns all the values in the given entry as an integer array. This method has to implemented at the TiffReader
   * level as it might need to read the underlying file.
   * @param entry the entry to get its value
   * @return the array of value as int[]
   * @throws IOException if an error happens while reading the file
   */
  public int[] getIntValues(AbstractIFDEntry entry) throws IOException {
    int[] values = new int[entry.getCountAsInt()];
    buffer = reader.readEntry(entry, buffer);
    switch (entry.type) {
      case TiffConstants.TYPE_BYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xff & buffer.get();
        break;
      case TiffConstants.TYPE_SBYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.get();
        break;
      case TiffConstants.TYPE_SHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xffff & buffer.getShort();
        break;
      case TiffConstants.TYPE_SSHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getShort();
        break;
      case TiffConstants.TYPE_LONG:
        // TODO we should handle unsigned 32-bit integers that are larger than INT_MAX
      case TiffConstants.TYPE_SLONG:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getInt();
        break;
      case TiffConstants.TYPE_LONG8:
      case TiffConstants.TYPE_SLONG8:
      case TiffConstants.TYPE_IFD8:
        for (int $i = 0; $i < values.length; $i++) {
          long value = buffer.getLong();
          if (value > Integer.MAX_VALUE)
            throw new RuntimeException("Value too big");
          values[$i] = (int) value;
        }
        break;
      default:
        throw new RuntimeException(String.format("Unsupported type %d", entry.type));
    }
    return values;
  }


  /**
   * Returns all the values in the given entry as an integer array. This method has to implemented at the TiffReader
   * level as it might need to read the underlying file.
   * @param entry the entry to get its value
   * @return the array of values as long[]
   * @throws IOException if an error happens while reading the file
   */
  public long[] getLongValues(AbstractIFDEntry entry) throws IOException {
    long[] values = new long[entry.getCountAsInt()];
    buffer = reader.readEntry(entry, buffer);
    switch (entry.type) {
      case TiffConstants.TYPE_BYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xff & buffer.get();
        break;
      case TiffConstants.TYPE_SBYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.get();
        break;
      case TiffConstants.TYPE_SHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xffff & buffer.getShort();
        break;
      case TiffConstants.TYPE_SSHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getShort();
        break;
      case TiffConstants.TYPE_LONG:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = (long)buffer.getInt() & 0xffffffffL;
        break;
        // TODO we should handle unsigned 32-bit integers that are larger than INT_MAX
      case TiffConstants.TYPE_SLONG:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getInt();
        break;
      case TiffConstants.TYPE_LONG8:
      case TiffConstants.TYPE_SLONG8:
      case TiffConstants.TYPE_IFD8:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getLong();
        break;
      default:
        throw new RuntimeException(String.format("Unsupported type %d", entry.type));
    }
    return values;
  }

  /**
   * Return the offset of the given tile in the file.
   * @param tileID the ID of the tile to retrieve its offset in the file.
   * @return the offset of the tile or -1 if the tile is not stored in the file
   */
  public long getTileOffset(int tileID) {
    return tileOffsets[tileID];
  }

  public int getBitsPerPixel() {
    return bitsPerPixel;
  }
}
