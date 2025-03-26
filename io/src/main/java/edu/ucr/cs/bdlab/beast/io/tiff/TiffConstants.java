/*
 * Copyright 2021 University of California, Riverside
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

import java.util.HashMap;
import java.util.Map;

public class TiffConstants {
  /** 8-bit unsigned integer */
  public static final short TYPE_BYTE = 1;
  /** 8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero). */
  public static final short TYPE_ASCII = 2;
  /** 16-bit (2-byte) unsigned integer */
  public static final short TYPE_SHORT = 3;
  /** 32-bit (4-byte) unsigned integer */
  public static final short TYPE_LONG = 4;
  /** Two LONGs: the first represents the numerator of a fraction; the second, the denominator */
  public static final short TYPE_RATIONAL = 5;
  /** An 8-bit signed (twos-complement) integer */
  public static final short TYPE_SBYTE = 6;
  /** An 8-bit byte that may contain anything, depending on the definition of the field */
  public static final short TYPE_UNDEFINED = 7;
  /** A 16-bit (2-byte) signed (twos-complement) integer */
  public static final short TYPE_SSHORT = 8;
  /** A 32-bit (4-byte) signed (twos-complement) integer */
  public static final short TYPE_SLONG = 9;
  /** Two SLONG's: the first represents the numerator of a fraction, the second the denominator */
  public static final short TYPE_SRATIONAL = 10;
  /** Single precision (4-byte) IEEE format */
  public static final short TYPE_FLOAT = 11;
  /** Double precision (8-byte) IEEE format */
  public static final short TYPE_DOUBLE = 12;
  /** A 64-bit unsigned long */
  public static final short TYPE_LONG8 = 16;
  /** A 64-bit unsigned long */
  public static final short TYPE_SLONG8 = 17;
  /** A 64-bit unsigned long IFD offset */
  public static final short TYPE_IFD8 = 18;
  /**
   * No compression, but pack data into bytes as tightly as possible, leaving no unused
   * bits (except at the end of a row). The component values are stored as an array of
   * type BYTE. Each scan line (row) is padded to the next BYTE boundary.
   */
  public static final int COMPRESSION_NONE = 1;
  /**1-Dimensional Modified Huffman run length encoding. See Section 10 in the tiff6 specs*/
  public static final int COMPRESSION_CCITT_HUFFMAN = 2;
  /**LZW compression scheme*/
  public static final int COMPRESSION_LZW = 5;
  /**JPEG compression*/
  public static final int COMPRESSION_JPEG = 6;
  /**YCbCr JPEG compression*/
  public static final int COMPRESSION_JPEG2 = 7;
  /**Deflate compression*/
  public static final int COMPRESSION_DEFLATE = 8;
  /**Packbits compression (run-length encoding)*/
  public static final int COMPRESSION_PACKBITS = 32773;

  public static final int SAMPLE_UNSIGNED_INT = 1;
  public static final int SAMPLE_SIGNED_INT = 2;
  public static final int SAMPLE_IEEE_FLOAT = 3;
  public static final int SAMPLE_UNDEFINED = 4;

  /**TIFF file signature*/
  public static final short SIGNATURE = 42;
  /**TIFF file signature for big tiff files*/
  public static final short BIG_SIGNATURE = 43;

  /**
   * 1 = Chunky format. The component values for each pixel are stored contiguously.
   * The order of the components within the pixel is specified by
   * PhotometricInterpretation. For example, for RGB data, the data is stored as RGBRGBRGB...
   */
  public static final int ChunkyFormat = 1;

  /**
   * 2 = Planar format. The components are stored in separate "component planes." The
   * values in StripOffsets and StripByteCounts are then arranged as a 2-dimensional
   * array, with SamplesPerPixel rows and StripsPerImage columns. (All columns for row 0 are stored first,
   * followed by the columns of row 1, and so on.)
   * PhotometricInterpretation describes the type of data stored in each component plane.
   * For example, RGB data is stored with the Red components in one component plane, the Green in another,
   * and the Blue in another.
   */
  public static final int PlanarFormat = 2;

  public static final int PhotometricGrayscale = 1;
  public static final int PhotometricRGB = 2;
  public static final int PhotometricCMY = 5;

  /**
   * This value indicates that the image data is in the YCbCr color space. TIFF uses the international
   * standard notation YCbCr for color-difference sample coding. Y is the luminance component.
   * Cb and Cr are the two chrominance components. RGB pixels are converted to and from
   * YCbCr for storage and display.
   */
  public static final int PhotometricYCbCr = 6;

  /**For debugging purposes, a map from each tag to a humean readable name*/
  public static final Map<Short, String> TagNames = new HashMap<>();

  /**Marker for little endian TIFF file (in the header)*/
  public static final short LITTLE_ENDIAN = 0x4949;

  /**Marker for big endian TIFF file (in the header)*/
  public static final short BIG_ENDIAN = 0x4D4D;

  static {
    // Initialize TagNames
    TiffConstants.TagNames.put(TiffConstants.TAG_NEW_SUBFILE_TYPE, "New Subfile Type");
    TiffConstants.TagNames.put(TiffConstants.TAG_IMAGE_WIDTH, "Image Width");
    TiffConstants.TagNames.put(TiffConstants.TAG_IMAGE_LENGTH, "Image Length");
    TiffConstants.TagNames.put(TiffConstants.TAG_BITS_PER_SAMPLE, "Bits per sample");
    TiffConstants.TagNames.put(TiffConstants.TAG_COMPRESSION, "Compression");
    TiffConstants.TagNames.put(TiffConstants.TAG_PHOTOMETRIC_INTERPRETATION, "Photometric Interpretation");
    TiffConstants.TagNames.put(TiffConstants.TAG_DOCUMENT_NAME, "Document Name");
    TiffConstants.TagNames.put(TiffConstants.TAG_IMAGE_DESCRIPTION, "Image Description");
    TiffConstants.TagNames.put(TiffConstants.TAG_STRIP_OFFSETS, "Strip Offsets");
    TiffConstants.TagNames.put(TiffConstants.TAG_ORIENTATION, "Orientation");
    TiffConstants.TagNames.put(TiffConstants.TAG_SAMPLES_PER_PIXEL, "Samples per Pixel");
    TiffConstants.TagNames.put(TiffConstants.TAG_ROWS_PER_STRIP, "Rows pre Strip");
    TiffConstants.TagNames.put(TiffConstants.TAG_STRIP_BYTE_COUNTS, "Strip Byte Counts");
    TiffConstants.TagNames.put(TiffConstants.TAG_XRESOLUTION, "X Resolution");
    TiffConstants.TagNames.put(TiffConstants.TAG_YRESOLUTION, "Y Resolution");
    TiffConstants.TagNames.put(TiffConstants.TAG_PLANAR_CONFIGURATION, "Planar Configuration");
    TiffConstants.TagNames.put(TiffConstants.TAG_RESOLUTION_UNIT, "Resolution Unit");
    TiffConstants.TagNames.put(TiffConstants.TAG_PAGE_NUMBER, "Page Number");
    TiffConstants.TagNames.put(TiffConstants.TAG_SOFTWARE, "Software");
    TiffConstants.TagNames.put(TiffConstants.TAG_DATETIME, "DateTime");
    TiffConstants.TagNames.put(TiffConstants.TAG_PREDICTOR, "Predictor");
    TiffConstants.TagNames.put(TiffConstants.TAG_COLOR_MAP, "Color Map");
    TiffConstants.TagNames.put(TiffConstants.TAG_TILE_WIDTH, "Tile Width");
    TiffConstants.TagNames.put(TiffConstants.TAG_TILE_LENGTH, "Tile Length");
    TiffConstants.TagNames.put(TiffConstants.TAG_TILE_OFFSETS, "Tile Offsets");
    TiffConstants.TagNames.put(TiffConstants.TAG_TILE_BYTE_COUNTS, "Tile Byte Counts");
    TiffConstants.TagNames.put(TiffConstants.TAG_EXTRA_SAMPLES, "Extra Samples");
    TiffConstants.TagNames.put(TiffConstants.TAG_SAMPLE_FORMAT, "Sample Format");
    TiffConstants.TagNames.put(TiffConstants.TAG_FILL_ORDER, "Fill Order");
    TiffConstants.TagNames.put(TiffConstants.TAG_WHITE_POINT, "White Point");
    TiffConstants.TagNames.put(TiffConstants.TAG_PRIMARY_CHROMATICITIES, "Primary Chromaticities");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_TABLES, "JPEG Tables");
    TiffConstants.TagNames.put(TiffConstants.TAG_REFERENCE_BLACK_WHITE, "Reference black white");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_PROC, "JPEG process");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_INTERCHANGE_FORMAT, "JPEG interchange format");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_INTERCHANGE_FORMAT_LENGTH, "JPEG interchange format length");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_RESTART_INTERVAL, "JPEG restart interval");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_LOSSLESS_PREDICTORS, "JPEG lossless predictor-selection values");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_POINT_TRANSFORMS, "JPEG list of point transforms values");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_QTABLES, "JPEG list of offsets to the quantization tables");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_DC_TABLES, "JPEG list of offsets to the DC Huffman tables");
    TiffConstants.TagNames.put(TiffConstants.TAG_JPEG_AC_TABLES, "JPEG list of offsets to the Huffman AC tables");
    TiffConstants.TagNames.put(TiffConstants.TAG_YCbCr_SUBSAMPLING, "Subsampling factors for the chrominance components of YCbCr image");
  }

  /**
   * Type = LONG
   * N = 1
   * Replaces the old SubfileType field, due to limitations in the definition of that field.
   * NewSubfileType is mainly useful when there are multiple subfiles in a single
   * TIFF file.
   * This field is made up of a set of 32 flag bits. Unused bits are expected to be 0. Bit 0 is the low-order bit.
   * Currently, defined values are:
   * Bit 0 is 1 if the image is a reduced-resolution version of another image in this TIFF file;
   * else the bit is 0.
   * Bit 1 is 1 if the image is a single page of a multi-page image (see the PageNumber field
   * description); else the bit is 0.
   * Bit 2 is 1 if the image defines a transparency mask for another image in this TIFF file.
   * The PhotometricInterpretation value must be 4, designating a transparency mask.
   * These values are defined as bit flags because they are independent of each other.
   * Default is 0.
   */
  public static final short TAG_NEW_SUBFILE_TYPE = 254;

  /** The number of columns in the image, i.e., the number of pixels per scanline */
  public static final short TAG_IMAGE_WIDTH = 256;

  /** The number of rows (sometimes described as scanlines) in the image */
  public static final short TAG_IMAGE_LENGTH = 257;

  /**
   * The number of bits per component.
   * Allowable values for Baseline TIFF grayscale images are 4 and 8, allowing either 16 or 256 distinct shades of gray
   */
  public static final short TAG_BITS_PER_SAMPLE = 258;
  /**
   * 1 = No compression, but pack data into bytes as tightly as possible, leaving no unused
   * bits (except at the end of a row). The component values are stored as an array of
   * type BYTE. Each scan line (row) is padded to the next BYTE boundary.
   * 2 = CCITT Group 3 1-Dimensional Modified Huffman run length encoding.
   * See Section 10 for a description of Modified Huffman Compression.
   * 32773 = PackBits compression, a simple byte-oriented run length scheme. See the PackBits section for details.
   * Data compression applies only to raster image data. All other TIFF fields are unaffected.
   * Baseline TIFF readers must handle all three compression schemes
   */
  public static final short TAG_COMPRESSION = 259;
  /**
   * 0 = WhiteIsZero. For bilevel and grayscale images: 0 is imaged as white. The maximum
   * value is imaged as black. This is the normal value for Compression=2.
   * 1 = BlackIsZero. For bilevel and grayscale images: 0 is imaged as black. The maximum
   * value is imaged as white. If this value is specified for Compression=2, the
   * image should display and print reversed.
   * See also: https://www.awaresystems.be/imaging/tiff/tifftags/photometricinterpretation.html
   */
  public static final short TAG_PHOTOMETRIC_INTERPRETATION = 262;

  public static final int PHOTOMETRIC_MINISWHITE = 0;
  public static final int PHOTOMETRIC_MINISBLACK = 1;
  public static final int PHOTOMETRIC_RGB = 2;
  public static final int PHOTOMETRIC_PALETTE = 3;
  public static final int PHOTOMETRIC_MASK = 4;
  public static final int PHOTOMETRIC_SEPARATED = 5;
  public static final int PHOTOMETRIC_YCBCR = 6;
  public static final int PHOTOMETRIC_CIELAB = 8;
  public static final int PHOTOMETRIC_ICCLAB = 9;
  public static final int PHOTOMETRIC_ITULAB = 10;
  public static final int PHOTOMETRIC_LOGL = 32844;
  public static final int PHOTOMETRIC_LOGLUV = 32845;

  /**
   * The logical order of bits within a byte.
   * Type = SHORT
   * N = 1
   * 1 = pixels are arranged within a byte such that pixels with lower column values are
   * stored in the higher-order bits of the byte.
   * 1-bit uncompressed data example: Pixel 0 of a row is stored in the high-order bit
   * of byte 0, pixel 1 is stored in the next-highest bit, ..., pixel 7 is stored in the low-
   * order bit of byte 0, pixel 8 is stored in the high-order bit of byte 1, and so on.
   * CCITT 1-bit compressed data example: The high-order bit of the first compres-
   * sion code is stored in the high-order bit of byte 0, the next-highest bit of the first
   * compression code is stored in the next-highest bit of byte 0, and so on.
   * 2 = pixels are arranged within a byte such that pixels with lower column values are
   * stored in the lower-order bits of the byte.
   */
  public static final short TAG_FILL_ORDER = 266;

  /**
   * The name of the document from which this image was scanned.
   * Type = ASCII
   */
  public static final short TAG_DOCUMENT_NAME = 269;

  /** A string that describes the subject of the image. */
  public static final short TAG_IMAGE_DESCRIPTION = 270;

  /**
   * Type = SHORT or LONG
   * For each strip, the byte offset of that strip.
   */
  public static final short TAG_STRIP_OFFSETS = 273;
  /**
   * The orientation of the image with respect to the rows and columns.
   * Type = SHORT
   * N = 1
   * 1 = The 0th row represents the visual top of the image, and the 0th column represents the visual left-hand side.
   * 2 = The 0th row represents the visual top of the image, and the 0th column represents the visual right-hand side.
   * 3 = The 0th row represents the visual bottom of the image, and the 0th column represents the visual right-hand side.
   * 4 = The 0th row represents the visual bottom of the image, and the 0th column represents the visual left-hand side.
   * 5 = The 0th row represents the visual left-hand side of the image, and the 0th column represents the visual top.
   * 6 = The 0th row represents the visual right-hand side of the image, and the 0th column represents the visual top.
   * 7 = The 0th row represents the visual right-hand side of the image, and the 0th column represents the visual bottom.
   * 8 = The 0th row represents the visual left-hand side of the image, and the 0th column represents the visual bottom.
   * Default is 1.
   */
  public static final short TAG_ORIENTATION = 274;

  /**
   * Type = SHORT
   * The number of components per pixel. This number is 3 for RGB images, unless
   * extra samples are present. See the ExtraSamples field for further information.
   */
  public static final short TAG_SAMPLES_PER_PIXEL = 277;

  /**
   * Type = SHORT or LONG
   * The number of rows in each strip (except possibly the last strip.)
   * For example, if ImageLength is 24, and RowsPerStrip is 10, then there are 3
   * strips, with 10 rows in the first strip, 10 rows in the second strip, and 4 rows in the
   * third strip. (The data in the last strip is not padded with 6 extra rows of dummy data.)
   */
  public static final short TAG_ROWS_PER_STRIP = 278;

  /**
   * Type = SHORT or LONG
   * For each strip, the number of bytes in that strip after any compression.
   *
   */
  public static final short TAG_STRIP_BYTE_COUNTS = 279;

  /**
   * Type = RATIONAL
   * The number of pixels per ResolutionUnit in the ImageWidth (typically, horizontal see Orientation) direction.
   */
  public static final short TAG_XRESOLUTION = 282;

  /**
   * Type = RATIONAL
   * The number of pixels per ResolutionUnit in the ImageLength (typically, vertical) direction.
   */
  public static final short TAG_YRESOLUTION = 283;

  /**
   * How the components of each pixel are stored.
   * Type = SHORT
   * N = 1
   * 1 = Chunky format. The component values for each pixel are stored contiguously.
   * The order of the components within the pixel is specified by
   * PhotometricInterpretation. For example, for RGB data, the data is stored as
   * RGBRGBRGB...
   * 2 = Planar format. The components are stored in separate "component planes." The
   * values in StripOffsets and StripByteCounts are then arranged as a 2-dimensional
   * array, with SamplesPerPixel rows and StripsPerImage columns. (All of the columns
   * for row 0 are stored first, followed by the columns of row 1, and so on.)
   * PhotometricInterpretation describes the type of data stored in each component
   * plane. For example, RGB data is stored with the Red components in one component plane,
   * the Green in another, and the Blue in another.
   * PlanarConfiguration=2 is not currently in widespread use and it is not recommended for general interchange.
   * It is used as an extension and Baseline TIFF
   * readers are not required to support it.
   * If SamplesPerPixel is 1, PlanarConfiguration is irrelevant, and need not be included.
   * If a row interleave effect is desired, a writer might write out the data as
   * PlanarConfiguration=2 -separate sample planes- but break up the planes into
   * multiple strips (one row per strip, perhaps) and interleave the strips.
   * Default is 1. See also BitsPerSample, SamplesPerPixel.
   */
  public static final short TAG_PLANAR_CONFIGURATION = 284;

  /**
   * Applications often want to know the size of the picture represented by an image.
   * This information can be calculated from ImageWidth and ImageLength given the
   * following resolution data:
   * 1 = No absolute unit of measurement. Used for images that may have a non-square
   * aspect ratio but no meaningful absolute dimensions.
   * 2 = Inch.
   * 3 = Centimeter.
   * Default = 2 (inch).
   */
  public static final short TAG_RESOLUTION_UNIT = 296;

  /**
   * The page number of the page from which this image was scanned.
   * Type = SHORT
   * N = 2
   * This field is used to specify page numbers of a multiple page (e.g. facsimile) document.
   * PageNumber[0] is the page number; PageNumber[1] is the total number of pages in the document.
   * If PageNumber[1] is 0, the total number of pages in the document is not available.
   * Pages need not appear in numerical order.
   * The first page is numbered 0 (zero).
   * No default.
   */
  public static final short TAG_PAGE_NUMBER = 297;

  /**
   * Name and version number of the software package(s) used to create the image
   * Type = ASCII
   */
  public static final short TAG_SOFTWARE = 305;

  /**
   * Date and time of image creation. The format is: “YYYY:MM:DD HH:MM:SS”, with hours like those on a 24-hour clock,
   * and one space character between the date and the time.
   * The length of the string, including the terminating NUL, is 20 bytes.
   * Type = ASCII
   */
  public static final short TAG_DATETIME = 306;

  /**
   * Type = SHORT
   * N = 1
   * A predictor is a mathematical operator that is applied to the image data before an
   * encoding scheme is applied. Currently, this field is used only with LZW (Compression=5) encoding because
   * LZW is probably the only TIFF encoding scheme that benefits significantly from a predictor step. See Section 13.
   * The possible values are:
   * 1 = No prediction scheme used before coding.
   * 2 = Horizontal differencing.
   * 3 = Floating-point predictor. Horizontal floating-point differencing.
   * Default is 1.
   */
  public static final short TAG_PREDICTOR = 317;

  /**
   * Type = RATIONAL
   * N = 2
   * The chromaticity of the white point of the image. This is the chromaticity when
   * each of the primaries has its ReferenceWhite value. The value is described using
   * the 1931 CIE xy chromaticity diagram and only the chromaticity is specified.
   * This value can correspond to the chromaticity of the alignment white of a monitor,
   * the filter set and light source combination of a scanner or the imaging model of a
   * rendering package. The ordering is white[x], white[y].
   * For example, the CIE Standard Illuminant D65 used by CCIR Recommendation
   * 709 and Kodak PhotoYCC is:
   * 3127/10000,3290/10000
   * No default.
   */
  public static final short TAG_WHITE_POINT = 318;

  /**
   * Type = RATIONAL
   * N = 6
   * The chromaticities of the primaries of the image. This is the chromaticity for each
   * of the primaries when it has its ReferenceWhite value and the other primaries
   * have their ReferenceBlack values. These values are described using the 1931 CIE
   * xy chromaticity diagram and only the chromaticities are specified. These values
   * can correspond to the chromaticities of the phosphors of a monitor, the filter set
   * and light source combination of a scanner or the imaging model of a rendering
   * package. The ordering is red[x], red[y], green[x], green[y], blue[x], and blue[y].
   * For example the CCIR Recommendation 709 primaries are:
   * 640/1000,330/1000,
   * 300/1000, 600/1000,
   * 150/1000, 60/1000
   * No default.
   */
  public static final short TAG_PRIMARY_CHROMATICITIES = 319;

  /**
   * Type = SHORT
   * N = 3 * (2**BitsPerSample)
   * This field defines a Red-Green-Blue color map (often called a lookup table) for
   * palette color images. In a palette-color image, a pixel value is used to index into an
   * RGB-lookup table. For example, a palette-color pixel having a value of 0 would
   * be displayed according to the 0th Red, Green, Blue triplet.
   * In a TIFF ColorMap, all the Red values come first, followed by the Green values,
   * then the Blue values. In the ColorMap, black is represented by 0,0,0 and white is
   * represented by 65535, 65535, 65535.
   */
  public static final short TAG_COLOR_MAP = 320;

  /**
   * Type = SHORT or LONG
   * N
   * = 1
   * The tile width in pixels. This is the number of columns in each tile.
   * Assuming integer arithmetic, three computed values that are useful in the follow-
   * ing field descriptions are:
   * TilesAcross = (ImageWidth + TileWidth - 1) / TileWidth
   * TilesDown = (ImageLength + TileLength - 1) / TileLength
   * TilesPerImage = TilesAcross * TilesDown
   * These computed values are not TIFF fields; they are simply values determined by
   * the ImageWidth, TileWidth, ImageLength, and TileLength fields.
   * TileWidth and ImageWidth together determine the number of tiles that span the
   * width of the image (TilesAcross). TileLength and ImageLength together determine the number of tiles that span
   * the length of the image (TilesDown).
   * We recommend choosing TileWidth and TileLength such that the resulting tiles
   * are about 4K to 32K bytes before compression. This seems to be a reasonable
   * value for most applications and compression schemes.
   * TileWidth must be a multiple of 16. This restriction improves performance in
   * some graphics environments and enhances compatibility with compression
   * schemes such as JPEG.
   * Tiles need not be square.
   * Note that ImageWidth can be less than TileWidth, although this means that the
   * tiles are too large or that you are using tiling on really small images, neither of
   * which is recommended. The same observation holds for ImageLength and
   * TileLength.
   * No default. See also TileLength, TileOffsets, TileByteCounts.
   */
  public static final short TAG_TILE_WIDTH = 322;

  /**
   * Type = SHORT or LONG
   * N = 1
   * The tile length (height) in pixels. This is the number of rows in each tile.
   * TileLength must be a multiple of 16 for compatibility with compression schemes
   * such as JPEG.
   * Replaces RowsPerStrip in tiled TIFF files.
   * No default. See also TileWidth, TileOffsets, TileByteCounts.
   */
  public static final short TAG_TILE_LENGTH = 323;

  /**
   * Type = LONG
   * N = TilesPerImage for PlanarConfiguration = 1
   * N = SamplesPerPixel * TilesPerImage for PlanarConfiguration = 2
   * For each tile, the byte offset of that tile, as compressed and stored on disk. The
   * offset is specified with respect to the beginning of the TIFF file. Note that this
   * implies that each tile has a location independent of the locations of other tiles.
   * Offsets are ordered left-to-right and top-to-bottom. For PlanarConfiguration = 2,
   * the offsets for the first component plane are stored first, followed by all the offsets
   * for the second component plane, and so on.
   * No default. See also TileWidth, TileLength, TileByteCounts.
   */
  public static final short TAG_TILE_OFFSETS = 324;

  /**
   * Type = SHORT or LONG
   * N = TilesPerImage for PlanarConfiguration = 1
   * N = SamplesPerPixel * TilesPerImage for PlanarConfiguration = 2
   * For each tile, the number of (compressed) bytes in that tile.
   * See TileOffsets for a description of how the byte counts are ordered.
   * No default. See also TileWidth, TileLength, TileOffsets.
   */
  public static final short TAG_TILE_BYTE_COUNTS = 325;

  /**
   * Type = SHORT
   * N = m
   * Specifies that each pixel has m extra components whose interpretation is defined
   * by one of the values listed below. When this field is used, the SamplesPerPixel
   * field has a value greater than the PhotometricInterpretation field suggests.
   * For example, full-color RGB data normally has SamplesPerPixel=3. If
   * SamplesPerPixel is greater than 3, then the ExtraSamples field describes the
   * meaning of the extra samples. If SamplesPerPixel is, say, 5 then ExtraSamples
   * will contain 2 values, one for each extra sample.
   * ExtraSamples is typically used to include non-color information, such as opacity,
   * in an image. The possible values for each item in the field's value are:
   * 0 = Unspecified data
   * 1 = Associated alpha data (with pre-multiplied color)
   * 2 = Unassociated alpha data
   * For further details, check pages 31 and 32 of the TIFF specs
   */
  public static final short TAG_EXTRA_SAMPLES = 338;

  /**
   * Type = SHORT
   * N = SamplesPerPixel
   * This field specifies how to interpret each data sample in a pixel. Possible values
   * are:
   * 1 = unsigned integer data
   * 2 = two's complement signed integer data
   * 3 = IEEE floating point data [IEEE]
   * 4 = undefined data format
   * Note that the SampleFormat field does not specify the size of data samples; this is
   * still done by the BitsPerSample field.
   * A field value of "undefined" is a statement by the writer that it did not know how
   * to interpret the data samples; for example, if it were copying an existing image. A
   * reader would typically treat an image with "undefined" data as if the field were
   * not present (i.e. as unsigned integer data).
   * Default is 1, unsigned integer data.
   */
  public static final short TAG_SAMPLE_FORMAT = 339;

  /**
   * JPEG quantization and/or Huffman tables.
   *
   * This field is the only auxiliary TIFF field added for new-style JPEG compression (Compression=7).
   * It is optional.
   *
   * The purpose of JPEGTables is to predefine JPEG quantization and/or Huffman tables for subsequent use by JPEG image
   * segments. When this is done, these rather bulky tables need not be duplicated in each segment, thus saving space
   * and processing time. JPEGTables may be used even in a single-segment file, although there is no space savings
   * in that case.
   *
   * When the JPEGTables field is present, it shall contain a valid JPEG "abbreviated table specification" datastream.
   *
   * A JPEG table starts by 0xff followed by start of image (SOI) marker 0xd8.
   * It terminates with 0xff followed by end of image (EOI) marker 0xd9.
   */
  public static final short TAG_JPEG_TABLES = 347;

  /**
   * Specifies a pair of headroom and footroom image data values (codes) for each pixel component.
   */
  public static final short TAG_REFERENCE_BLACK_WHITE = 532;

  /**
   * Type = SHORT, N=1
   * This Field indicates the JPEG process used to produce the compressed data.
   * The values for this field are defined to be consistent with the numbering convention used in ISO DIS 10918-2.
   * Two values are defined at this time.<br/>
   * 1= Baseline sequential process <br/>
   * 14= Lossless process with Huffman coding<br/>
   * When the lossless process with Huffman coding is selected by this Field, the Huffman tables used
   * to encode the image are specified by the JPEGDCTables field, and the JPEGACTables field is not used.
   * Values indicating JPEG processes other than those specified above will be defined in the future.
   * Not all the fields described in this section are relevant to the JPEG process selected by this Field.
   * The following table specifies the fields that are applicable to each value defined by this Field.
   * <table>
   *   <tr><th>TagName                    </th> <th>JPEGProc=14</th> <th>JPEGProc=14</th> </tr>
   *   <tr><td>JPEGInterchangeFormat      </td> <td>     X     </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGInterchangeFormatLength</td> <td>     X     </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGRestart Interval       </td> <td>     X     </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGLosslessPredictors     </td> <td>           </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGPointTransforms        </td> <td>           </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGQTables                </td> <td>     X     </td> <td>           </td> </tr>
   *   <tr><td>JPEGDCTables               </td> <td>     X     </td> <td>     X     </td> </tr>
   *   <tr><td>JPEGACTables               </td> <td>     X     </td> <td>           </td> </tr>
   * </table>
   * This Field is mandatory whenever the Compression Field is JPEG (no default).
   */
  public static final short TAG_JPEG_PROC = 512;

  /**
   * Type = LONG N=1
   * This Field indicates whether a JPEG interchange format bitstream is present in the TIFF file.
   * If a JPEG interchange format bitstream is present, then this Field points to the Start of Image (SOI) marker code.
   * If this Field is zero or not present, a JPEG interchange format bitstream is not present.
   */
  public static final short TAG_JPEG_INTERCHANGE_FORMAT = 513;

  /**
   * Type = LONG N=1
   * This Field indicates the length in bytes of the JPEG interchange format bitstream.
   * This Field is useful for extracting the JPEG interchange format bitstream without parsing the bitstream.
   * This Field is relevant only if the JPEGInterchangeFormat Field is present and is non-zero.
   */
  public static final short TAG_JPEG_INTERCHANGE_FORMAT_LENGTH = 514;

  /**
   * Type = SHORT N=1
   * This Field indicates the length of the restart interval used in the compressed image data.
   * The restart interval is defined as the number of Minimum Coded Units (MCUs) between restart markers.
   * Restart intervals are used in JPEG compressed images to provide support for multiple strips or tiles.
   * At the start of each restart interval, the coding state is reset to default values,
   * allowing every restart interval to be decoded independently of previously decoded data.
   * TIFF strip and tile offsets shall always point to the start of a restart interval.
   * Equivalently, each strip or tile contains an integral number of restart intervals.
   * Restart markers need not be present in a TIFF file; they are implicitly coded at the start of every strip or tile.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more information about the restart interval
   * and restart markers.
   * If this Field is zero or is not present, the compressed data does not contain restart markers.
   */
  public static final short TAG_JPEG_RESTART_INTERVAL = 515;

  /**
   * Type = SHORT, N = SamplesPerPixel
   * This Field points to a list of lossless predictor-selection values, one per component.
   * The allowed predictors are listed in the following table.
   * <table>
   * <tr> <th>Selection-value</td> <th>Prediction</th></tr>
   * <tr> <td>1</td>               <td>A 2B 3C</td></tr>
   * <tr> <td>4</td>               <td> A+B-C</td></tr>
   * <tr> <td>5</td>               <td> A+((B-C)/2)</td></tr>
   * <tr> <td>6</td>               <td> B+((A-C)/2)</td></tr>
   * <tr> <td>7</td>               <td> (A+B)/2</td></tr>
   * </table>
   * A, B, and C are the samples immediately to the left, immediately above, and diagonally to the left and above the sample to be coded, respectively.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more details.
   * This Field is mandatory whenever the JPEGProc Field specifies one of the lossless processes (no default).
   */
  public static final short TAG_JPEG_LOSSLESS_PREDICTORS = 517;

  /**
   * Type = SHORT
   * N = SamplesPerPixel
   * This Field points to a list of point transform values, one per component.
   * This Field is relevant only for lossless processes.
   * If the point transformation value is nonzero for a component, a point transformation of the input
   * is performed prior to the lossless coding. The input is divided by 2**Pt, where Pt is the point transform value.
   * The output of the decoder is rescaled to the input range by multiplying by 2**Pt.
   * Note that the scaling of input and output can be performed by arithmetic shifts.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more details.
   * The default value of this Field is 0 for each component (no scaling).
   */
  public static final short TAG_JPEG_POINT_TRANSFORMS = 518;

  /**
   * Type = LONG
   * N = SamplesPerPixel
   * This Field points to a list of offsets to the quantization tables, one per component.
   * Each table consists of 64 BYTES (one for each DCT coefficient in the 8x8 block).
   * The quantization tables are stored in zigzag order.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more details.
   * It is strongly recommended that, within the TIFF file, each component be assigned separate tables.
   * This Field is mandatory whenever the JPEGProc Field specifies a DCT-based process (no default).
   */
  public static final short TAG_JPEG_QTABLES = 519;

  /**
   * Type = LONG
   * N = SamplesPerPixel
   * This Field points to a list of offsets to the DC Huffman tables or the lossless Huffman tables, one per component.
   * The format of each table is as follows:
   * 16 BYTES of “BITS”, indicating the number of codes of lengths 1 to 16;
   * Up to 17 BYTES of “V ALUES”, indicating the values associated with those codes, in order of length.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more details.
   * It is strongly recommended that, within the TIFF file, each component be assigned separate tables.
   * This Field is mandatory for all JPEG processes (no default).
   */
  public static final short TAG_JPEG_DC_TABLES = 520;

  /**
   * Type = LONG
   * N = SamplesPerPixel
   * This Field points to a list of offsets to the Huffman AC tables, one per component.
   * The format of each table is as follows:
   * 16 BYTES of "BITS", indicating the number of codes of lengths 1 to 16;
   * Up to 256 BYTES of "VALUES", indicating the values associated with those codes, in order of length.
   * See the JPEG Draft International Standard (ISO DIS 10918-1) for more details.
   * It is strongly recommended that, within the TIFF file, each component be assigned separate tables.
   *
   * This Field is mandatory whenever the JPEGProc Field specifies a DCT-based process (no default).
   */
  public static final short TAG_JPEG_AC_TABLES = 521;

  /**
   * Specifies the subsampling factors used for the chrominance components of a YCbCr image.
   * The two fields of this field, YCbCr SubsampleHoriz and YCbCr SubsampleVert, specify the horizontal and vertical
   * subsampling factors respectively.
   * The two fields of this field are defined as follows:
   * Short 0: YCbCrSubsampleHoriz:
   * 1 = ImageWidth of this chroma image is equal to the ImageWidth of the associated luma image.
   * 2 = ImageWidth of this chroma image is half the ImageWidth of the associated luma image.
   * 4 = ImageWidth of this chroma image is one-quarter the ImageWidth of the associated luma image.
   *
   * Short 1: YCbCrSubsampleVert:
   * 1 = ImageLength (height) of this chroma image is equal to the ImageLength of the associated luma image.
   * 2 = ImageLength (height) of this chroma image is half the ImageLength of the associated luma image.
   * 4 = ImageLength (height) of this chroma image is one-quarter the ImageLength of the associated luma image.
   *
   * Both Cb and Cr have the same subsampling ratio. Also, YCbCr SubsampleVert shall always be less than or equal to
   * YCbCrSubsampleHoriz.
   *
   * ImageWidth and ImageLength are constrained to be integer multiples of YCbCrSubsampleHoriz and
   * YCbCrSubsampleVert respectively. TileWidth and TileLength have the same constraints.
   * RowsPerStrip must be an integer multiple of YCbCr SubsampleVert.
   *
   * The default values of this field are [ 2, 2 ].
   */
  public static final short TAG_YCbCr_SUBSAMPLING = 530;

  /**The size of each type in bytes*/
  public static final byte[] TypeSizes = {0, 1, 1, 2, 4, 8, 1, 1, 2, 4, 8, 4, 8, -1, -1, -1, 8, 8, 8};
}
