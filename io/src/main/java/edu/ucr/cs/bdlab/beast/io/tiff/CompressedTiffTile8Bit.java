package edu.ucr.cs.bdlab.beast.io.tiff;

import com.esotericsoftware.kryo.DefaultSerializer;

/**
 * A specialized version of CompressedTiffTile where bits per sample is 8-bits.
 * Avoids bit extraction and directly extracts the corresponding bytes.
 */
@DefaultSerializer(TiffTileSerializer.class)
public class CompressedTiffTile8Bit extends CompressedTiffTile {
  public CompressedTiffTile8Bit(byte[] tileData, int compressionScheme, int predictor, int[] bitsPerSample,
                                int[] sampleFormats, int bitsPerPixel, int i1, int j1, int i2, int j2,
                                int planarConfiguration, boolean littleEndian) {
    super(new byte[][] {tileData}, compressionScheme, predictor, bitsPerSample, sampleFormats, bitsPerPixel, i1, j1, i2, j2,
        planarConfiguration, littleEndian);
    if (planarConfiguration != TiffConstants.ChunkyFormat)
      throw new RuntimeException("Unsupported planarConfiguration "+planarConfiguration);
  }

  @Override
  public long getRawSampleValue(int iPixel, int jPixel, int iSample) {
    switch (planarConfiguration) {
      case TiffConstants.ChunkyFormat:
        int pixelOffset = ((jPixel - j1) * (i2 - i1 + 1) + (iPixel - i1)) * bitsPerSample.length;
        return getTileData()[0][pixelOffset + iSample] & 0xff;
      case TiffConstants.PlanarFormat:
      default:
        throw new RuntimeException("Unsupported RawSampleValue with PlanarFormat");
    }
  }

  @Override
  public int getSampleValueAsInt(int iPixel, int jPixel, int iSample) {
    return (int) getRawSampleValue(iPixel, jPixel, iSample);
  }
}
