package edu.ucr.cs.bdlab.beast.io.tiff;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo serializer for TiffTiles.
 */
public class TiffTileSerializer extends Serializer<AbstractTiffTile> {
  @Override
  public void write(Kryo kryo, Output output, AbstractTiffTile tile) {
    // Write abstract tiff tile information
    output.writeInt(tile.i1);
    output.writeInt(tile.i2);
    output.writeInt(tile.j1);
    output.writeInt(tile.j2);
    output.writeInt(tile.getNumSamples());
    output.writeInts(tile.bitsPerSample);
    output.writeInt(tile.bitsPerPixel);
    output.writeInt(tile.planarConfiguration);
    output.writeBoolean(tile.littleEndian);
    output.writeInts(tile.sampleFormats);
    // Write compressed tiff tile information
    CompressedTiffTile ctile = (CompressedTiffTile) tile;
    ctile.compress();
    output.writeInt(ctile.tileData.length);
    for (int iBand = 0; iBand < ctile.tileData.length; iBand++) {
      output.writeInt(ctile.tileData[iBand].length);
      output.writeBytes(ctile.tileData[iBand]);
    }
    output.writeInt(ctile.compressionScheme);
    output.writeInt(ctile.predictor);
    output.writeInt(ctile.jpegTable == null? 0 : ctile.jpegTable.length);
    if (ctile.jpegTable != null)
      output.writeBytes(ctile.jpegTable);
  }

  @Override
  public AbstractTiffTile read(Kryo kryo, Input input, Class<AbstractTiffTile> aClass) {
    // Construct the tiff tile first
    int i1 = input.readInt();
    int i2 = input.readInt();
    int j1 = input.readInt();
    int j2 = input.readInt();
    int numSamples = input.readInt();
    int[] bitsPerSample = input.readInts(numSamples);
    int bitsPerPixel = input.readInt();
    int planarConfiguration = input.readInt();
    boolean littleEndian = input.readBoolean();
    int[] sampleFormats = input.readInts(numSamples);
    int numPlanes = input.readInt();
    byte[][] tileData = new byte[numPlanes][];
    for (int iPlane = 0; iPlane < numPlanes; iPlane++) {
      int dataLength = input.readInt();
      byte[] data = input.readBytes(dataLength);
      tileData[iPlane] = data;
    }
    int compressionScheme = input.readInt();
    int predictor = input.readInt();
    byte[] jpegTable = null;
    int jpegTableSize = input.readInt();
    if (jpegTableSize > 0)
      jpegTable = input.readBytes(jpegTableSize);
    boolean all8bits = true;
    int i = 0;
    while (i < numSamples && all8bits)
      all8bits = bitsPerSample[i++] == 8;
    AbstractTiffTile tiffTile = all8bits && numPlanes == 1?
        new CompressedTiffTile8Bit(tileData[0], compressionScheme, predictor, bitsPerSample,
            sampleFormats, bitsPerPixel, i1, j1, i2, j2, planarConfiguration, littleEndian) :
        new CompressedTiffTile(tileData, compressionScheme, predictor, bitsPerSample,
            sampleFormats, bitsPerPixel, i1, j1, i2, j2, planarConfiguration, littleEndian);
    if (jpegTable != null)
      ((CompressedTiffTile)tiffTile).setJpegTable(jpegTable);
    return tiffTile;
  }
}
