package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Envelope;

/**
 * Unit test for simple App.
 */
public class TileIndexTest extends TestCase {

  public void testGetMBR() {
    EnvelopeNDLite spaceMBR = new EnvelopeNDLite(2, 0.0, 0.0, 1024.0, 1024.0);
    int x = 0, y = 0, z = 0;
    Envelope tileMBR = new Envelope();
    TileIndex.getMBR(spaceMBR, z, x, y, tileMBR);
    Envelope expectedMBR = spaceMBR.toJTSEnvelope();
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    z = 1;
    expectedMBR = new Envelope(spaceMBR.getMinCoord(0), spaceMBR.getSideLength(0) / 2,
        spaceMBR.getMinCoord(1), spaceMBR.getSideLength(1) / 2);
    TileIndex.getMBR(spaceMBR, z, x, y, tileMBR);
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    z = 10;
    x = 100;
    y = 100;
    expectedMBR = new Envelope(100.0, 101.0, 100.0, 101.0);
    TileIndex.getMBR(spaceMBR, z, x, y, tileMBR);
    assertTrue("Expected MBR of ("+z+","+x+","+y+") to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));

    // Assert MBR from tile index
    long ti = TileIndex.encode(1, 1, 0);
    Envelope actualTileMBR = new Envelope();
    TileIndex.getMBR(new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0), ti, actualTileMBR);
    Envelope expectedTileMBR = new Envelope(1.0, 2.0, 0.0, 1.0);
    assertEquals(expectedTileMBR, actualTileMBR);
  }

  public void testWithNonZeroOriginal() {
    EnvelopeNDLite spaceMBR = new EnvelopeNDLite(2, 1024.0, 1024.0, 2048.0, 2048.0);
    long ti = TileIndex.encode(0, 0 ,0);
    Envelope tileMBR = new Envelope();
    TileIndex.getMBR(spaceMBR, ti, tileMBR);
    Envelope expectedMBR = spaceMBR.toJTSEnvelope();
    assertTrue("Expected MBR of "+ti+" to be "+expectedMBR+" but found to be "+tileMBR,
        expectedMBR.equals(tileMBR));
  }

  public static void assertTileEncode(int z, int x, int y) {
    long ti = TileIndex.encode(z, x, y);
    TileIndex tileIndex;
    tileIndex = TileIndex.decode(ti, new TileIndex());
    assertEquals(z, tileIndex.z);
    assertEquals(x, tileIndex.x);
    assertEquals(y, tileIndex.y);

  }

  public void testEncodeAndDecode() {
    assertTileEncode(6, 5, 7);
    assertTileEncode(0, 0, 0);
    assertTileEncode(1, 1, 0);
    assertTileEncode(16, 153, 554);
    assertTileEncode(18, 26142, 66294);
  }

}
