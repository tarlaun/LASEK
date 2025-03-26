package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class SubPyramidTest extends JavaSparkTest {

  public void testOverlappingCells() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0,0.0,10.0,10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 1, 0,0,2,2);

    java.awt.Rectangle overlaps = new java.awt.Rectangle();
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 5.0,5.0,10.0,10.0), overlaps);
    assertEquals(1, overlaps.x);
    assertEquals(1, overlaps.y);
    assertEquals(1, overlaps.width);
    assertEquals(1, overlaps.height);
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 4.9,4.9,5.1,5.1), overlaps);
    assertEquals(0, overlaps.x);
    assertEquals(0, overlaps.y);
    assertEquals(2, overlaps.width);
    assertEquals(2, overlaps.height);
  }

  public void testOverlappingCellsWithPoints() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0,0.0,10.0,10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 1, 0,0,2,2);

    java.awt.Rectangle overlaps = new java.awt.Rectangle();
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), 2);
    new PointND(new GeometryFactory(), 0.0, 0.0).envelope(e);
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(e), overlaps);
    assertEquals(0, overlaps.x);
    assertEquals(0, overlaps.y);
    assertEquals(1, overlaps.width);
    assertEquals(1, overlaps.height);
  }

  public void testOverlappingCellsAligned() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0,0.0,8.0,8.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 1, 0,0,2,2);

    java.awt.Rectangle overlaps = new java.awt.Rectangle();
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 4.0), overlaps);
    assertEquals(0, overlaps.x);
    assertEquals(0, overlaps.y);
    assertEquals(1, overlaps.width);
    assertEquals(1, overlaps.height);

    // Test special case of a point at the very end
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 8.0, 8.0, 8.0, 8.0), overlaps);
    assertEquals(1, overlaps.x);
    assertEquals(1, overlaps.y);
    assertEquals(1, overlaps.width);
    assertEquals(1, overlaps.height);
  }

  public void testOverlappingCellsShouldClip() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0,0.0,1024.0,1024.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 4, 6, 16,8,20,12);
    java.awt.Rectangle overlaps = new java.awt.Rectangle();

    // Case 1: No clipping required
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 256.0,128.0,320.0,192.0), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 2: Clip part on the top left
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 128.0,64.0,320.0,192.0), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 3: Clip part on the bottom right
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 256.0,128.0,512.0,256.0), overlaps);
    assertEquals(16, overlaps.x);
    assertEquals(8, overlaps.y);
    assertEquals(4, overlaps.width);
    assertEquals(4, overlaps.height);

    // Case 4: If completely outside, return an empty rectangle with non-positive width or height
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 128.0,64.0,196.0,96.0), overlaps);
    assertTrue(overlaps.width <= 0 || overlaps.height <= 0);
  }

  public void testSetFromTileID() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1024.0, 1024.0);
    SubPyramid subPyramid = new SubPyramid();
    subPyramid.set(mbr, 3, 1, 3);

    assertEquals(3, subPyramid.getMinimumLevel());
    assertEquals(3, subPyramid.getMaximumLevel());
    assertEquals(1, subPyramid.getC1());
    assertEquals(2, subPyramid.getC2());
    assertEquals(3, subPyramid.getR1());
    assertEquals(4, subPyramid.getR2());
  }

  public void testGetNumberOfTiles() {
    SubPyramid p = new SubPyramid(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0), 0, 0, 0, 0, 1, 1);
    assertEquals(1, p.getTotalNumberOfTiles());

    p = new SubPyramid(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0), 0, 2, 0, 0, 4, 4);
    assertEquals(21, p.getTotalNumberOfTiles());

    p = new SubPyramid(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0), 2, 3, 0, 0, 8, 8);
    assertEquals(80, p.getTotalNumberOfTiles());

    p = new SubPyramid(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0), 1, 2, 2, 2, 4, 4);
    assertEquals(5, p.getTotalNumberOfTiles());

    p = new SubPyramid(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0), 1, 2, 1, 2, 3, 4);
    assertEquals(6, p.getTotalNumberOfTiles());
  }

  public void testOverlappingCellsArbitraryLevels() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0,0.0,10.0,10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 2);

    java.awt.Rectangle overlaps = new java.awt.Rectangle();
    subPyramid.getOverlappingTiles(new EnvelopeNDLite(2, 5.0,5.0,10.0,10.0), overlaps, 0);
    assertEquals(new java.awt.Rectangle(0, 0, 1, 1), overlaps);
  }

  public void testTileSize() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 10.0, 10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 2);
    assertEquals(10.0, subPyramid.getTileWidth(0));
    assertEquals(5.0, subPyramid.getTileWidth(1));
    assertEquals(2.5, subPyramid.getTileWidth(2));
  }

  public void testSetMaximumLevel() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 10.0, 10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 2);
    subPyramid.setMaximumLevel(3);
    assertEquals(8, subPyramid.getR2());
    assertEquals(8, subPyramid.getC2());

    subPyramid = new SubPyramid(mbr, 0, 2, 1, 2, 2, 3);
    subPyramid.setMaximumLevel(3);
    assertEquals(2, subPyramid.getC1());
    assertEquals(4, subPyramid.getC2());
    assertEquals(4, subPyramid.getR1());
    assertEquals(6, subPyramid.getR2());

    subPyramid = new SubPyramid(mbr, 0, 2, 1, 2, 2, 3);
    subPyramid.setMaximumLevel(1);
    assertEquals(0, subPyramid.getC1());
    assertEquals(1, subPyramid.getC2());
    assertEquals(1, subPyramid.getR1());
    assertEquals(2, subPyramid.getR2());
  }

  public void testIterationFullPyramid() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 10.0, 10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 2);
    List<Long> tiles = new ArrayList<>();
    for (long tileID : subPyramid) {
      tiles.add(tileID);
    }
    tiles.sort(null);
    assertArrayEqualsGeneric(new Long[] {
            TileIndex.encode(0, 0, 0),
            TileIndex.encode(1, 0, 0),
            TileIndex.encode(1, 0, 1),
            TileIndex.encode(1, 1, 0),
            TileIndex.encode(1, 1, 1),
            TileIndex.encode(2, 0, 0),
            TileIndex.encode(2, 0, 1),
            TileIndex.encode(2, 0, 2),
            TileIndex.encode(2, 0, 3),
            TileIndex.encode(2, 1, 0),
            TileIndex.encode(2, 1, 1),
            TileIndex.encode(2, 1, 2),
            TileIndex.encode(2, 1, 3),
            TileIndex.encode(2, 2, 0),
            TileIndex.encode(2, 2, 1),
            TileIndex.encode(2, 2, 2),
            TileIndex.encode(2, 2, 3),
            TileIndex.encode(2, 3, 0),
            TileIndex.encode(2, 3, 1),
            TileIndex.encode(2, 3, 2),
            TileIndex.encode(2, 3, 3),
    }, tiles.toArray(new Long[0]));
  }


  public void testIterationPartialPyramid() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 10.0, 10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 2, 3, 6,2,8,4);
    List<Long> tiles = new ArrayList<>();
    for (long tileID : subPyramid) {
      tiles.add(tileID);
    }
    tiles.sort(null);
    assertArrayEqualsGeneric(new Long[] {
            TileIndex.encode(2, 3, 1),
            TileIndex.encode(3, 6, 2),
            TileIndex.encode(3, 6, 3),
            TileIndex.encode(3, 7, 2),
            TileIndex.encode(3, 7, 3),
    }, tiles.toArray(new Long[0]));
  }

  public void testSubPyramidIterator() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 10.0, 10.0);
    SubPyramid subPyramid = new SubPyramid(mbr, 0, 5);
    int count = 0;
    for (SubPyramid sb : subPyramid.getSubPyramids(3))
      count++;
    assertEquals(65, count);

    List<Long> expectedRootTiles = new ArrayList<>();
    for (int x = 0; x < 4; x++)
      for (int y = 0; y < 4; y++)
        expectedRootTiles.add(TileIndex.encode(2, x, y));
    expectedRootTiles.add(TileIndex.encode(0, 0, 0));
    for (SubPyramid sb : subPyramid.getSubPyramids(4)) {
      long rootTileID = sb.getRootTileID();
      assertTrue(String.format("Unexpected tile ID %d", rootTileID), expectedRootTiles.remove(rootTileID));
    }
    assertTrue("Some tile indexes were not found", expectedRootTiles.isEmpty());
  }
}
