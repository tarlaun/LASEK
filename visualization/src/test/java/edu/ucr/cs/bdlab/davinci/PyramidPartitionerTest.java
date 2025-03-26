package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.synopses.Prefix2DHistogram;
import edu.ucr.cs.bdlab.beast.synopses.UniformHistogram;
import edu.ucr.cs.bdlab.beast.util.LongArray;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class PyramidPartitionerTest extends TestCase {

  public void testPartitionWithoutHistogram() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    SubPyramid pyramid = new SubPyramid(mbr, 1, 2, 1, 2, 3, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid);
    assertEquals(pyramid.getTotalNumberOfTiles(), pp.getPartitionCount());
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.1, 1.4, 1.4));
    List<TileIndex> expectedTiles = new ArrayList<>();
    expectedTiles.add(new TileIndex(2,2,2));
    expectedTiles.add(new TileIndex(1,1,1));
    assertEquals(expectedTiles.size(), matchedPartitions.length);
    for (long tileID : matchedPartitions) {
      TileIndex ti = new TileIndex();
      TileIndex.decode(tileID, ti);
      assertTrue(String.format("Matched tile %s is not in the list of expected tiles", ti), expectedTiles.remove(ti));
    }
    assertTrue(String.format("There are still %d unmatched tiles: %s", expectedTiles.size(), expectedTiles.toString()), expectedTiles.isEmpty());
  }

  public void testShouldClearMatchingPartitions() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    SubPyramid pyramid = new SubPyramid(mbr, 1, 2, 1, 2, 3, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid);
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.1, 1.4, 1.4));
    assertEquals(2, matchedPartitions.length);
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.1, 1.4, 1.4));
    assertEquals(2, matchedPartitions.length);
  }

  public void testPartitionDataTilesWithHistogram() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {2, 2}, 5);
    h.addEntry(new int[] {3, 2}, 5);
    SubPyramid pyramid = new SubPyramid(mbr, 1, 2, 1, 2, 3, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, new Prefix2DHistogram(h), 7, MultilevelPyramidPlotHelper.TileClass.DataTile);
    assertEquals(pyramid.getTotalNumberOfTiles(), pp.getPartitionCount());
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.1, 1.4, 1.4));
    List<TileIndex> expectedTiles = new ArrayList<>();
    expectedTiles.add(new TileIndex(2,2,2));
    assertEquals(expectedTiles.size(), matchedPartitions.length);
    for (long tileID : matchedPartitions) {
      TileIndex ti = new TileIndex();
      TileIndex.decode(tileID, ti);
      assertTrue(String.format("Matched tile %s is not in the list of expected tiles", ti), expectedTiles.remove(ti));
    }
    assertTrue(String.format("There are still %d unmatched tiles: %s", expectedTiles.size(), expectedTiles.toString()), expectedTiles.isEmpty());
  }

  public void testPartitionImageTilesWithHistogram() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {2, 2}, 5);
    h.addEntry(new int[] {3, 2}, 5);
    SubPyramid pyramid = new SubPyramid(mbr, 1, 2, 1, 2, 3, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, new Prefix2DHistogram(h), 7, MultilevelPyramidPlotHelper.TileClass.ImageTile);
    assertEquals(pyramid.getTotalNumberOfTiles(), pp.getPartitionCount());
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.1, 1.4, 1.4));
    List<TileIndex> expectedTiles = new ArrayList<>();
    expectedTiles.add(new TileIndex(1,1,1));
    assertEquals(expectedTiles.size(), matchedPartitions.length);
    for (long tileID : matchedPartitions) {
      TileIndex ti = new TileIndex();
      TileIndex.decode(tileID, ti);
      assertTrue(String.format("Matched tile %s is not in the list of expected tiles", ti), expectedTiles.remove(ti));
    }
    assertTrue(String.format("There are still %d unmatched tiles: %s", expectedTiles.size(), expectedTiles.toString()), expectedTiles.isEmpty());
  }

  public void testSizePartitioningMode() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {2, 2}, 5);
    h.addEntry(new int[] {3, 2}, 5);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2, 0, 0, 4, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, new Prefix2DHistogram(h), 7, 15);
    long[] matchedPartitions=pp.overlapPartitions(new EnvelopeNDLite(2, 1.1, 1.2, 1.9, 1.3));
    List<TileIndex> expectedTiles = new ArrayList<>();
    expectedTiles.add(new TileIndex(1,1,1));
    expectedTiles.add(new TileIndex(0,0,0));
    assertEquals(expectedTiles.size(), matchedPartitions.length);
    for (long tileID : matchedPartitions) {
      TileIndex ti = new TileIndex();
      TileIndex.decode(tileID, ti);
      assertTrue(String.format("Matched tile %s is not in the list of expected tiles", ti), expectedTiles.remove(ti));
    }
    assertTrue(String.format("There are still %d unmatched tiles: %s", expectedTiles.size(), expectedTiles.toString()), expectedTiles.isEmpty());
  }

  public void testSizePartitioningModeWithEmptyEnvelopes() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {2, 2}, 5);
    h.addEntry(new int[] {3, 2}, 5);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2, 0, 0, 4, 4);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, new Prefix2DHistogram(h), 7, 15);
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
    assertEquals(0, matchedPartitions.length);
  }

  public void testFullPartitionWithGranularity() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 10, 0, 0, 1 << 10, 1 << 10);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid);
    pp.setGranularity(3);
    assertEquals(1 + 16 +1024 + 65536, pp.getPartitionCount());

    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0 / 1024.0, 1.0 / 1024.0));
    assertEquals(4, matchedPartitions.length);
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0 / 512.0, 1.0 / 512.0));
    assertEquals(4, matchedPartitions.length);
  }

  public void testSizeBasedPartitioningWithGranularity() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2, 0, 0, 1 << 2, 1 << 2);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {1, 0}, 100);
    h.addEntry(new int[] {0, 1}, 100);
    h.addEntry(new int[] {2, 0}, 100);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, h, 50, 150);
    // This will only assign to the top level
    pp.setGranularity(3);
    assertEquals(1, pp.getPartitionCount());
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0 / 1024.0, 1.0 / 1024.0));
    assertEquals(1, matchedPartitions.length);

    // Reduce the granularity to 2 to test levels 0 and 1
    pp.setGranularity(2);
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(2, matchedPartitions.length);

    pp.minThreshold = 150;
    pp.maxThreshold = 250;
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(1, matchedPartitions.length);

    pp.minThreshold = 250;
    pp.maxThreshold = 350;
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(1, matchedPartitions.length);
  }

  public void testTileClassBasedPartitioningWithGranularity() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2, 0, 0, 1 << 2, 1 << 2);
    UniformHistogram h = new UniformHistogram(mbr, 4, 4);
    h.addEntry(new int[] {1, 0}, 100);
    h.addEntry(new int[] {0, 1}, 100);
    h.addEntry(new int[] {2, 0}, 100);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid, h, 150, MultilevelPyramidPlotHelper.TileClass.ImageTile);
    // This will only assign to the top level
    pp.setGranularity(3);
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(1, matchedPartitions.length);

    // Reduce the granularity to 2 to test levels 0 and 1
    pp.setGranularity(2);
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(2, matchedPartitions.length);

    pp.tileToConsider = MultilevelPyramidPlotHelper.TileClass.DataTile;
    matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 0.0, 0.0, 1.0, 1.0));
    assertEquals(2, matchedPartitions.length);
  }

  public void testPartitionWithBuffer() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1024.0, 1024.0);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid);
    // Set buffer size to approximately one pixel assuming a 256x256 tile
    pp.setBuffer(1 / 256.0);
    long[] matchedPartitions = pp.overlapPartitions(new EnvelopeNDLite(2, 257.0, 1.0, 511.0, 255.0));
    List<TileIndex> expectedTiles = new ArrayList<>();
    expectedTiles.add(new TileIndex(2,1,0));
    expectedTiles.add(new TileIndex(1,0,0));
    expectedTiles.add(new TileIndex(1,1,0));
    expectedTiles.add(new TileIndex(0,0,0));
    assertEquals(expectedTiles.size(), matchedPartitions.length);
    for (long tileID : matchedPartitions) {
      TileIndex ti = new TileIndex();
      TileIndex.decode(tileID, ti);
      assertTrue(String.format("Matched tile %s is not in the list of expected tiles", ti), expectedTiles.remove(ti));
    }
    assertTrue(String.format("There are still %d unmatched tiles: %s", expectedTiles.size(), expectedTiles.toString()), expectedTiles.isEmpty());
  }

  public void testMaintainBufferSizeAndGranularitySubPartitioner() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 1024.0, 1024.0);
    SubPyramid pyramid = new SubPyramid(mbr, 0, 2);
    PyramidPartitioner pp = new PyramidPartitioner(pyramid);
    pp.setBuffer(1.0);
    pp.setGranularity(5);
    PyramidPartitioner pp2 = new PyramidPartitioner(pp, pyramid);
    assertEquals(1.0, pp2.buffer, 1E-3);
    assertEquals(5, pp2.granularity);
  }
}