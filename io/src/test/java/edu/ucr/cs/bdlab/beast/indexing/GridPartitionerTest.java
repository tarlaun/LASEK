package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import junit.framework.TestCase;
import org.locationtech.jts.geom.GeometryFactory;

public class GridPartitionerTest extends TestCase {
  public void testOutOfBounds() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 0.9, 0.9);
    GridPartitioner g = new GridPartitioner(mbr, new int[] {92, 97});
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), 2, 0.91, 0.91, 0.92, 0.92);
    IntArray overlappingResults = new IntArray();
    g.overlapPartitions(e, overlappingResults);
    assertEquals(0, overlappingResults.size());
  }

  public void testWithEmptyBox() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2);
    GridPartitioner g = new GridPartitioner(mbr, 1);
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), 2, 0.91, 0.91, 0.92, 0.92);
    IntArray overlappingResults = new IntArray();
    g.overlapPartitions(e, overlappingResults);
    assertEquals(0, overlappingResults.size());
  }

  public void testMBRLastPartition() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.005881941522663525, 0.014892128524887638,
        0.9992193245362517, 0.9947320023919717);
    GridPartitioner g = new GridPartitioner(mbr, new int[] {14, 14});
    EnvelopeNDLite partitionMBR = g.getPartitionMBR(13);
    assertEquals(0.9992193245362517, partitionMBR.getMaxCoord(0));
  }

  public void testMBRFirstPartition() {
    EnvelopeNDLite gridMBR = new EnvelopeNDLite(2, 0.0, 0.05, 0.9782302717552865, 0.9672899988517042);
    GridPartitioner g = new GridPartitioner(gridMBR, new int[] {43, 41});
    EnvelopeNDLite partitionMBR = g.getPartitionMBR(24);
    assertEquals(0.05, partitionMBR.getMinCoord(1));
  }
}