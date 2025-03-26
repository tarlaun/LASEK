package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AbstractRTreeBBPartitionerTest extends JavaSpatialSparkTest {

  public void testConstruct() throws IOException {
    double[][] coords = readCoordsResource("/test.points");
    AbstractRTreeBBPartitioner p = new AbstractRTreeBBPartitioner.RTreeGuttmanBBPartitioner();
    p.setup(new BeastOptions(false), false);
    Summary summary = new Summary();
    summary.set(new double[] {1.0, 0.0}, new double[] {12.0, 12.0});
    summary.setNumFeatures(11);
    summary.setSize(11 * 2 * 8);
    p.construct(summary, coords, null, 4);
    assertTrue("Too few partitions", p.numPartitions() > 2);
    Set<Integer> partitions = new HashSet();
    EnvelopeND mbr = new EnvelopeND(new GeometryFactory(), 2);
    for (int iPoint = 0; iPoint < coords[0].length; iPoint++) {
      for (int d = 0; d < coords.length; d++) {
        mbr.setMinCoord(d, coords[d][iPoint]);
        mbr.setMaxCoord(d, coords[d][iPoint]);
      }
      partitions.add(p.overlapPartition(mbr));
    }
    assertEquals(p.numPartitions(), partitions.size());
  }

  public void testOverlapPartitionShouldChooseMinimalArea() {
    EnvelopeND[] partitions = { new EnvelopeND(new GeometryFactory(), 2, 0.0,0.0,4.0,4.0),
        new EnvelopeND(new GeometryFactory(), 2, 1.0,1.0,3.0,3.0)};
    AbstractRTreeBBPartitioner p = new AbstractRTreeBBPartitioner.RTreeGuttmanBBPartitioner();
    initializeBBPartitioner(p, partitions);
    assertEquals(0, p.overlapPartition(new EnvelopeND(new GeometryFactory(), 2, 0.5, 0.5, 0.5, 0.5)));
    assertEquals(1, p.overlapPartition(new EnvelopeND(new GeometryFactory(), 2, 2.0, 2.0, 2.0, 2.0)));
  }

  private void initializeBBPartitioner(AbstractRTreeBBPartitioner p, EnvelopeND[] partitions) {
    int dimensions = partitions[0].getCoordinateDimension();
    int envelopes = partitions.length;
    p.minCoord = new double[dimensions][envelopes];
    p.maxCoord = new double[dimensions][envelopes];
    for (int i = 0; i < partitions.length; i++) {
      for (int d = 0; d < dimensions; d++) {
        p.minCoord[d][i] = partitions[i].getMinCoord(d);
        p.maxCoord[d][i] = partitions[i].getMaxCoord(d);
      }
    }
  }
}