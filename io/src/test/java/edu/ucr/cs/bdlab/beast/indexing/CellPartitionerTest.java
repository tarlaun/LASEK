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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CellPartitionerTest extends JavaSpatialSparkTest {

  /**
   * Read partition information from a master file as an array of partitions. The master file is a TSV
   * (tab-separated values) with a header line
   * {@link edu.ucr.cs.bdlab.beast.indexing.IndexHelper#printMasterFileHeader(int, PrintStream)}
   * and one line per partition. This method is supposed
   * to be called only when preparing a job and the master file is supposed to be of a decent size. Therefore, this
   * method does not optimize much for the object instantiation or memory overhead.
   * @param fs the file system that contains the file
   * @param file the path to the file
   * @return the list of partitions inside the given file
   * @throws IOException if an error happens while reading the file
   */
  public static IFeature[] readPartitions(FileSystem fs, Path file) throws IOException {
    List<IFeature> partitions = new ArrayList<>();

    LineReader reader = null;
    try {
      reader = new LineReader(fs.open(file));
      Text line = new Text();
      // Read header
      if (reader.readLine(line) == 0)
        throw new RuntimeException("Header not found!");
      // Determine number of dimensions
      String[] parts = line.toString().split("\t");
      int ixmin = 0;
      while (ixmin < parts.length && !parts[ixmin].equals("xmin"))
        ixmin++;
      int numDimensions = (parts.length - ixmin) / 2;
      reader.close();
      reader = null;
      String iformat = String.format("envelopek(%d,xmin)", numDimensions);
      Iterator<IFeature> features = SpatialFileRDD.readLocalJ(file.toString(), iformat,
          new BeastOptions().set(CSVFeatureReader.SkipHeader, true), fs.getConf());
      while (features.hasNext())
        partitions.add(features.next());
      return partitions.toArray(new IFeature[0]);
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  public void testOverlapPartition() throws IOException {
    String[] masterFile = readTextResource("/test.partitions");
    Path masterPath = new Path(scratchPath(), "tempindex/_master.grid");
    Configuration conf = new Configuration();
    FileSystem fs = masterPath.getFileSystem(conf);
    PrintStream printStream = new PrintStream(fs.create(masterPath));
    for (String l : masterFile)
      printStream.println(l);
    printStream.close();
    IFeature[] cells = readPartitions(masterPath.getFileSystem(conf), masterPath);
    EnvelopeNDLite[] cells2 = new EnvelopeNDLite[cells.length];
    for (int i = 0; i < cells.length; i++) {
      cells2[i] = new EnvelopeNDLite().merge(cells[i].getGeometry());
    }
    CellPartitioner p = new CellPartitioner(cells2);
    int cellId = p.overlapPartition(new EnvelopeND(new GeometryFactory(), 2, -124.7306754,40.4658126,-124.7306754,40.4658126));
    assertEquals("42", cells[cellId].getAs("ID"));
    cellId = p.overlapPartition(new EnvelopeND(new GeometryFactory(), 2, 10.0, 46.0, 10.0, 46.0));
    assertEquals("10", cells[cellId].getAs("ID"));
  }

  public void testOverlapPartitions() {
    EnvelopeNDLite[] cells = new EnvelopeNDLite[3];
    cells[0] = new EnvelopeNDLite(2, 0, 0, 1, 1);
    cells[1] = new EnvelopeNDLite(2, 1, 1, 2, 2);
    cells[2] = new EnvelopeNDLite(2, 1, 0, 2, 1);
    SpatialPartitioner partitioner = new CellPartitioner(cells);
    IntArray matchedPartitions = new IntArray();
    partitioner.overlapPartitions(new EnvelopeNDLite(2, 0, 0, 2, 1), matchedPartitions);
    assertEquals(2, matchedPartitions.size());
    matchedPartitions.sort();
    assertEquals(0, matchedPartitions.get(0));
    assertEquals(2, matchedPartitions.get(1));
  }

  public void testOverlapPartitionsWithPoints() {
    EnvelopeNDLite[] cells = new EnvelopeNDLite[1];
    cells[0] = new EnvelopeNDLite(2, 0, 0, 1, 1);
    SpatialPartitioner partitioner = new CellPartitioner(cells);
    IntArray matchedPartitions = new IntArray();
    partitioner.overlapPartitions(new EnvelopeNDLite(2, 0, 0, 0, 0), matchedPartitions);
    assertEquals(1, matchedPartitions.size());
    assertEquals(0, matchedPartitions.get(0));
  }

  public void testIsDisjoint() {
    EnvelopeNDLite[] cells = new EnvelopeNDLite[3];
    cells[0] = new EnvelopeNDLite(2, 0, 0, 1, 1);
    cells[1] = new EnvelopeNDLite(2, 1, 1, 2, 2);
    cells[2] = new EnvelopeNDLite(2, 1, 0, 2, 1);
    SpatialPartitioner partitioner = new CellPartitioner(cells);
    assertTrue("Should be disjoint", partitioner.isDisjoint());
    cells[0] =new EnvelopeNDLite(2, 0, 0, 1, 1);
    cells[1] =new EnvelopeNDLite(2, 1, 1, 2, 2);
    cells[2] =new EnvelopeNDLite(2, 0.5, 0.5, 1.5, 1.5);
    partitioner = new CellPartitioner(cells);
    assertFalse("Should not be disjoint", partitioner.isDisjoint());
  }
}