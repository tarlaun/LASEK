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
package edu.ucr.cs.bdlab.beast.operations;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.indexing.GridPartitioner;
import edu.ucr.cs.bdlab.beast.indexing.RSGrovePartitioner;
import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class IndexTest extends JavaSparkTest {

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
      int numDimensions = (line.toString().split("\t").length - 7) / 3;
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

  public void testBuildIndex() throws IOException {
    Path inputPath = new Path(scratchPath(), "input");
    copyResource("/test.rect", new File(inputPath.toString()));
    Path outputPath = new Path(scratchPath(), "output.index");
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialFileRDD.InputFormat(), "pointk(4)");
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    opts.set(IndexHelper.PartitionCriterionThreshold(), "Fixed(4)");
    opts.set(IndexHelper.GlobalIndex(), "rsgrove");
    opts.set(IndexHelper.BalancedPartitioning(), false);
    Index.run(opts, new String[] {inputPath.toString()}, new String[] {outputPath.toString()}, sparkContext());
    FileSystem fileSystem = outputPath.getFileSystem(new Configuration());
    Path masterFilePath = new Path(outputPath, "_master.rsgrove");
    assertTrue("Master file does not exist", fileSystem.exists(masterFilePath));
    // Test the MBR of the global index
    IFeature[] partitions = readPartitions(fileSystem, masterFilePath);
    assertEquals(4, ((EnvelopeND)partitions[0].getGeometry()).getCoordinateDimension());
    EnvelopeNDLite env = new EnvelopeNDLite(4);
    for (IFeature p : partitions)
      env.merge(p.getGeometry());
    assertEquals(133.0, env.getMinCoord(0));
    assertEquals(954.0, env.getMaxCoord(0));
    assertEquals(51.0, env.getMinCoord(3));
    assertEquals(1000.0, env.getMaxCoord(3));
  }

  public void testOverwriteOutput() throws IOException {
    Path inputPath = new Path(scratchPath(), "input");
    copyResource("/test.rect", new File(inputPath.toString()));
    File outputPath = makeDirCopy("/test_index");
    BeastOptions opts = new BeastOptions(false)
        .set(SpatialWriter.OverwriteOutput(), true)
        .set(SpatialFileRDD.InputFormat(), "pointk(4)")
        .set(CSVFeatureReader.FieldSeparator, ",")
        .set(IndexHelper.PartitionCriterionThreshold(), "Fixed(4)")
        .set(IndexHelper.GlobalIndex(), "rsgrove")
        .set(IndexHelper.BalancedPartitioning(), false);
    Index.run(opts, new String[] {inputPath.toString()}, new String[] {outputPath.toString()}, sparkContext());
    FileSystem fileSystem = new Path(outputPath.getPath()).getFileSystem(new Configuration());
    Path masterFilePath = new Path(outputPath.getPath(), "_master.rsgrove");
    assertTrue("Master file does not exist", fileSystem.exists(masterFilePath));
    // Test the MBR of the global index
    IFeature[] partitions = readPartitions(fileSystem, masterFilePath);
    assertEquals(4, ((EnvelopeND)(partitions[0].getGeometry())).getCoordinateDimension());
    EnvelopeNDLite env = new EnvelopeNDLite(4);
    for (IFeature p : partitions)
      env.merge(p.getGeometry());
    assertEquals(133.0, env.getMinCoord(0));
    assertEquals(954.0, env.getMaxCoord(0));
    assertEquals(51.0, env.getMinCoord(3));
    assertEquals(1000.0, env.getMaxCoord(3));
  }


  public abstract static class AbstractPartitioner extends SpatialPartitioner {

    @Override
    public int overlapPartition(EnvelopeNDLite mbr) {
      return 0;
    }

    @Override
    public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {

    }

    @Override
    public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    }

    @Override
    public int numPartitions() {
      return 0;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
    }

    @Override
    public boolean isDisjoint() {
      return false;
    }

    @Override
    public int getCoordinateDimension() {
      return 0;
    }
  }

  @SpatialPartitioner.Metadata(
      disjointSupported = true,
      extension = "sum",
      description = "test"
  )
  public static class SummaryPartitioner extends AbstractPartitioner {

    private Summary summary;

    @Override
    public void construct(Summary summary, double[][] sample, AbstractHistogram histogram, int n) {
      this.summary = summary;
    }

    @Override
    public EnvelopeNDLite getEnvelope() {
      return null;
    }

  }
  @SpatialPartitioner.Metadata(
      disjointSupported = true,
      extension = "smp",
      description = "test"
  )
  public static class SamplePartitioner extends AbstractPartitioner {

    double[] xs, ys;

    @Override
    public void construct(Summary summary, @Required double[][] sample, AbstractHistogram histogram, int n) {
      this.xs = sample[0];
      this.ys = sample[1];
    }

    @Override
    public EnvelopeNDLite getEnvelope() {
      return null;
    }

  }

  public void testCreatePartitioner() {
    List<IFeature> features = Arrays.asList(
        Feature.create(null, new PointND(new GeometryFactory(), 2, 0.0, 1.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 2.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.0, 7.0))
    );
    JavaRDD<IFeature> rddFeatures = javaSparkContext().parallelize(features);
    BeastOptions opts = new BeastOptions(false);
    opts.set(IndexHelper.SynopsisSize(), 100);
    SpatialPartitioner partitioner = IndexHelper.createPartitioner(rddFeatures, SummaryPartitioner.class,
        "fixed", 3, IFeature::getStorageSize, opts);
    assertNotNull(partitioner);
    assertEquals(SummaryPartitioner.class, partitioner.getClass());
    assertNotNull(((SummaryPartitioner)partitioner).summary);

    opts.set(IndexHelper.SynopsisSize(), 100);
    partitioner = IndexHelper.createPartitioner(rddFeatures, SamplePartitioner.class,
        "fixed", 3, IFeature::getStorageSize, opts);
    assertNotNull(partitioner);
    assertEquals(SamplePartitioner.class, partitioner.getClass());
    assertNotNull(((SamplePartitioner)partitioner).xs);
    assertTrue("Sample points should be different",
        ((SamplePartitioner)partitioner).xs[0] != ((SamplePartitioner)partitioner).xs[1]);
  }

  public void testCreatePartitionerWithEmptyGeometries() {
    List<IFeature> features = Arrays.asList(
        Feature.create(null, EmptyGeometry.instance),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 3.0, 2.0)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.0, 7.0))
    );
    JavaRDD<IFeature> rddFeatures = javaSparkContext().parallelize(features);
    BeastOptions opts = new BeastOptions(false);
    opts.set(IndexHelper.SynopsisSize(), 100);
    SpatialPartitioner partitioner = IndexHelper.createPartitioner(rddFeatures, SummaryPartitioner.class,
        "fixed", 3, IFeature::getStorageSize, opts);
    assertNotNull(partitioner);
    assertEquals(SummaryPartitioner.class, partitioner.getClass());
    assertNotNull(((SummaryPartitioner)partitioner).summary);

    opts.set(IndexHelper.SynopsisSize(), 100);
    partitioner = IndexHelper.createPartitioner(rddFeatures, SamplePartitioner.class,
        "fixed", 3, IFeature::getStorageSize, opts);
    assertNotNull(partitioner);
    assertEquals(SamplePartitioner.class, partitioner.getClass());
    assertNotNull(((SamplePartitioner)partitioner).xs);
    assertTrue("Sample points should be different",
        ((SamplePartitioner)partitioner).xs[0] != ((SamplePartitioner)partitioner).xs[1]);
  }

  public void testPartitionFeatures() {
    GeometryFactory factory = new GeometryFactory();
    List<IFeature> features = Arrays.asList(
        new Feature(new Object[] {factory.createPoint(new CoordinateXY(0.0, 1.0)), "1"}, null),
        new Feature(new Object[] {factory.createPoint(new CoordinateXY(2.0, 2.0)), "2"}, null),
        new Feature(new Object[] {factory.createPoint(new CoordinateXY(1.5, 3.0)), "3"}, null),
        new Feature(new Object[] {factory.createPoint(new CoordinateXY(1.0, 1.5)), "4"}, null)
    );
    JavaRDD<IFeature> featuresRDD = javaSparkContext().parallelize(features);
    BeastOptions clo = new BeastOptions(false)
        .set(IndexHelper.DisjointIndex(), true)
        .set(IndexHelper.SynopsisSize(), 10);

    GridPartitioner partitioner = (GridPartitioner) IndexHelper.createPartitioner(featuresRDD, GridPartitioner.class,
        "fixed", 4, IFeature::getStorageSize, clo);
    assertEquals(2, partitioner.numPartitions[0]);
    assertEquals(2, partitioner.numPartitions[1]);
    JavaPairRDD<Integer, IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner)
        .mapPartitionsWithIndex((partitionID, fs) -> {
          List<Tuple2<Integer, IFeature>> ret = new ArrayList<>();
          while (fs.hasNext()) {
            IFeature f = fs.next();
            ret.add(new Tuple2<>(partitionID, f));
          }
          return ret.iterator();
        }, true)
        .mapToPair(f -> f);
    List<Tuple2<Integer, IFeature>> theData = new ArrayList(partitionedData.collect());
    // Sort by attribute value to ensure a consistent and expected order of the results
    theData.sort((f1, f2) -> f1._2.get(1).toString().compareTo(f2._2.get(1).toString()));
    assertEquals(features.size(), theData.size());
    assertEquals(0, (int) theData.get(0)._1);
    assertEquals(3, (int) theData.get(1)._1);
    assertEquals(3, (int) theData.get(2)._1);
    assertEquals(1, (int) theData.get(3)._1);

    assertEquals(features.get(0), theData.get(0)._2);
    assertEquals(features.get(1), theData.get(1)._2);
    assertEquals(features.get(2), theData.get(2)._2);
    assertEquals(features.get(3), theData.get(3)._2);
  }

  public void testPartitionWithReplication() {
    List<IFeature> features = Arrays.asList(
        Feature.create(null, new EnvelopeND(new GeometryFactory(), 2, 0.5, 0.5, 1.5, 1.5))
    );
    SpatialPartitioner p = new GridPartitioner(new EnvelopeNDLite(2, 0.0, 0.0, 2.0, 2.0), new int[] {2, 2});
    JavaRDD<IFeature> featuresRDD = javaSparkContext().parallelize(features);
    JavaRDD<IFeature> partitionedFeatures = IndexHelper.partitionFeatures2(featuresRDD, p);
    assertEquals(4, partitionedFeatures.count());
  }

  public void testWriteIndex() throws IOException {
    List<IFeature> features = Arrays.asList(
        Feature.create(null, new PointND(new GeometryFactory(), 2, 0, 1)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 2, 2)),
        Feature.create(null, new PointND(new GeometryFactory(), 2, 1.5, 3))
    );
    JavaRDD<IFeature> featuresRDD = javaSparkContext().parallelize(features);
    BeastOptions clo = new BeastOptions(false)
        .set(IndexHelper.DisjointIndex(), true)
        .set(IndexHelper.SynopsisSize(), 10);

    GridPartitioner partitioner = (GridPartitioner) IndexHelper.createPartitioner(featuresRDD, GridPartitioner.class,
        "fixed", 4, IFeature::getStorageSize, clo);
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner);
    Path indexPath = new Path(scratchPath(), "temp_index");
    // Set output format properties
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialWriter.OutputFormat(), "point(0,1)");
    clo.set("separator", "\t");
    IndexHelper.saveIndex2J(partitionedData, indexPath.toString(), opts);
    String masterFilePath = new Path(indexPath, "_master.grid").toString();
    assertTrue("Master file not found", new File(masterFilePath).exists());
    String[] masterFileInfo = readFile(masterFilePath);
    long totalCount = 0;
    long totalSize = 0;
    for (int i = 1; i < masterFileInfo.length; i++) {
      long count = Long.parseLong(masterFileInfo[i].split("\t")[2]);
      long size = Long.parseLong(masterFileInfo[i].split("\t")[3]);
      totalCount += count;
      totalSize += size;
    }
    assertEquals(3, totalCount);
  }

  public void testWriteIndexWithPolygons() throws IOException {
    List<String> features = Arrays.asList(
        "POLYGON((0 0, 1 1, 2 0, 0 0))",
        "POLYGON((3 5, 4 2, 2 2, 3 5))"
    );
    JavaRDD<String> textRDD = javaSparkContext().parallelize(features);
    JavaRDD<IFeature> featuresRDD = SpatialReader.parseWKT(textRDD, 0, '\t');

    BeastOptions clo = new BeastOptions(false);
    clo.set(IndexHelper.SynopsisSize(), 10);
    GridPartitioner partitioner = (GridPartitioner) IndexHelper.createPartitioner(featuresRDD, GridPartitioner.class,
        "fixed", 4, IFeature::getStorageSize, clo);
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner);
    Path indexPath = new Path(scratchPath(), "temp_index");
    // Set output format properties
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialFileRDD.InputFormat(), "wkt(0)");
    opts.set("separator", "\t");
    IndexHelper.saveIndex2J(partitionedData, indexPath.toString(), opts);
    String masterFilePath = new Path(indexPath, "_master.grid").toString();
    assertTrue("Master file not found", new File(masterFilePath).exists());
    String[] masterFileInfo = readFile(masterFilePath);
    long totalCount = 0;
    long totalSize = 0;
    for (int i = 1; i < masterFileInfo.length; i++) {
      long count = Long.parseLong(masterFileInfo[i].split("\t")[2]);
      long size = Long.parseLong(masterFileInfo[i].split("\t")[3]);
      totalCount += count;
      totalSize += size;
    }
    assertEquals(2, totalCount);
  }

  public void testWritePartitionedIndex() throws IOException {
    int numPoints = 10000;
    List<IFeature> points = new ArrayList<>();
    Random random = new Random(1);
    for (int i = 0; i < numPoints; i++)
      points.add(Feature.create(null, new PointND(new GeometryFactory(), 2, random.nextDouble(), random.nextDouble())));

    JavaRDD<IFeature> featuresRDD = javaSparkContext().parallelize(points, 10);

    BeastOptions clo = new BeastOptions(false).set(IndexHelper.SynopsisSize(), 10);
    GridPartitioner partitioner = (GridPartitioner) IndexHelper.createPartitioner(featuresRDD, GridPartitioner.class,
        "fixed", 4, IFeature::getStorageSize, new BeastOptions(false));
    assertEquals(4, partitioner.numPartitions());
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner);
    Path indexPath = new Path(scratchPath(), "temp_index");
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialWriter.OutputFormat(), "point(0,1)");
    opts.set(CSVFeatureReader.FieldSeparator, "\t");
    IndexHelper.saveIndex2J(partitionedData, indexPath.toString(), opts);
    File masterFile = new File(new Path(indexPath, "_master.grid").toString());
    assertTrue("Master file not found", masterFile.exists());
    String[] lines = readFile(masterFile.toString());
    assertEquals(5, lines.length);
  }

  public void testWriteIndexWithFourDimensions() throws IOException {
    Path inputFile = new Path(scratchPath(), "test.rect");
    copyResource("/test.rect", new File(inputFile.toString()));
    BeastOptions opts = new BeastOptions(false);
    opts.set(SpatialFileRDD.InputFormat(), "pointk(4)");
    opts.set(SpatialWriter.OutputFormat(), "pointk(4)");
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    opts.set(CSVFeatureReader.SkipHeader, "false");
    JavaRDD<IFeature> featuresRDD = SpatialReader.readInput(javaSparkContext(), opts, inputFile.toString(),
        opts.getString(SpatialFileRDD.InputFormat()));
    SpatialPartitioner partitioner = IndexHelper.createPartitioner(featuresRDD, GridPartitioner.class,
        "fixed", 16, IFeature::getStorageSize, new BeastOptions(false).set(IndexHelper.SynopsisSize(), 1000));
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner);
    Path indexPath = new Path(scratchPath(), "temp_index");
    // Write the index to the output
    IndexHelper.saveIndex2J(partitionedData, indexPath.toString(), opts);
    String masterFilePath = new Path(indexPath, "_master.grid").toString();
    assertTrue("Master file not found", new File(masterFilePath).exists());
    String[] masterFileInfo = readFile(masterFilePath);
    long totalCount = 0;
    long totalSize = 0;
    for (int i = 1; i < masterFileInfo.length; i++) {
      String[] parts = masterFileInfo[i].split("\t");
      assertEquals(7 + 4 * 3, parts.length);
      long count = Long.parseLong(parts[2]);
      long size = Long.parseLong(parts[3]);
      totalCount += count;
      totalSize += size;
      for (int x = parts.length - 8; x < parts.length; x++)
        assertTrue("Invalid coordinate!", Double.isFinite(Double.parseDouble(parts[x])));
    }
    assertEquals(14, totalCount);
  }

  public void testParsePartitioningCriterion() {
    IndexHelper.NumPartitions result = IndexHelper.parsePartitionCriterion("Size(128m)");
    assertEquals(IndexHelper.Size$.MODULE$, result.pc());
    assertEquals(128L * 1024 * 1024, result.value());
  }

  public void testWriteRSGroveIndexWithFourDimensions() {
    Path inputFile = new Path(scratchPath(), "test.rect");
    copyResource("/test.rect", new File(inputFile.toString()));

    BeastOptions opts = new BeastOptions(false).set("iformat", "pointk(4)")
        .set("separator", ",");
    JavaRDD<IFeature> featuresRDD = SpatialReader.readInput(javaSparkContext(), opts, inputFile.toString(),
        opts.getString(SpatialFileRDD.InputFormat()));
    SpatialPartitioner partitioner = IndexHelper.createPartitioner(featuresRDD, RSGrovePartitioner.class,
        "fixed", 16, IFeature::getStorageSize, new BeastOptions(false)
            .set(IndexHelper.SynopsisSize(), 1000)
            .set(IndexHelper.BalancedPartitioning(), false));
    JavaRDD<IFeature> partitionedData = IndexHelper.partitionFeatures2(featuresRDD, partitioner);
    Path indexPath = new Path(scratchPath(), "temp_index");
    // Write the index to the output
    IndexHelper.saveIndex2J(partitionedData, indexPath.toString(), opts);
    String masterFilePath = new Path(indexPath, "_master.rsgrove").toString();
    assertTrue("Master file not found", new File(masterFilePath).exists());
    String[] masterFileInfo = readFile(masterFilePath);
    long totalCount = 0;
    long totalSize = 0;
    for (int i = 1; i < masterFileInfo.length; i++) {
      String[] parts = masterFileInfo[i].split("\t");
      assertEquals(7 + 4 * 3, parts.length);
      long count = Long.parseLong(parts[2]);
      long size = Long.parseLong(parts[3]);
      totalCount += count;
      totalSize += size;
      for (int x = parts.length - 8; x < parts.length; x++)
        assertTrue("Invalid coordinate!", Double.isFinite(Double.parseDouble(parts[x])));
    }
    assertEquals(14, totalCount);
  }

  
  public void testEndToEndIndexing() throws IOException {
    if (!shouldRunStressTest())
      return;
    String[] allPartitioners = {"rsgrove"};
    Path inPath = new Path(scratchPath(), "test.points");
    copyResource("/test111.points", new File(inPath.toString()));
    FileSystem fs = scratchPath().getFileSystem(new Configuration());
    for (String partitioner : allPartitioners) {
      Path outPath = new Path(scratchPath(), "test.index");
      BeastOptions clo = new BeastOptions(false);
      fs.delete(outPath, true);
      clo.set(SpatialFileRDD.InputFormat(), "pointk(2)");
      clo.set(SpatialWriter.OutputFormat(), "pointk(2)");
      clo.set(CSVFeatureReader.FieldSeparator, ",");
      clo.set(IndexHelper.GlobalIndex(), partitioner);
      clo.set(IndexHelper.PartitionCriterionThreshold(), "Fixed(10)");
      clo.set(RSGrovePartitioner.MMRatio, "0.3");
      Index.run(clo, new String[] {inPath.toString()}, new String[] {outPath.toString()}, sparkContext());

      FileSystem fileSystem = outPath.getFileSystem(new Configuration());
      Path masterFilePath = new Path(outPath, "_master."+partitioner);
      assertTrue(String.format("Master file '%s' does not exist", masterFilePath), fileSystem.exists(masterFilePath));
      // Test the MBR of the global index
      IFeature[] partitions = readPartitions(fileSystem, masterFilePath);
      assertEquals(2, GeometryHelper.getCoordinateDimension(partitions[0].getGeometry()));
      EnvelopeNDLite env = new EnvelopeNDLite(2);
      for (IFeature p : partitions)
        env.merge(p.getGeometry());
      assertEquals(-122.4850614, env.getMinCoord(0));
      assertEquals(139.63086943, env.getMaxCoord(0));
      assertEquals(-36.5311761, env.getMinCoord(1));
      assertEquals(61.9750933, env.getMaxCoord(1));
    }
  }

  public void testEndToEndWithAllIndexersWithTenPartition() throws IOException {
    if (!shouldRunStressTest())
      return;
    Set<String> allPartitioners = IndexHelper.getPartitioners().keySet();
    Path inPath = new Path(scratchPath(), "test.points");
    copyResource("/test111.points", new File(inPath.toString()));

    FileSystem fs = scratchPath().getFileSystem(new Configuration());
    for (String partitioner : allPartitioners) {
      try {
        Path outPath = new Path(scratchPath(), "test.index");
        BeastOptions opts = new BeastOptions(false);
        fs.delete(outPath, true);
        opts.set(SpatialFileRDD.InputFormat(), "pointk(2)");
        opts.set(SpatialWriter.OutputFormat(), "pointk(2)");
        opts.set(CSVFeatureReader.FieldSeparator, ",");
        opts.set(IndexHelper.GlobalIndex(), partitioner);
        opts.set(IndexHelper.PartitionCriterionThreshold(), "Fixed(10)");
        opts.set(RSGrovePartitioner.MMRatio, "0.3");
        Index.run(opts, new String[] {inPath.toString()}, new String[] {outPath.toString()}, sparkContext());

        FileSystem fileSystem = outPath.getFileSystem(new Configuration());
        Path masterFilePath = SpatialFileRDD.getMasterFilePath(fileSystem, outPath);
        assertTrue(String.format("Master file '%s' does not exist", masterFilePath), fileSystem.exists(masterFilePath));
        // Test the MBR of the global index
        IFeature[] partitions = readPartitions(fileSystem, masterFilePath);
        assertEquals(2, GeometryHelper.getCoordinateDimension(partitions[0].getGeometry()));
        EnvelopeNDLite env = new EnvelopeNDLite(2);
        for (IFeature p : partitions)
          env.merge(p.getGeometry());
        assertEquals(-122.4850614, env.getMinCoord(0));
        assertEquals(139.63086943, env.getMaxCoord(0));
        assertEquals(-36.5311761, env.getMinCoord(1));
        assertEquals(61.9750933, env.getMaxCoord(1));
      } catch (Exception e) {
        throw new RuntimeException(String.format("Error with the index '%s'", partitioner), e);
      }
    }
  }
}