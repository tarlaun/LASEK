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

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD$;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.synopses.HistogramOP;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.synopses.UniformHistogram;
import edu.ucr.cs.bdlab.beast.util.OperationMetadata;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@OperationMetadata(
    shortName =  "mindex",
    description = "Compute index metadata (master files) of the given dataset in different indexes",
    inputArity = "+",
    outputArity = "1")
public class ComputeIndexMetadata implements JCLIOperation {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(ComputeIndexMetadata.class);

  /**
   * The type of the global index (partitioner)
   */
  @OperationParam(
      description = "The types of the global indexes to consider separated with commas",
      required = true
  )
  public static final String GlobalIndexes = "gindexes";

  /**
   * Partitions the a dataset using a partitioner.
   * @param context the spark context
   * @param opts the user options
   * @param features the set of features
   * @param gindexes list of global indexes
   * @param disjoint set this parameter to create disjoint partitions with possibly some features replicated
   * @param partitionInfo information on how to calculate the number of partitions
   * @param synopsisSize the total size of the synopsis (sample or histogram) in bytes.
   * @return the list of partitioners, one for each of the given list of gindexes
   */
  public static SpatialPartitioner[] createPartitioners(JavaSparkContext context, BeastOptions opts,
                                                        JavaRDD<IFeature> features,
                                                        String[] gindexes, boolean disjoint,
                                                        String partitionInfo,
                                                        long synopsisSize) {
    SpatialPartitioner[] partitioners = new SpatialPartitioner[gindexes.length];

    // Retain only non-empty geometries. Empty geometries are not taken into account for creating the partitioner
    features = features.filter(f -> !f.getGeometry().isEmpty());

    // Since we compute two synopses (sample and histogram) we divide the allotted budget between them
    synopsisSize /= 2;

    // Compute a summary for the input data
    context.setJobGroup("Summary", "Compute the summary of the input (MBR)");
    Summary summary = Summary.computeForFeatures(features);

    // Compute a sample of the input
    context.setJobGroup("Sample", "Reading a sample of the input");
    int sampleSize = (int) (synopsisSize / (8 * summary.getCoordinateDimension()));
    double samplingRatio = (double) sampleSize / summary.numFeatures();
    JavaRDD<IFeature> sampleF = samplingRatio >= 1.0 ? features : features.sample(false, samplingRatio);
    int numDimensions = summary.getCoordinateDimension();
    List<PointND> sampleP = sampleF.map(f -> new PointND((f.getGeometry().getCentroid()))).collect();

    LOG.info(String.format("Picked a sample of %d points", sampleP.size()));
    // Convert to a column-oriented primitive 2D array for memory conservation and cache efficiency
    double[][] coords = new double[numDimensions][sampleP.size()];
    for (int d = 0; d < coords.length; d++)
      for (int i = 0; i < sampleP.size(); i++)
        coords[d][i] = sampleP.get(i).getCoordinate(d);

    context.setJobGroup("Histogram", "Compute the histogram of the input");
    int numBuckets = (int) (synopsisSize / 8);
    UniformHistogram histogram = HistogramOP.computePointHistogramTwoPass(features.rdd(), f->f.getStorageSize(), summary, numBuckets);

    for(int i = 0; i < gindexes.length; i++) {
      String gindex = gindexes[i];
      Class<? extends SpatialPartitioner> partitionerClass = IndexHelper.getPartitioners().get(gindex);
      try {
        // Instantiate the partitioner
        SpatialPartitioner partitioner = partitionerClass.newInstance();
        partitioner.setup(opts, disjoint);

        SpatialPartitioner.Metadata pMetadata = partitioner.getMetadata();
        if (disjoint && !pMetadata.disjointSupported())
          throw new RuntimeException("Partitioner " + partitionerClass.getName() + " does not support disjoint partitioning");

        long t1 = System.nanoTime();
        int numPartitions = IndexHelper.computeNumberOfPartitions(
            IndexHelper.parsePartitionCriterion(partitionInfo), summary
        );
        partitioner.construct(summary, coords, histogram, numPartitions);
        long t2 = System.nanoTime();
        LOG.info(String.format("Partitioner '%s' constructed in %f seconds", partitionerClass.getSimpleName(), (t2-t1)*1E-9));
        partitioners[i] = partitioner;
      } catch (InstantiationException e) {
        e.printStackTrace();
        LOG.warn("Could not create a partitioner. Returning null!");
        return null;
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        LOG.warn("Could not create a partitioner. Returning null!");
        return null;
      }
    }
    return partitioners;
  }

  @Override
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
    // Extract index parameters from the command line arguments
    boolean disjoint = opts.getBoolean(DisjointIndex, false);
    long synopsisSize = opts.getSizeAsBytes(SynopsisSize, 10L * 1024 * 1024);
    String criterionThreshold = opts.getString(PartitionCriterionThreshold, "Size(128m)");

    File file = null;
    try {
      file = new File(outputs[0]);
      boolean success = file.mkdir();
      if(success) {
        System.out.println("Create output successfully: " + outputs[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    // Start processing the input to build the index
    // Read the input features
    JavaRDD<IFeature> features = SpatialReader.readInput(sc, opts, inputs[0], opts.getString(SpatialFileRDD.InputFormat()));

    String[] gindexes = opts.getString(GlobalIndexes, "").split(",");

    // Create the partitioners out of the input
    sc.setJobGroup("Create partitioner", "Create a partitioner out of the input");
    SpatialPartitioner[] partitioners = createPartitioners(sc, opts, features, gindexes, disjoint, criterionThreshold, synopsisSize);

    long t1 = System.nanoTime();
    JavaPairRDD<Tuple2<Integer, Integer>, Summary> partitionSummaries = features.mapPartitionsToPair(fs -> {
      Map<Tuple2<Integer, Integer>, Summary> partialSummaries = new HashMap<>();
      EnvelopeNDLite mbr = new EnvelopeNDLite(2);
      while (fs.hasNext()) {
        IFeature f = fs.next();
        mbr.setEmpty();
        mbr.merge(f.getGeometry());
        for(int partitionerIndex = 0; partitionerIndex < partitioners.length; partitionerIndex++) {
          int partitionId = partitioners[partitionerIndex].overlapPartition(mbr);
          Summary partialSummary = partialSummaries.get(new Tuple2<>(partitionerIndex, partitionId));
          if(partialSummary == null) {
            partialSummary = new Summary();
            partialSummary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(f.getGeometry()));
            partialSummaries.put(new Tuple2<>(partitionerIndex, partitionId), partialSummary);
          }
          partialSummary.expandToFeature(f);
        }
      }

      List<Tuple2<Tuple2<Integer, Integer>, Summary>> partitionSummaryList = new ArrayList<>();
      for(Map.Entry<Tuple2<Integer, Integer>, Summary> entry: partialSummaries.entrySet()) {
        partitionSummaryList.add(new Tuple2<>(entry.getKey(), entry.getValue()));
      }
      return partitionSummaryList.iterator();
    });

    partitionSummaries = partitionSummaries.reduceByKey((s1, s2) -> {
      s1.expandToSummary(s2);
      return s1;
    });

    Map<Integer, Iterable<Tuple2<Integer, Summary>>> indexPartitionsMap = partitionSummaries.mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2))).groupByKey().collectAsMap();
    for(Map.Entry<Integer, Iterable<Tuple2<Integer, Summary>>> entry: indexPartitionsMap.entrySet()) {
      try {
        FileWriter fileWriter = new FileWriter(outputs[0] + "/_master." + gindexes[entry.getKey().intValue()]);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        Iterator<Tuple2<Integer, Summary>> iterator = entry.getValue().iterator();
        while (iterator.hasNext()) {
          Tuple2<Integer, Summary> t = iterator.next();
          printWriter.println(IndexHelper.getPartitionAsText(t._1, String.format("part-%05d", t._1), t._2));
        }
        printWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    long t2 = System.nanoTime();
    LOG.info(String.format("Compute master files in %f seconds", (t2-t1)*1E-9));
    return null;
  }

  /**Whether to build a disjoint index (with no overlapping partitions)*/
  @OperationParam(
      description = "Build a disjoint index with no overlaps between partitions",
      defaultValue = "false"
  )
  public static final String DisjointIndex = "disjoint";

  /**The size of the synopsis used to summarize the input before building the index*/
  @OperationParam(
      description = "The size of the synopsis used to summarize the input, e.g., 1024, 10m, 1g",
      defaultValue = "10m"
  )
  public static final String SynopsisSize = "synopsissize";

  /**The criterion used to calculate the number of partitions*/
  @OperationParam(
      description = "The criterion used to compute the number of partitions. It can be one of:\n" +
          "\tFixed(n): Create a fixed number of partitions (n partitions)\n" +
          "\tSize(s): Create n partitions such that each partition contains around s bytes\n" +
          "\tCount(c): Create n partitions such that each partition contains around c records",
      defaultValue = "Size(128m)"
  )
  public static final String PartitionCriterionThreshold = "pcriterion";

  public void printUsage(PrintStream out) {
    out.println("The available indexes are:");
    for (Map.Entry<String, Class<? extends SpatialPartitioner>> partitioner : IndexHelper.getPartitioners().entrySet()) {
      SpatialPartitioner.Metadata indexerMetadata = partitioner.getValue().getAnnotation(SpatialPartitioner.Metadata.class);
      out.printf("- %s: %s\n", partitioner.getKey(), indexerMetadata.description());
    }
  }
}
