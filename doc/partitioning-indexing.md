# Spatial Partitioning
Whether you explicitly do it or not, Spark needs to partition your data to enable parallel processing.
By default, Spark uses a non-spatial partitioning based on the input file size.
That is, 128 MB per partition. This is not always the most efficient way to partition spatial data.
Beast provides several alternatives for spatial data partitioning.
This page explains how to use spatial partitioning and how it can speed up some calculations using spatial partitioning.

## How to partition spatial data
In Beast, the easiest way to partition spatial data is using the `spatialPartition` method as shown below.
```scala
val partitionedFeatures: RDD[IFeature] = features.spatialPartition(classOf[RSGrovePartitioner])
```
In the resulting RDD, the key is the partition number and the value is the feature itself. All features with the same key will always be in the same Spark partition, i.e., the same machine. Beast supports the following partitioners instead of the RSGrovePartitioner.

- *GridPartitioner*: Partitions the features using a uniform grid.
- *ZCurvePartitioner*: Uses the space filling curve, Z-Curve, to sort the records and partition them such that each partition contains roughly the same number of features.
- *HCurvePartitioner*: Similar to the ZCurvePartitioner but uses the Hilbert curve instead.
- *KDTreePartitioner*: Partitions the data by recursively splitting the input space along each dimension along the median point.
- *STRPartitioner*: Uses the Sort-Tile-Recursive (STR) method to partition the space such that each partition contains roughly the same number of records.
- *RGRovePartitioner*, *RSGrovePartitioner*, and *RRSGrovePartitioner*: Gray-box partitioners based on the R-tree, R*-tree, and RR*-tree, as described in the [R-Grove paper](https://www.cs.ucr.edu/~eldawy/publications/18-SIGSPATIAL-RGrove.pdf) and [article](http://aseldawy.blogspot.com/2018/11/in-big-data-forest-we-grow-groves-not.html).
- *RTreeGuttmanBBPartitioner* and *RStarTreeBBPartitioner*: Black-box partitioners based on the R-tree and the R*-tree as described in the [R-Grove paper](https://www.cs.ucr.edu/~eldawy/publications/18-SIGSPATIAL-RGrove.pdf) and [article](http://aseldawy.blogspot.com/2018/11/in-big-data-forest-we-grow-groves-not.html).

The recommended partitioner to use is the R*-Grove partitioner which is enhanced to provide better load balancing as describe in the [R*-Grove paper](https://www.cs.ucr.edu/~eldawy/publications/20_rsgrove.pdf).

## How to use partitioned data
Once the data is partitioned, you can process the data more efficiently as shown below.

### Range query
The range query method will automatically prune partitions that are completely outside the query range.
```scala
partitionedFeatures.rangeQuery(range)
```

### Spatial Join
If you run spatial join on two spatially partitioned RDDs, the partitions will be utilized to improve the performance by running the distributed join algorithm.
```scala
partitionedFeatures1.spatialJoin(partitionedFeatures2)
```

### Materializing the partitioned data
You can save the partitioned features to disk so that you can retrieve the partitioned data later.
```scala
// Save the partitioned data where each partition is stored as a shapefile
partitionedFeatures.saveAsShapefile("partitioned_data")
// Load the data back in a partitioned form
val loadedPartitioned = sparkContext.shapefile("partitioned_data")
assert(loadedPartitioned.isSpatiallyPartitioned)
```

The spatially partitioned set that is loaded can be used in the same way as the RDD that was loaded normally and then partitioned.

### Disjoint partitions
The following partitioners can produce spatially disjoint partitions where the partitions do not spatially intersect with each other.
If the features contain geometries with extents, e.g., lines or polygons, some of them might be replicated
to multiple partitions to produce spatially disjoint partitions.

- GridPartitioner
- RGrovePartitioner,RSGrovePartitioner,RRSGrovePartitioner
- KDTreePartitioner
- STRPartitioner

To produce disjoint partitions, use the following command.
```scala
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.{Fixed, NumPartitions}
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
val partitioner = IndexHelper.createPartitioner(features, classOf[RSGrovePartitioner],
  NumPartitions(Fixed, features.getNumPartitions), _ => 1, "disjoint" -> true)
val partitionedFeatures = features.spatialPartition(partitioner)
```

Notice that replicated records need to be taken into account when processing the query to avoid duplicate results. The rangeQuery and spatialJoin functions implemented in Beast employ a duplicate avoidance technique that produces the correct answer. For other functions, you need to carefully consider the duplicates in your algorithm.

## Partition a file from command line
If you need to partition some input data using a spatial partitioner into multiple files, Beast provide a command-line option for that. See the following example:
```shell
beast index <inputfile> iformat:shapefile gindex:rsgrove <outputpath>
```
The output will contain one file per partition. In addition, it will contain a master file that contains information about these partitions. The command can additionally take the following options.

- `-balanced`: Will try to produce highly-balanced partitions by computing the histogram of the data. Currently, only the RSGrovePartitioner supports this feature.
- `synopsissize:10m`: The size of the summary that is used to construct the partitioner. By default, only a sample of the input is used. If `-balanced` option is enabled, a histogram can also be constructed.
- `pcriterion:size(128m)`: The criterion used to determine the number of produced partitions. The available criteria are: `fixed(n)` which produces roughly `n` partitions; `size(s)` will produce partitions with the size of each partition roughly `s`; and `count(c)` produces partition without roughly `c` features per partition.
- `oformat:xxx`: The format of the output files. By default, the same input format of the input is used.
- `-disjoint`: Produces disjoint partitions if the selected partitioner supports that. If it does not, an error will be raised.