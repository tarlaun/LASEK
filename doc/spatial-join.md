# Big Spatial Join

Spatial join is one of the most compute-intensive spatial operations. It finds pairs of spatial features that satisfy some spatial predicate, e.g., intersects or contains. For example, you can use spatial join to find the city that contains each point or to find intersecting roads and power lines.
Beast comes with several spatial join algorithms and optimizations that can be parallelized to handle big data.
This article describes how to use them and how they work.

## Quick Code Example
The following code snippet loads two datasets, geotagged tweets (points) and US state boundaries (polygons), and runs a spatial join operation to find the state that contains each tweet.

### Scala
```scala
// Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
val polygons = sc.shapefile("tl_2018_us_state.zip")

// Load points in GeoJSON format.
// Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/data_index.geojson.gz?mbr=-117.8538,33.2563,-116.8142,34.4099
val points = sc.geojsonFile("Tweets.geojson.gz")

// Run a spatial join operation between points and polygons (point-in-polygon) query
val sjResults: RDD[(IFeature, IFeature)] =
    matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)
```

### Java
```java
// Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
JavaRDD<IFeature> polygons = SpatialReader.readInput(sparkContext, new BeastOptions(), "tl_2018_us_state.zip", "shapefile");

// Load points in GeoJSON format.
// Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/data_index.geojson.gz?mbr=-117.8538,33.2563,-116.8142,34.4099
JavaRDD<IFeature> points = SpatialReader.readInput(sparkContext, new BeastOptions(), "Tweets.geojson.gz", "geojson");

// Run a spatial join operation between points and polygons (point-in-polygon) query
JavaPairRDD<IFeature, IFeature> sjResults = JavaSpatialRDDHelper.spatialJoin(matchedPolygons, matchedPoints,
    SpatialJoinAlgorithms.ESJPredicate.Contains, SpatialJoinAlgorithms.ESJDistributedAlgorithm.PBSM);
```

## Supported Algorithms
Beast supports four distributed spatial join algorithms, BNLJ, PBSM, DJ, and REPJ, which are described below.

### Block nested loop join (BNLJ)
This algorithm simply runs a cross product between all pairs of partitions in the input RDDs.
For each pair of RDDs, it runs a plane-sweep algorithm to find the matching features.
This algorithm is recommended if you have one very small dataset.
In this case, the small dataset will be effectively replicated to all machines while the bigger
dataset will be partitioned so the algorithm will behave similarly to a broadcast join.

### Partition Based Spatial-Merge join (PBSM)
This algorithm is based on the PBSM algorithm originally proposed by
[Patel _et al_](https://doi.org/10.1145/233269.233338). It partitions the space using a uniform grid
and runs a plane-sweep algorithm for each grid cell. This algorithm is recommended if the two datasets 
are big and are not indexed. Instead of the regular grid, PBSM can also use the R*-Grove partitioner 
to improve the load balance.

### Distributed Join (DJ)
The DJ algorithm can only be applied if the two datasets are already spatially partitioned. 
In this case, the DJ algorithm finds all pairs of spatially overlapping partitions and runs a plane-sweep algorithm
for each pair of overlapping partitions.

### Repartition Join (REPJ)
This algorithm is applicable only if at least one dataset is spatially partitioned.
It works by partitioning the non-partitioned dataset to match the partitioned dataset.
Once both datasets are partitioned using the same partitioned, the algorithm moves forward similar to the DJ algorithm.

### Self Join (SJ)
This algorithm takes a single dataset and finds all pairs of overlapping records that are not equal to each other.
It first partitions this dataset using the R*-Grove partitioner and then processed each partitioning independently.
If the input is already partitioning using a disjoint partitioner, then the partitioning step will be skipped.

### Refinement optimization (Quad-split)
To further improve the performance of spatial join, Beast adds a refinement optimization technique called quad-split. If the average number of points per geometry is higher than 100, this optimization will be triggered to recursively split complex geometries until the maximum number of points per geometry is less than 100. The figure below illustrates how the quad split technique works. Gaps in the split geometry are added for illustration only.
![qsplit.png](https://bitbucket.org/repo/Bgpd9Bx/images/795437936-qsplit.png)

### Default Algorithm
If no join algorithm is explicitly provided, Beast will make the following choice automatically.

- If both datasets are spatially partitioned, DJ will be used.
- If `n2 * n2 < P` then use PBSM where `n1` and `n2` is the number of partitions in the two datasets, respectively, and `P` is the default parallelism of the Spark context.
- If only one dataset is partitioned, REPJ will be used.
- If none of the datasets is partitioned, PBSM will be used.

Regardless of the spatial join algorithm use above, the quad split optimization is triggered when the average number of points per geometry is higher than 100.

## Join Predicates
Currently, the following join predicates are supported.

---------------------
| Predicate     | Meaning                                                                       |
|---------------|-------------------------------------------------------------------------------|
| Intersects    | (Default) Find all pairs of intersecting (non-disjoint) features              |
| MBRIntersects | Find all pairs of features whose minimum bounding rectangles (MBRs) intersect |
| Contains      | Find pairs of features where the first feature contains the second features   |

## Profiling
The spatial join algorithm accepts an additional (optional) parameters called `mbrCount`.
This parameter is of type `LongAccumulator` and is used to count the total number of MBR overlap tests.
This can be used as a rough estimate for the computation cost of the algorithm.
Notice that this number does not account for all stages of the algorithm so it cannot be used alone.