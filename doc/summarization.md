# Summarization of spatial data

Beast provides several ways for summarizing big spatial data. Summarization creates a small-size synopsis 
of a big dataset that can be used for approximate query processing or to make decisions about the data 
such as which algorithm to run. Below, we give an overview of the available summaries in Beast and how to calculate them.

## Geometry Summary

### Definition
A geometric summary is a fixed-size summary of type [Summary](../src/master/cg/src/main/scala/edu/ucr/cs/bdlab/beast/synopses/Summary.scala) that contains the following aggregate values:

- **size**: Total size of the input in bytes.
- **numFeatures**: Total number of features or records. Similar to `rdd.count()`.
- **numPoints**: The sum of the number of points in all geometries.
- **numNonEmptyGeometries**: The total number of features that contain a non-empty geometry.
- **sumSideLength[]**: The sum of all side lengths of the MBR of all geometries. This is an array equal to the number of dimensions of the geometries.
- **geometryType**: The most restrictive geometry type of all geometries in this data type.

All these summaries are computed in one pass over the data.

### How to compute
To compute the summary from command-line, use the command `beast summary`.

In Scala, use the `summary` action as shown below.
```scala
import edu.ucr.cs.bdlab.beast._
val rdd = sparkContext.shapefile("input.zip")
rdd.summary()
```

In Java, use the `summary` action as shown below.
```java
JavaRDD<IFeature> polygons = SpatialReader.readInput(sparkContext, new BeastOptions(), "input.zip", "shapefile");
Summary summary = JavaSpatialRDDHelper.summary(rdd);
```

An alternative method to compute the summary is as an accumulator. This allows you to piggy-back the calculation on any existing Spark job with little to no overhead. The following example shows that.
```scala
var features = sparkContext.shapefile("input.zip")
val accumulator = Summary.createSummaryAccumulator(sparkContext)
features = polygons.map(f=> {accumulator.add(f); f})
// ... run an action on features
val summary = accumulator.value
```

### A note about calculating the size
The size of the features in bytes depends on the format in which the records are represented. By default, the summary function uses an estimate of the memory representation assuming all coordinates are stored as 64-bit double-precision floating-point numbers. However, this behavior can be overridden by providing a function that measures the size of each record. For example, the following code snippet overrides the calculating assuming that each point has two 32-bit single-precision floating-point coordinates.

```scala
val summary = Summary.computeForFeatures(features, f => f.getGeometry.getNumPoints * 2 * 4)
```

Beast provides a short-hand for estimating the size assuming that the records are written with one of the supported file formats. For example, if you want to estimate the size assuming that features are written as  GeoJSON file, use the following command.
```scala
// Estimate the output size if the records are written to GeoJSON
val summary = GeometricSummary.computeForFeaturesWithOutputSize(features, "iformat" -> "geojson")
```

## Simple Histogram
### Definition
The histogram partitions the space using a uniform grid and measures one aggregate function in each cell in the grid. For features that span multiple grid cells, the feature is assigned to the grid cell that contains the center of the MBR of the record. This function can be either the number of records or the total size of the records. The size of the records is computed as described above and can be overridden in the same way.

### How to compute
The following two commands compute a count and size histograms, respectively. The size of the histogram in both of them is 100x100.
```scala
val countHistogram = features.uniformHistogramCount(Array(100, 100))
val sizeHistogram = features.uniformHistogramSize(Array(100, 100))
```
The count histogram counts the number of features in each cell while the size histogram computes the total size of all features in each cell. With the size histogram, the size of the feature can be overridden a shown below.
```scala
val sizeHistogram = features.uniformHistogramSize(Array(100, 100),
    sizeFunction = new FeatureWriterSizeFunction("iformat" -> "geojson"))
```

### How to use the histogram
The following code snippet shows different ways you can use the histogram.
```scala
// Get the value of the cell at position (4, 3)
histogram.getValue(Array(4, 3), Array(1, 1))
// Sum the values of the cells in a window with a corner at position (4, 3) with size 10 x 10
histogram.getValue(Array(4, 3), Array(10, 10))
// Get the value at the bin that contains the point with coordinates (-120.3, 34.4)
histogram.getBinValue(histogram.getBinID(Array(-120.3, 34.4)))
```

### Efficiency of range queries
If you plan to run many large range queries over the histogram, you can make them more efficient without increasing the memory requirement by computing the prefix sum. Currently, this is only supported for two-dimensional histograms since the computation cost increases exponentially with the number of dimensions.
```scala
val histogram = polygons.uniformHistogramCount(Array(100, 100), prefixSum = true)
// The following function will run in constant time regardless of the size of the range
histogram.getValue(Array(4, 3), Array(10, 10))
```
The prefix histogram feature works with both count and size histogram.

## Euler Histogram
### Definition
Euler histogram addresses the problem of features that span multiple grid cells. Instead of assigning the feature to only one cell, this function maintains four counters in each cell that count the number of features that have:

1. A corner in that cell.
2. A top edge that goes through the cell.
3. A left edge that goes through the cell.
4. Any overlay with the cell.

These four counters are combined to accurately compute the desired function in any range of cells in the histogram. Notice that this additional accuracy comes with the cost of 4x increase in the memory requirement. If the input features contain only points or very small rectangles, it might not be worth using Euler histogram.

### How to compute
The following commands compute the count and size Euler histograms.
```scala
val eulerCountHistogram = polygons.eulerHistogramCount(Array(100, 100))
val eulerSizeHistogram = polygons.eulerHistogramSize(Array(100, 100))
```

The Euler histogram is used in the same way as the regular histogram. It can also be overloaded with a custom size function. Finally, the prefix histogram can also be applied to Euler histogram to speed up the range queries on the histogram.

## Conclusion of the histogram
In conclusion, here are the main decisions you will need to make to compute a histogram.

1. Simple/Euler histogram: Use the simple histogram if all the features contain points or geometries with a very small spatial range. Use the Euler histogram only if the features contain geometries with a large spatial span.
2. Count/Size histogram: This really depends on the application. If you are interested in counting the features regardless of their sizes, use the count histogram. If the complexity of the geometry makes a difference to your application or if there is a custom weighting function to each feature, then use the size histogram.
3. Prefix Sum?: If you are going to query large portions of the generated histogram, in terms of the number of grid cells, then use the prefix sum.