# Spatial Data Generator

## TL;DR
Beast can generate large datasets with points, boxes, or polygons. Use [Spider](https://spider.cs.ucr.edu/) to explore the available options and generate stub code to use.

```scala
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.generator._

val randomPoints: RDD[IFeature] = sc.generateSpatialData.uniform(1000000000)

val randomBoxes: RDD[IFeature] = sc.generateSpatialData
  .distribution(GaussianDistribution)
  .config(UniformDistribution.GeometryType, "box")
  .config(UniformDistribution.MaxSize, "0.01,0.01")
  .generate(cardinality=10000000)

val randomPolygons: RDD[IFeature] = sc.generateSpatialData
  .distribution(BitDistribution)
  .config(UniformDistribution.GeometryType, "polygons")
  .config(UniformDistribution.NumSegments, "20")
  .generate(cardinality=10000000)
```

## Introduction
Working with big spatial data often involves testing algorithms for corner cases and stress tests for large scale data. Wouldn't it be convenient to have an easy and standardized way of doing these tests without having to upload huge amounts of test data? The Spatial Data Generator (SDG) is a special component added in Beast that can generate standardized data for testing. Even better, the data is generated in parallel so you do not even have to worry about loading a big file into your cluster. This article explain how to use SDG and how it internally works. SDG is available since Beast 0.8.3.

*Keywords*: Benchmarking, spatial data generator, stress test, scalability tests

## Prerequisites
Create a new [Beast project](dev-setup) in Scala or configure your Spark project to use Beast by adding it as a dependency.

## How to use
The easiest way to use SDG is by directly generating a big dataset as a distributed dataset in Spark. For example, the following command generates a uniform dataset with one billion points.
```scala
import edu.ucr.cs.bdlab.beast._
val randomData: SpatialRDD = sc.generateSpatialData.uniform(1000000000)
```
Similarly, the following command generates a parcel dataset with one million boxes with the shown parameters.
```scala
val parcels: SpatialRDD = sc.generateSpatialData
      .parcel(1000000, dither = 0.1, splitRange = 0.4)
```

To explore the distributions and decide on the distribution parameters, you can use our interactive spatial data generator, [SpiderWeb: https://spider.cs.ucr.edu](https://spider.cs.ucr.edu). Check our [YouTube video](https://youtu.be/h0xCG6Swdqw) on how to use SpiderWeb.

### Generator Configuration
All generators support the following configuration parameters.

- `SpatialGenerator.Dimensions` (Default: 2): Specifies the number of dimensions of the generated data. Diagonal and Sierpinski distributions currently support only two-dimensional data.
- `SpatialGenerator.RecordsPerPartitoin` (Default: 1000000): Maximum number of records per partition. If you generate more than that number, multiple partitions will be created.
- `SpatialGenerator.Seed` (Default: System.currentTimeMillis): Specifies a seed for the random number generator. This ensures that SDG will always generate the same dataset by fixing the seed.
- `SpatialGenerator: AffineMatrix` (Default: Identity Matrix): An optional affine matrix that is applied on each record to scale, translate, and rotate the space of the generated data. The affine matrix is provided as a single string with six values (sx, ry, rx, ,sy, tx, ty) as demonstrated on [https://spider.cs.ucr.edu] and with the same order used in the Java [AffineTransform](https://docs.oracle.com/javase/8/docs/api/java/awt/geom/AffineTransform.html) class.

### Point-based Generators
The five generators, uniform, diagonal, Gaussian, bit, and Sierpinski are called point-based generators. They are mainly defined for points but they can be extended to generate boxes by using these points as centers for random rectangles. To use them for generating boxes, use the `makeBoxex` function before calling the corresponding generator. For example, the following code generates boxes with random sizes in the range of [0, 0.3) and [0, 0.4] for the width and height, respectively.

```scala
sparkContext.generateSpatialData
      .makeBoxes(0.3, 0.4)
      .uniform(1000000)
```

### Uniform Generator
The uniform distribution generates points in the unit square where all dimensions are uniformly distributed in the range (0,1). Below is an example of how the data looks like.
```scala
sc.generateSpatialData
  .makeBoxes(0.1, 0.2)
  .uniform(100)
  .plotImage(300, 300, "uniform.png")
```
Result:

![Uniformly distributed data](images/uniform.png)

### Diagonal Generator
The diagonal distribution generates points along the diagonal line that goes from (0,0) to (1,1). Currently, the diagonal distribution supports only two-dimensional data. It can be configured with the following additional parameters.

- `percentage`: The ratio of points that lie exactly on the diagonal line.
- `buffer`: The size of the buffer around the diagonal line on which the remaining points are distributed.

```scala
sc.generateSpatialData
  .diagonal(1000, percentage = 0.3, buffer = 0.2)
  .plotImage(300, 300, "diagonal.png",
    opts = Seq(GeometricPlotter.PointSize -> 0))
```
Result:

![Diagonally distributed data](images/diagonal.png)

### Gaussian Generator
The Gaussian generator generates points in which every dimension follows a normal distribution centered at 0.5 and with a standard deviation 0.2. Additionally, all generated points are in the unit square.
are distributed.
```scala
sc.generateSpatialData
  .gaussian(1000)
  .plotImage(300, 300, "gaussian.png",
     opts = Seq(GeometricPlotter.PointSize -> 0))
```
Result:

![Gaussian distributed data](images/gaussian.png)

### Bit Generator
The bit distribution generates points where each dimension is a random number in the range [0, 1). The dimension is generated by setting each bit in a floating-point number with some fixed probability. The following two parameters can be used to configure the bit distribution.

- `digits` (Default: 10): The number of binary digits to set in the generated number.
- `probability` (Default: 0.2): The probability of setting each bit.

```scala
sc.generateSpatialData
  .bit(1000, digits = 10, probability = 0.2)
  .plotImage(300, 300, "bit.png",
    opts = Seq(GeometricPlotter.PointSize -> 0))
```
Result:

![Bit distributed data](images/bit.png)

### Sierpinski Generator
The Sierpinski distribution is a fractal-based distribution where points are randomly generated in the Sierpinski triangle shown below. Currently, only two-dimensional data is supported. No parameters are needed for this generator.
```scala
sc.generateSpatialData
  .sierpinski(1000)
  .plotImage(300, 300, "sierpinski.png",
    opts = Seq(GeometricPlotter.PointSize -> 0))
```
Result:

![Sierpinski distributed data](images/sierpinski.png)

### Parcel Generator
This generator produces boxes that resemble parcel areas. It works by recursively splitting the input domain (unit square) along the longest dimension and then randomly dithering each generated box to add some randomness. This generator can only generate boxes and can be configured using the following parameters.

- `dither ` (Default: 0.2): The amount of randomness added to each box. If dither is zero, no randomness is added. If dither is 1.0, all values are allowed.
- `splitRange ` (Default: 0.2): The range in which the box is split. This parameter can be in the range [0.0, 0.5]. 0.5 means all ranges are allowed for splitting. 0.5 means that the box will always be split in half.

```scala
sc.generateSpatialData
  .parcel(100, dither = 0.2, splitRange = 0.3)
  .plotImage(300, 300, "parcel.png")
```
Result:

![Parcel distributed data](images/parcel.png)

### Affine Transformation
Since all generators generate points in the unit square, an additional affine transformation can be added to transform all the generated data. This makes it possible to generate data in any region and of any size. For example, the following code snippet generates data and scales it along the x-axis with a scale of 2.0 and translate it along the y-axis with 3.0 points.
```scala
val transform = new AffineTransform()
transform.scale(2.0, 1.0)
transform.translate(0.0, 3.0)
println(sparkContext.generateSpatialData
  .affineTransform(transform)
  .uniform(1000)
  .summary)
```
Result:
```
MBR: [(8.241933801833579E-4, 3.003420311794198), (1.9951256046603492, 3.995727367390311)], size: 80000, numFeatures: 1000, numPoints: 1000, avgSideLength: [0.0, 0.0]
```

As a short-hand for generating data in an arbitrary box, use the function `mbr` as follows.
```scala
println(sparkContext.generateSpatialData
  .mbr(new EnvelopeNDLite(2, 1.0, 0.0, 4.0, 8.0))
  .uniform(1000)
  .summary)
```

Result:
```
MBR: [(1.0010679230316184, 0.005049640711646042), (3.9970940730159423, 7.996089672986504)], size: 80000, numFeatures: 1000, numPoints: 1000, avgSideLength: [0.0, 0.0]
```

An easy way to determine the affine transformation parameters is to use [SpiderWeb](https://spider.cs.ucr.edu) to see how the generated data will look like and then copy the affine transformation values from the web interface.

## How it works
SDG works by defining a new type of RDD that directly generates the data. Upon creation of the RDD, the partitions are created based on the distribution type and the number of records to generate. Data is not generated immediately, rather, they are lazily generated when the RDD is processed with an action. This makes SDG very scalable since the generation works in parallel. For all point-based generators, the partitions are all initialized with the same parameters of the dataset since each point is generated independently. This means that all partitions cover almost the entire input space. However, this approach does not work with the parcel distribution since the generated boxes must be disjoint.

To generate parcel dataset in parallel, the generator runs in two phases. First, the parcel generator is used to generate the boundaries of the partitions. For example, if five partitions are to be generated, we use the parcel generator to generate only five boxes on the input space. In that phase, the `dither` parameter is set to zero to ensure that the entire input space is covered and because the dithering step should be applied to the final boxes, not the partitions. Second, the parcel generator is used to generate data within each partition. The affine matrix is used to ensure that the boxes within each partition are generated within the space defined in the first step. In summary, this means that, unlike the other five generators, the partitions of the parcel distribution are always disjoint.

## Further Readings
* [Spider Web: A Spatial Data Generator on the Web](https://spider.cs.ucr.edu)
* The original SDG paper: [Tin Vu, Sara Migliorini, Ahmed Eldawy, and Alberto Bulussi. "Spatial Data Generators", In 1st ACM SIGSPATIAL International Workshop on Spatial Gems (SpatialGems 2019)](https://www.cs.ucr.edu/~eldawy/publications/19_SpatialGems.pdf).
* Spider Web demonstration paper: [Puloma Katiyar, Tin Vu, Sara Migliorini, Alberto Belussi, and Ahmed Eldawy. SpiderWeb: A Spatial Data Generator on the Web, In 28th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems (ACM SIGSPATIAL 2020), November 2020. DOI>10.1145/3397536.3422351](https://www.cs.ucr.edu/~eldawy/publications/20-SIGSPAITAL-SpatialDataGenerators-Demo.pdf)