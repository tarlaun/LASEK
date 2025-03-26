# Beast
Beast is a Spark add-on for Big Exploratory Analytics of Spatio-temporal data. There are three ways of accessing Beast functions.

## TL;DR

**CLI**: To setup the CLI interface and beast-shell, extract the binary package in [.tar.gz](../../../downloads/beast-0.10.1-RC2-bin.tar.gz) format.
Run `$BEAST/bin/beast` to launch the command line interface. 

**Dev**: To create a new Java/Scala project for Beast using the following command.
```shell
mvn archetype:generate -B -DgroupId=com.example.beastExample -DartifactId=beast-project \
    -DarchetypeGroupId=edu.ucr.cs.bdlab -DarchetypeArtifactId=distribution -DarchetypeVersion=0.10.1-RC2
```

Or in your existing project, add the following dependency:
```xml
<!-- https://mvnrepository.com/artifact/edu.ucr.cs.bdlab/beast -->
<dependency>
  <groupId>edu.ucr.cs.bdlab</groupId>
  <artifactId>beast-spark</artifactId>
  <version>0.10.1-RC2</version>
</dependency>
```

## 1- Command-line Interface (CLI)
With this method, you can access some preset operations on spatial files in various formats such as Shapefile, CSV, and GeoJSON.

1. Download the latest Beast binaries in [.tar.gz](../../../downloads/beast-0.10.1-RC2-bin.tar.gz) or [.zip](../../../downloads/beast-0.10.1-RC2-bin.zip) format.
2. Extract the downloaded package in your home directory or where you wish to install it.
3. (Optional) Add `extractdir/bin` to your executable path.
```shell
export PATH=$HOME/beast-0.10.1-RC2/bin:$PATH
```
4. To test that it is installed correctly, run `beast` and you should get output similar to the following.
```
****************************************************
Choose one of the following operations to run
cat - Writes a file to the output. Used to convert file format
gridsummary - Computes data summary over a grid on the data and writes it to a CSV file
histogram - Computes a uniform histogram for a geometry file
imagex - Extracts images for a set of geometries from another set of geospatial images
index - Builds a distributed spatial index
mindex - Compute index metadata (master files) of the given dataset in different indexes
mplot - Plots the input file as a multilevel pyramid image
raptor - Computes RaptorZS
server - Starts the web UI server for Beast
sj - Computes spatial join that finds all overlapping features from two files.
soilsalinity - Computes NDVI
splot - Plots the input file as a single image
summary - Computes the minimum bounding rectangle (MBR), count, and size of a dataset
vmplot - Plots the input file as a multilevel pyramid image with mvt tiles
zs - Computes zonal statistics between a vector file and a raster file. Input files (vector, raster)
****************************************************
```
## 2- Beast Interactive Scala shell
After following steps 1-3 above, you can start Beast shell by typing `beast-shell`.
This starts a Scala shell similar to `spark-shell` with Beast classes loaded and linked.

## 3- Application Programming Interface (API) for Scala/Java RDD and SparkSQL
This method allows you to develop programs in Java or Scala for Spark that use the features from Beast
such as loading spatial files in various formats, spatially partition the data for efficient processing and load balancing, and visualize spatial data.

## More details

* Start on this page to [setup your project for Beast](dev-setup.md).
* Check this [quick guide for Beast Scala functions](scala-quickguide.md).
* [Supported input and output file formats](input-output.md).
* [Spatial Data Generator](spatial-data-generator.md).
* [Summarization of spatial data](summarization.md)
* [Spatial partitioning and indexing](partitioning-indexing.md).
* [Spatial join](spatial-join.md).
* [Raster visualization functions](visualization.md) and [vector visualization](visualization_mvt.md)
* [Raster processing](rdpro.md) and [Raster+Vector (Raptor) join](raptor-join.md)

## Questions
If you have any questions, please send them on [StackOverflow](http://stackoverflow.com) and tag them with `#beast` and `#ucrbeast`. The team tracks questions on these hashtags and will answer.
