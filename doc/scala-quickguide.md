# Scala Example

This tutorial explains how to use Beast from a Scala program. Scala makes your code more concise and readable as it
provides direct access to Beast features. This tutorial will show a simple development setup and a few examples.
The complete source code can be found [here](../src/main/scala/edu/ucr/cs/bdlab/beastExamples/ScalaExamples.scala).

## Prerequisites

In order to use Beast, you need the following prerequisites installed on your machine.

* Java Development Kit (JDK). [Oracle JDK](https://www.oracle.com/technetwork/java/javase/downloads/index.html) 1.8 or later is recommended.
* [Apache Maven](https://maven.apache.org/) or [SBT](https://www.scala-sbt.org)

## Steps
### 1A. Project setup with Maven

If you have an existing Maven-based project, then you can integrate it with Beast by
adding the following dependency to your `pom.xml` file.
```xml
<!-- https://mvnrepository.com/artifact/edu.ucr.cs.bdlab/beast -->
<dependency>
  <groupId>edu.ucr.cs.bdlab</groupId>
  <artifactId>beast-spark</artifactId>
  <version>0.10.1-RC2</version>
</dependency>
```
Instead, you can create a new Maven project with the Beast template using the following command.
```shell
mvn archetype:generate -B -DgroupId=com.example.beastExample -DartifactId=beast-project \
    -DarchetypeGroupId=edu.ucr.cs.bdlab -DarchetypeArtifactId=distribution -DarchetypeVersion=0.10.1-RC2
```

If you do not have a project setup, you need to create a [simple project first](https://www.scala-sbt.org/1.x/docs/Hello.html).

### 2. Write your code

Now, you can write your code in the new project. Below, is a simple code that shows you how to use Beast.

### 2A. Initialize Spark Context
There is nothing special in this step. Initialize the Spark context as you do with normal Spark applications.
```scala
val conf = new SparkConf
conf.setAppName("Beast Example")
// Set Spark master to local if not already set
if (!conf.contains("spark.master"))
  conf.setMaster("local[*]")
```

### 2B. Import Beast features
```scala
import edu.ucr.cs.bdlab.beast._
```

### 2C. Load spatial files
```scala
// Load a shapefile. Download a sample file at: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
val polygons = sc.shapefile("tl_2018_us_state.zip")

// Load points in GeoJSON format. Download from https://star.cs.ucr.edu/?Tweets&d#mbr=9qh2vfk9,9qh9y89z
val points = sc.geojsonFile("Tweets_index.geojson")
```

### 2D. Perform a spatial filter (range query)
```scala
val range = new EnvelopeNDLite(2, -117.337182, 33.622048, -117.241395, 33.72865)
val matchedPolygons: RDD[IFeature] = polygons.rangeQuery(range)
val matchedPoints: RDD[IFeature] = points.rangeQuery(range)
```

### 2E. Run a spatial join operation
```scala
val sjResults: RDD[(IFeature, IFeature)] =
      matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)
```

### 2F. Prepare the output features
```scala
val finalResults: RDD[IFeature] = sjResults.map(pip => {
  val polygon: IFeature = pip._1
  val point: IFeature = pip._2
  Feature.append(point, polygon.getAs[String]("NAME"), "state")
})
```

### 2G. Write the output as a CSV file
```scala
finalResults.saveAsCSVPoints(filename="output", xColumn = 0, yColumn = 1, delimiter = ';')
```

## 3. Package

To package your code into JAR, simply run the following command.

```shell
mvn package
```

This will generate a new JAR under `target/` directory.

# 4. Run

Use the following command to run the main class.
```shell
beast --class edu.ucr.cs.bdlab.beastExamples.ScalaExamples target/projectname.jar 
```
PS: Make sure that the input files are accessible in the working directory.
