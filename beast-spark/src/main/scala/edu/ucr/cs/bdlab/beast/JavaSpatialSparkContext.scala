/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import org.apache.spark.api.java._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
 * A wrapper around JavaSparkContext that adds spatial functions for loading files.
 */
class JavaSpatialSparkContext(val _sc: SparkContext) extends JavaSparkContext(_sc) {

  // Semi-delegate methods that call the ones defined for SparkContext but returns Java objects

  /**
   * Reads the given file according to the given spatial format. If spatial format is not given, it auto-detects
   * the input based on the extension and then file contents (for CSV files only)
   * @param filename the name of the file or directory of files
   * @param iformat the input format as a short string. If `null`, the input format is auto-detected
   * @param opts additional options for initializing the input reader
   * @return the [[JavaSpatialRDD]] that contains the records
   */
  def spatialFile(filename: String, iformat: String, opts: BeastOptions): JavaSpatialRDD =
    JavaRDD.fromRDD(sc.spatialFile(filename, iformat, opts))

  /**
   * Reads the given file according to the given spatial format. If spatial format is not given, it auto-detects
   * the input based on the extension and then file contents (for CSV files only)
   * @param filename the name of the file or directory of files
   * @param iformat the input format as a short string. If `null`, the input format is auto-detected
   * @return the [[JavaSpatialRDD]] that contains the records
   */
  def spatialFile(filename: String, iformat: String): JavaSpatialRDD =
    spatialFile(filename, iformat, new BeastOptions())

  /**
   * Read the given file name as a spatial file and use the associated options to determine how to load it
   * @param filename the file name
   * @param opts the use options that can be used to determine how to load the file
   * @return the loaded file as a [[SpatialRDD]]
   */
  def spatialFile(filename: String, opts: BeastOptions): JavaSpatialRDD =
    spatialFile(filename, opts.getString(SpatialFileRDD.InputFormat), new BeastOptions())

  /**
   * Reads features from an Esri Shapefile(c)
   * @param filename the name of the .shp file, a compressed ZIP file that contains shapefiles,
   *                 or a directory that contains shapefiles or ZIP files.
   * @return an RDD of features
   */
  def shapefile(filename: String) : JavaSpatialRDD = spatialFile(filename, "shapefile")

  /**
   * Reads data from a Shapefile
   * @param filename the name of the GeoJSON file or a directory that contains GeoJSON file
   * @return an RDD of features
   */
  def geojsonFile(filename: String) : JavaSpatialRDD = spatialFile(filename, "geojson")

  /**
   * Reads points from a CSV file given the names of the columns that contain the x and y coordinates
   * @param filename the name of the file or directory that contains the data
   * @param xColumn the name or index of the column that contains the x coordinate
   * @param yColumn the name or index of the column that contains the y coordinate
   * @param delimiter the field delimiter, e.g., ',' or '\t'
   * @param skipHeader whether to skip the header line or not. If the xColumn and yColumn are names,
   *                   this parameter should be `true` to work correclty.
   * @return the set of records in the file
   */
  def readCSVPoint(filename: String, xColumn: String, yColumn: String, delimiter: Char, skipHeader: Boolean): JavaSpatialRDD =
    JavaRDD.fromRDD(sc.readCSVPoint(filename, xColumn, yColumn, delimiter, skipHeader))

  /**
   * Read a CSV file with WKT-encoded geometry
   * @param filename the name of the file or directory fo the input
   * @param wktColumn the column that includes the WKT-encoded geometry, either an Integer for the index of the
   *                  attribute or String for its name
   * @param delimiter the field delimiter, comma by default
   * @param skipHeader whether to skip the header line or not, if wktColumn is a string, this has to be true,
   *                   if wktColumn is an Integer, this is false by default but can be overloaded
   * @return the set of features in the input file
   */
  def readWKTFile(filename: String, wktColumn: String, delimiter: Char, skipHeader: Boolean): JavaSpatialRDD =
    JavaRDD.fromRDD(sc.readWKTFile(filename, wktColumn, delimiter, skipHeader))

  /**
   * Reads a Hierarchical Data Format (HDF) file or a directory of HDF files
   * @param filename the name of the file or directory
   * @param layer the layer name to read from the file
   * @return a raster RDD that contains all the tiles in the given file(s)
   */
  def hdfFile(filename: String, layer: String): JavaRasterRDD[Float] =
    JavaRDD.fromRDD(sc.hdfFile(filename, layer))

  /**
   * Reads a geoTiff file as an RDD of tiles.
   * @param filename the name of the GeoTIFF file or the directory that contains GeoTIFF files.
   * @param layer the index of the layer to read
   * @param opts additional options to pass to the reader
   * @return a raster RDD that contains all the tiles in the given input file(s)
   */
  def geoTiff[T](filename: String, layer: Int, opts: BeastOptions): JavaRasterRDD[T] =
    JavaRDD.fromRDD(sc.geoTiff(filename, layer, opts))

  /**
   * Reads a geoTiff file as an RDD of tiles. This function loads the first image in each GeoTIFF file.
   * @param filename the name of the GeoTIFF file or the directory that contains GeoTIFF files.
   * @param layer the index of the layer to read
   * @return a raster RDD that contains all the tiles in the given input file(s)
   */
  def geoTiff[T](filename: String, layer: Int): JavaRasterRDD[T] =
    JavaRDD.fromRDD(sc.geoTiff(filename, layer))

  /**
   * Reads a geoTiff file as an RDD of tiles. This function loads the first image in each GeoTIFF file.
   * @param filename the name of the GeoTIFF file or the directory that contains GeoTIFF files.
   * @return a raster RDD that contains all the tiles in the given input file(s)
   */
  def geoTiff[T](filename: String): JavaRasterRDD[T] =
    JavaRDD.fromRDD(sc.geoTiff(filename))

  // The following section contains constructors similar to the ones that come with JavaSparkContext

  def this() = this(new SparkContext())

  def this(jsc: JavaSparkContext) = this(jsc.sc)

  /**
   * @param conf a [[org.apache.spark.SparkConf]] object specifying Spark parameters
   */
  def this(conf: SparkConf) = this(new SparkContext(conf))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   */
  def this(master: String, appName: String) = this(new SparkContext(master, appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(conf.setMaster(master).setAppName(appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile JAR file to send to the cluster. This can be a path on the local file system
   *                or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(master: String, appName: String, sparkHome: String, jarFile: String) =
    this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String]) =
    this(new SparkContext(master, appName, sparkHome, jars))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String],
           environment: java.util.Map[String, String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq, environment.asScala))
}
