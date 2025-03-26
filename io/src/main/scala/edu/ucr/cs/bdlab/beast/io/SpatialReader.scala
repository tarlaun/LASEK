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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite._
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{CoordinateXY, CoordinateXYM, CoordinateXYZM, Geometry}
import org.locationtech.jts.io.WKTReader

import java.io.Serializable
import java.util

/**
  * Reads and parses input files that contain spatial features into RDDs
  */
object SpatialReader extends Logging {

  /**
    * A function that parses WKT-encoded CSV files
    * @param fieldSeparator the single character that separates fields in the same line
    * @param gCol the 0-based index of the column that contains the WKT geometry
    */
  class RDDWKTParser(fieldSeparator: Char, gCol: Int, quoteCharacters: String = CSVFeatureReader.DefaultQuoteCharacters)
    extends Function[String, IFeature] with Serializable {

    /**WKT parser to parse WKT-encoded geometries*/
    @transient var wktReader: WKTReader = _

    /**A temporary Text line for extracting fields and WKT values*/
    @transient var line: Text = _

    override def apply(s: String): IFeature = {
      if (line == null) line = new Text
      if (wktReader == null) {
        wktReader = new WKTReader(GeometryReader.getGeometryFactory(4326))
        // Leaving this flag as true results in returning CoordinateSequence of dimension 3 even if the input
        // has only two dimensions
        wktReader.setIsOldJtsCoordinateSyntaxAllowed(false)
      }
      line.set(s.getBytes)

      val wkt = CSVFeatureReader.deleteAttribute(line, fieldSeparator, gCol, quoteCharacters)
      val geometry =  if (wkt == null) {
        logWarning(s"Could not find the field #$gCol with the separator '$fieldSeparator' in the line '$line'")
        EmptyGeometry.instance
      } else {
        wktReader.read(wkt)
      }
      var values = Array[Any]()
      while (line.getLength > 0) {
        values = values :+ CSVFeatureReader.deleteAttribute(line, fieldSeparator, 0, quoteCharacters)
      }
      Feature.create(geometry, null, null, values)
    }
  }

  def parseWKT(lines: JavaRDD[String], gCol: Int, fieldSeparator: Char): JavaSpatialRDD =
    parseWKT(lines.rdd, gCol, fieldSeparator)

  /**
    * Parse a text file that contains WKT-encoded geometries
    *
    * @param lines an RDD of text as one record per line
    * @param gCol the index of the field that contains the WKT geometry
    * @param fieldSeparator the field (column) separator, e.g., tab (\t) or comma (,)
    * @return a parse RDD where each line is converted to a feature
    */
  def parseWKT(lines: RDD[String], gCol: Int, fieldSeparator: Char) : SpatialRDD =
    lines.map(new RDDWKTParser(fieldSeparator, gCol))

  /**
    * A class that parses CSV lines with point attributes
    * @param fieldSeparator the single character that separates fields in the same line
    * @param colIndexes the indices of the columns that contain the coordinates of the point
    */
  class RDDPointParser(fieldSeparator: Char, private var colIndexes: Array[Int],
                       quoteCharacters: String = CSVFeatureReader.DefaultQuoteCharacters) extends Function[String, IFeature] with Serializable {
    /**
      * Adjust the positions of the columns so that they can be extracted in order
      */
    if (colIndexes != null)
    for (i <- 0 until colIndexes.size) {
      if (colIndexes(i) != -1) {
        for (j <- i + 1 until colIndexes.size) {
          if (colIndexes(j) > colIndexes(i))
            colIndexes(j) = colIndexes(j) - 1
        }
      }
    }

    val geometryFactory = GeometryReader.getGeometryFactory(4326)

    /**A temporary line to use for parsing. It avoids recreating a Text object for each record.*/
    @transient private var line : Text = _

    override def apply(s: String): IFeature = {
      if (line == null) line = new Text
      line.set(s)
      val coords: Array[Double] = new Array[Double](colIndexes.size)
      util.Arrays.fill(coords, Double.NaN)
      for (iCol <- 0 until colIndexes.size)
        if (colIndexes(iCol) != -1)
          coords(iCol) = CSVFeatureReader.deleteAttribute(line, fieldSeparator, colIndexes(iCol), quoteCharacters).toDouble

      val p: Geometry = coords.length match {
        case 2 => geometryFactory.createPoint(new CoordinateXY(coords(0), coords(1)))
        case 3 => geometryFactory.createPoint(new CoordinateXYM(coords(0), coords(1), coords(2)))
        case 4 => geometryFactory.createPoint(new CoordinateXYZM(coords(0), coords(1), coords(2), coords(3)))
        case _ => new PointND(geometryFactory, coords: _*)
      }
      var values = Array[Any]()
      while (line.getLength > 0)
        values = values :+ CSVFeatureReader.deleteAttribute(line, fieldSeparator, 0, quoteCharacters)
      Feature.create(p, (1 to values.length).map(i => s"$$attr$i").toArray, null, values)
    }
  }

  /**Java shortcut*/
  def parsePointsXY(textFile: JavaRDD[String], xCol: Int, yCol: Int, fieldSeparator: Char): JavaSpatialRDD =
    JavaRDD.fromRDD(parsePointsXY(textFile.rdd, xCol, yCol, fieldSeparator))

  /**
    * Parses a CSV file with a custom separator that contains two-dimension points. This is a Spark transformation.
    *
    * @param textFile       a text file loaded as one line per record
    * @param xCol           the index of the column that contains the x-coordinate (0-based)
    * @param yCol           the index of the column that contains the y-coordinate (0-based)
    * @param fieldSeparator the field separator
    * @return a new RDD that transforms that given text file into { @link IFeature}s with points as geometries.
    */
  def parsePointsXY(textFile: RDD[String], xCol: Int, yCol: Int, fieldSeparator: Char): SpatialRDD =
    textFile.map(new RDDPointParser(fieldSeparator, Array(xCol, yCol)))

  /**Java shortcut*/
  def parsePointsXYZ(textFile: JavaRDD[String], xCol: Int, yCol: Int, zCol: Int,
                      fieldSeparator: Char): JavaSpatialRDD =
    JavaRDD.fromRDD(parsePointsXYZ(textFile.rdd, xCol, yCol, zCol, fieldSeparator))

  /**
    * A transformation that transforms a text file (CSV) into three-dimensional points.
    *
    * @param textFile       the text file to parse as a single line per record
    * @param xCol           the index of the column that contains the x-coordinate (0-based)
    * @param yCol           the index of the column that contains the y-coordinate (0-based)
    * @param zCol           the index of the column that contains the z-coordinate (0-based)
    * @param fieldSeparator the field separator
    * @return the transformed RDD that contains the features with points as geometries
    */
  def parsePointsXYZ(textFile: RDD[String], xCol: Int, yCol: Int, zCol: Int, fieldSeparator: Char): SpatialRDD =
    parsePointsXYZM(textFile, xCol, yCol, zCol, -1, fieldSeparator)

  /**Java shortcut*/
  def parsePointsXYM(textFile: JavaRDD[String], xCol: Int, yCol: Int, mCol: Int, fieldSeparator: Char): JavaSpatialRDD =
    JavaRDD.fromRDD(parsePointsXYM(textFile.rdd, xCol, yCol, mCol, fieldSeparator))

  /**
    * A transformation that transforms a text file (CSV) into two-dimensional points with measure (m) values.
    *
    * @param textFile       the text file to parse as a single line per record
    * @param xCol           the index of the column that contains the x-coordinate (0-based)
    * @param yCol           the index of the column that contains the y-coordinate (0-based)
    * @param mCol           the index of the column that contains the measure value (0-based)
    * @param fieldSeparator the field separator
    * @return the transformed RDD that contains the features with points as geometries
    */
  def parsePointsXYM(textFile: RDD[String], xCol: Int, yCol: Int, mCol: Int, fieldSeparator: Char): SpatialRDD =
    parsePointsXYZM(textFile, xCol, yCol, -1, mCol, fieldSeparator)

  /**
    * Parses an input text that contains the attributes x, y, z, and m. If zCol or mCol are -1 they are ignored.
    *
    * @param textFile       the text file to parse as a single line per record
    * @param xCol           the index of the column that contains the x-coordinate (0-based)
    * @param yCol           the index of the column that contains the y-coordinate (0-based)
    * @param zCol           the index of the column that contains the z-coordinate (0-based)
    * @param mCol           the index of the column that contains the measure value (0-based)
    * @param fieldSeparator the field separator
    * @return the transformed RDD that contains the features with points as geometries
    */
  def parsePointsXYZM(textFile: RDD[String], xCol: Int, yCol: Int, zCol: Int, mCol: Int, fieldSeparator: Char): SpatialRDD =
    textFile.map(new RDDPointParser(fieldSeparator, Array(xCol, yCol, zCol, mCol)))

  /**Java shortcut*/
  def readInput(sc: JavaSparkContext, opts: BeastOptions, filename: String, iFormat: String): JavaSpatialRDD =
    JavaRDD.fromRDD(readInput(sc.sc, opts, filename, iFormat))

  /**
    * Loads an input file with the given input format.
    *
    * @param sc  the context to use for loading the file
    * @param opts     the user-provided options that contain the input format details.
    * @param filename the filename (or path) to load
    * @param iFormat  use this input format to load the file and ignore the input format in the given user options.
    * @return an RDD that contains the loaded features
    */
  def readInput(sc: SparkContext, opts: BeastOptions, filename: String, iFormat: String) : SpatialRDD =
   new SpatialFileRDD(sc, filename, new BeastOptions(opts).set(SpatialFileRDD.InputFormat, iFormat))

  /**
   * Converts a SpatialRDD to a dataframe
   * @param sparkSession the Spark session used to create the dataframe
   * @param features the spatial RDD
   * @return the created data frame
   */
  def rddToDataFrame(sparkSession: SparkSession, features: SpatialRDD): DataFrame =
    sparkSession.createDataFrame(features.mapPartitions(_.map(_.asInstanceOf[Row]), preservesPartitioning = true),
      features.first().schema)

  /**
   * Converts a partitioned SpatialRDD to a dataframe
   * @param sparkSession the Spark session used to create the dataframe
   * @param features the spatial RDD
   * @return the created data frame
   */
  def rddToDataFrame2(sparkSession: SparkSession, features: PartitionedSpatialRDD): DataFrame =
    sparkSession.createDataFrame(
      // TODO Keep the partition ID in the generated records
      features.mapPartitions(_.map(_._2.asInstanceOf[Row]), preservesPartitioning = true),
      features.first()._2.schema)

  /**
   * Convert a dataframe that contains a geometry column to a SpatialRDD
   * @param df the dataframe to convert to a SpatialRDD
   * @return a SpatialRDD, i.e., RDD[IFeature]
   */
  def dataFrameToSpatialRDD(df: DataFrame): SpatialRDD = {
    require(df.schema.exists(_.dataType == GeometryDataType), s"The schema ${df.schema} does not contain a geometry column")
    val geometryIndex: Int = df.schema.indexWhere(_.dataType == GeometryDataType)
    df.rdd.map(row => Feature.create(row, row.getAs[Geometry](geometryIndex)))
  }
}
