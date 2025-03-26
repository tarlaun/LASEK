package edu.ucr.cs.bdlab.beast.io

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A set of functions for reading and writing GeoParquet and SpatialParquet files
 */
object SpatialParquetSource {

  /** The column that contains the geometry object when reading GeoParquet or SpatialParquet */
  val GeometryColumn: String = "geometry_column"

  /**
   * Read data from the given path according to the given options
   *
   * @param spark   the [[SparkSession]] used to load the input
   * @param path    the path that contains the input
   * @param options additional options for parsing spatial data
   * @return the spatial DataFrame
   */
  def readSpatialParquet(spark: SparkSession, path: String, options: java.util.Map[String, String]): DataFrame = {
    val unparsedDataframe: DataFrame = spark.read.options(options).parquet(path)
    val geometryColumn = options.get(GeometryColumn)
    decodeSpatialParquet(unparsedDataframe, if (geometryColumn != null) geometryColumn else "geometry")
  }

  /**
   * Read a parquet file stored in the GeoParquet format.
   * @param spark the spark session used to read the file
   * @param path the path of the GeoParquet file
   * @param options additional options for reading the file including the "geometry-column" attribute.
   * @return
   */
  def readGeoParquet(spark: SparkSession, path: String, options: java.util.Map[String, String]): DataFrame = {
    val unparsedDataframe: DataFrame = spark.read.options(options).parquet(path)
    val geometryColumn = options.get(GeometryColumn)
    decodeGeoParquet(unparsedDataframe, if (geometryColumn != null) geometryColumn else "geometry")
  }

  /**
   * Write a [[DataFrame]] in the SpatialParquet format.
   * @param dataFrame the data frame to write to disk
   * @param path the path to write the file to
   * @param options additional options for writing the file
   */
  def writeSpatialParquet(dataFrame: DataFrame, path: String, options: java.util.Map[String, String]): Unit = {
    encodeSpatialParquet(dataFrame)
      .write.options(options).parquet(path)
  }

  /**
   * Write a [[DataFrame]] in the GeoParquet format.
   * @param dataFrame the data frame to write to disk
   * @param path the path to write the file to
   * @param options additional options for writing the file
   */
  def writeGeoParquet(dataFrame: DataFrame, path: String, options: java.util.Map[String, String]): Unit = {
    encodeGeoParquet(dataFrame)
      .write.options(options).parquet(path)
  }
  /**
   * Decodes a [[DataFrame]] that was encoded using the SpatialParquet format
   * @param dataframe
   * @param geomColumnName
   * @return
   */
  def decodeSpatialParquet(dataframe: DataFrame, geomColumnName: String): DataFrame = {
    dataframe.selectExpr("*", s"ST_FromSpatialParquet($geomColumnName) AS ${geomColumnName}_decoded")
      .drop(geomColumnName)
      .withColumnRenamed(s"${geomColumnName}_decoded", geomColumnName)
  }

  /**
   * Parses an existing DataFrame according to the given options that determine the format of the spatial attributes.
   *
   * @param dataframe an existing dataframe
   * @return a dataframe that parses and replaces the spatial attributes with a geometry column
   */
  def encodeSpatialParquet(dataframe: DataFrame): DataFrame = {
    val geomColumn: Option[StructField] = dataframe.schema.find(_.dataType == GeometryDataType)
    if (geomColumn.isEmpty) {
      dataframe
    } else {
      val geomColumnName = geomColumn.get.name
      dataframe.selectExpr("*", s"ST_ToSpatialParquet($geomColumnName) AS ${geomColumnName}_encoded")
        .drop(geomColumnName)
        .withColumnRenamed(s"${geomColumnName}_encoded", geomColumnName)
    }
  }

  /**
   * Encode the given DataFrame into GeoParquet format by replacing the geometry column with two new columns,
   * MBR and the WKB representation of the geometry.
   * @param dataframe
   * @return
   */
  def encodeGeoParquet(dataframe: DataFrame): DataFrame = {
    val geomColumn: Option[StructField] = dataframe.schema.find(_.dataType == GeometryDataType)
    if (geomColumn.isEmpty)
      dataframe
    else {
      val geomColumnName = geomColumn.get.name
      dataframe.selectExpr("*",
        s"ST_XMin($geomColumnName) AS ${geomColumnName}_minx",
        s"ST_YMin($geomColumnName) AS ${geomColumnName}_miny",
        s"ST_XMax($geomColumnName) AS ${geomColumnName}_maxx",
        s"ST_YMax($geomColumnName) AS ${geomColumnName}_maxy",
        s"ST_ToWKB($geomColumnName) AS ${geomColumnName}_encoded")
        .drop(geomColumnName)
        .withColumnRenamed(s"${geomColumnName}_encoded", geomColumnName)
    }
  }

  def decodeGeoParquet(dataframe: DataFrame, geomColumnName: String): DataFrame = {
    dataframe.selectExpr("*", s"ST_FromWKB($geomColumnName) AS ${geomColumnName}_decoded")
      .drop(geomColumnName)
      .withColumnRenamed(s"${geomColumnName}_decoded", geomColumnName)
  }
}
