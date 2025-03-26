package edu.ucr.cs.bdlab.beast.io

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An object that contains a set of functions for loading CSV and text-delimited spatial files.
 */
object SpatialCSVSource {
  /** The type of geometry in the text file {point,envelope,wkt} */
  val GeometryType: String = "geometry_type"

  /** The columns that contain the dimensions separate by comma. Columns can be referred by name or position. */
  val DimensionColumns: String = "dimensions"

  /** Set to true to read envelopes as JTS polygons. Set to false (default) to read envelopes as GeoLite Envelopes */
  val JTSEnvelope: String = "jtsenvelope"

  /**
   * Read data from the given path according to the given options
   *
   * @param spark   the [[SparkSession]] used to load the input
   * @param path    the path that contains the input
   * @param options additional options for parsing spatial data
   * @return the spatial DataFrame
   */
  def read(spark: SparkSession, path: String, options: java.util.Map[String, String]): DataFrame = {
    val unparsedDataframe: DataFrame = spark.read.options(options).csv(path)
    parse(unparsedDataframe, options)
  }

  import scala.collection.JavaConverters._

  /**
   * Read data from the given path according to the given options
   *
   * @param spark   the [[SparkSession]] used to load the input
   * @param path    the path that contains the input
   * @param options additional options for parsing spatial data
   * @return the spatial DataFrame
   */
  def read(spark: SparkSession, path: String, options: Map[String, Any]): DataFrame =
    read(spark, path, options.map(kv => (kv._1, kv._2.toString)).asJava)

  /**
   * Parses an existing DataFrame according to the given options that determine the format of the spatial attributes.
   * The geometry type is provided in the option "geometryType" and can be one of the following:
   *
   *  1. "point": Indicates that the input contains separate columns for the point coordinates.
   *    The columns for the dimensions in order are provided in the option "dimensions". They can be either column
   *    indices (integer) or column names.
   *  1. "wkt": The geometry is encoded in WKT format in the column provided by the "dimensions" attribute.
   *  1. "envelope": An n-dimensional orthogonal rectangle with the dimensions provided by the "dimensions" attribute.
   *
   * @param dataframe an existing dataframe
   * @param options   options to locate and parse the geometry attributes.
   * @return a dataframe that parses and replaces the spatial attributes with a geometry column
   */
  def parse(dataframe: DataFrame, options: java.util.Map[String, String]): DataFrame = {
    val geometryType: String = options.get(GeometryType).toLowerCase
    // Make
    var dimensionColumns: Array[String] = options.getOrDefault(DimensionColumns, "0").split(",")
    val inputSchema = dataframe.schema
    dimensionColumns = dimensionColumns.map(n =>
      if (inputSchema.exists(_.name == n))
        n
      else
        inputSchema(n.toInt).name
    )
    geometryType match {
      case "point" =>
        val numDimensions: Int = dimensionColumns.length
        require(numDimensions >= 2, s"Invalid dimensions for points '${options.get(DimensionColumns)}''")
        val outputColumns = new Array[String](inputSchema.length - numDimensions + 1)
        var iCol = 0
        var geometryProduced = false
        for (f <- inputSchema) {
          if (!dimensionColumns.contains(f.name)) {
            outputColumns(iCol) = s"`${f.name}`"
            iCol += 1
          } else if (!geometryProduced) {
            outputColumns(iCol) = s"ST_CreatePoint(${dimensionColumns.mkString(",")}) AS point"
            iCol += 1
            geometryProduced = true
          }
        }
        assert(iCol == outputColumns.length)
        dataframe.selectExpr(outputColumns: _*)
      case "wkt" =>
        require(dimensionColumns.length == 1, s"Invalid dimension columns for WKT '${options.get(DimensionColumns)}'")
        val wktColumn = dimensionColumns.head
        val outputColumns: Array[String] = inputSchema.map(f => f.name).toArray
        val iCol: Int = outputColumns.indexOf(wktColumn)
        outputColumns(iCol) = s"ST_FromWKT($wktColumn) AS `$wktColumn`"
        dataframe.selectExpr(outputColumns: _*)
      case "envelope" =>
        val jtsEnvelopes = options.getOrDefault(JTSEnvelope, "false").equals("true")
        val numDimensions: Int = dimensionColumns.length
        require(numDimensions > 0 && numDimensions % 2 == 0,
          s"Invalid dimension columns for envelope '${options.get(DimensionColumns)}'")
        require(!jtsEnvelopes || numDimensions == 4,
          s"JTSEnvelopes can only be created for two-dimensional envelopes but received ${numDimensions} dimensions")
        val functionName = if (jtsEnvelopes) "ST_CreateBox" else "ST_CreateEnvelopeN"
        val outputColumns = new Array[String](inputSchema.length - numDimensions + 1)
        var iCol = 0
        var geometryProduced = false
        for (f <- inputSchema) {
          if (!dimensionColumns.contains(f.name)) {
            outputColumns(iCol) = s"`${f.name}`"
            iCol += 1
          } else if (!geometryProduced) {
            outputColumns(iCol) = s"${functionName}(${dimensionColumns.map(x => s"CAST(${x} AS Double)").mkString(",")}) AS envelope"
            iCol += 1
            geometryProduced = true
          }
        }
        assert(iCol == outputColumns.length)
        dataframe.selectExpr(outputColumns: _*)
      case other => throw new RuntimeException(s"Unrecognized geometry type '${other}'")
    }
  }
}
