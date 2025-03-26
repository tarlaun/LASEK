package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.io.geojsonv2.GeoJSONSourcev2
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A reader for JSON files that contain a WKT geometry as a field.
 * Notice that this is different from GeoJSON formats where the geometry is represented as a nested object.
 * Check [[GeoJSONSourcev2]] and [[GeoJSONFeatureReader]]
 */
object JSONWKTSource {

  /** The name of the JSON attribute that contains the WKT field */
  val WKTAttribute: String = "wkt_attribute"

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
    val csvOptions = new java.util.HashMap[String, String]()
    csvOptions.putAll(options)
    val wktAttr = csvOptions.remove(WKTAttribute)
    csvOptions.put(SpatialCSVSource.GeometryType, "wkt")
    csvOptions.put(SpatialCSVSource.DimensionColumns, wktAttr)
    SpatialCSVSource.parse(unparsedDataframe, csvOptions)
    unparsedDataframe
  }

}
