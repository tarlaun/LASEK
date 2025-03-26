package edu.ucr.cs.bdlab.beast.io.geojsonv2

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A SparkSQL data source for GeoJSON files. Works for both well-formed and ill-formed GeoJSON files.
 *  1. Well-formed GeoJSON files are the ones that are available as one big object of type FeatureCollection
 *   which has an array of features. Features are allowed span multiple lines. This file source will still
 *   be able to split the file correctly.
 *  1. Ill-formed GeoJSON files are the ones where each feature is represented in a separate text line with no
 *   line breaks allowed within each feature. In other words, number of lines is equal to number of features.
 *  1. It can also work with a directory that contains a bunch of files which can be a mix of the two above.
 *  1. Input files are also allowed to be compressed using either .gz or .bz2 formats. If compressed using .bz2 format,
 *   the files will be split along the block boundaries to allow parallel decompression and loading.
 */
class GeoJSONSourcev2 extends FileDataSourceV2 {

  /**A short name to use by users to access this source */
  override def shortName(): String = "geojson"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[GeoJSONFormat]

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GeoJSONTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GeoJSONTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}
