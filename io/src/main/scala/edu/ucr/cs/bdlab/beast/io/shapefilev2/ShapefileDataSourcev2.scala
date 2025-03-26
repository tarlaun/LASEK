package edu.ucr.cs.bdlab.beast.io.shapefilev2

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.csv.CSVTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A SparkSQL data source for Esri Shapefiles (c). This data source is able to read binary shapefiles along with
 * the associated DBF file for non-spatial data, SHX file to quickly locate records, and optionally a .prj file
 * for the projection.
 * This data source can be used in three ways.
 *  1. If the input points to a single .shp file, it will read that file. The corresponding .dbf and .shx files should
 *   have the same name.
 *  1. If the input points to a single .zip file, all Shapefiles inside that ZIP file will be loaded. For efficiency,
 *   if the ZIP file is not in the local file system, e.g., in HDFS or S3, the file will be automatically copied over
 *   to a local temporary directory to allow random access within the file.
 *  1. If the input path points to a directory, all files in the directory will be processed recursively in the same
 *   way as the first two cases above.
 *  Notice that when more than one .shp file are loaded, they should all match in the schema. If not, the output
 *  of this data source might be incorrect. This is required because Spark assumes that all records in one Dataframe
 *  have a unified schema for efficiency and query optimization.
 */
class ShapefileDataSourcev2 extends FileDataSourceV2 {

  /**A short name to use by users to access this source */
  override def shortName(): String = "shapefile"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ShapefileFormat]

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ShapefileTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ShapefileTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}
