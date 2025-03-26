package edu.ucr.cs.bdlab.beast.io.kmlv2

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A SparkSQL data source for KMZ files. This data source (actually data sink) can only support writing
 * and not reading. The output is a KMZ file which is a ZIP file that contains a KML file
 */
class KMZSourcev2 extends FileDataSourceV2 {

  /**A short name to use by users to access this source */
  override def shortName(): String = "kmz"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[KMZFormat]

  override def getTable(options: CaseInsensitiveStringMap): Table = ???

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = ???
}
