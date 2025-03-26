package edu.ucr.cs.bdlab.beast.io.kmlv2

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A SparkSQL data source for KML/KMZ files. This data source (actually data sink) can only support writing
 * and not reading. It can write files in both KML and KMZ formats depending on the output extension of the file.
 */
class KMLSourcev2 extends FileDataSourceV2 {

  /**A short name to use by users to access this source */
  override def shortName(): String = "kml"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[KMLFormat]

  override def getTable(options: CaseInsensitiveStringMap): Table = ???

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = ???
}
