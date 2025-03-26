/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.util.FileUtil
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

/**
 * Write a Shapefile that consists of four files, .shp, .shx, .prj, and .dbf
 * @param path the path of the .shp file. Other files will be written in the same directory
 * @param dataSchema the schema of output records
 * @param context the task context
 */
class ShapefileWriter(val path: String, dataSchema: StructType, context: TaskAttemptContext)
  extends OutputWriter with Logging {

  // Write all files to a temporary directory and then move to the output directory
  var tempDir: File = _

  /** Writer for the geometry attribute */
  var shpWriter: ShapefileGeometryWriter = _

  /** Writer for non-geometric attributes */
  var dbfWriter: DBFWriter = _

  /** The index of the geometric attribute */
  val iGeom: Int = dataSchema.indexWhere(_.dataType == GeometryDataType)

  /** The index of all non-geometric attributes */
  val iNonGeom: Array[Int] = (0 until dataSchema.length).filterNot(_ == iGeom).toArray

  initialize()

  /** Initialize the writing */
  def initialize(): Unit = {
    val filename = new File(path).getName
    tempDir = File.createTempFile(context.getTaskAttemptID.toString, filename+".tmp")
    tempDir.delete()
    tempDir.mkdirs()
    shpWriter = new ShapefileGeometryWriter(new File(tempDir, FileUtil.replaceExtension(filename, ".shp")))
    dbfWriter = new DBFWriter(
      new BufferedOutputStream(new FileOutputStream(new File(tempDir, FileUtil.replaceExtension(filename, ".dbf")))),
      StructType(dataSchema.filterNot(_.dataType == GeometryDataType)))
  }

  override def write(row: InternalRow): Unit = {
    val geom = GeometryDataType.getGeometryFromRow(row, iGeom)
    shpWriter.write(geom)
    val nonGeomAttributes: Array[Any] = iNonGeom.map(i => row.get(i, dataSchema(i).dataType))
    dbfWriter.write(nonGeomAttributes)
  }

  override def close(): Unit = {
    shpWriter.close()
    dbfWriter.close()
    // Now, move the temporary files to the output path
    val outPath = new Path(path)
    val fileSystem = outPath.getFileSystem(context.getConfiguration)
    val finalOutput = new ZipOutputStream(fileSystem.create(outPath))
    for (tempFile <- tempDir.listFiles()) {
      finalOutput.putNextEntry(new ZipEntry(tempFile.getName))
      val tempIn = new FileInputStream(tempFile)
      IOUtils.copyLarge(tempIn, finalOutput)
      tempIn.close()
      tempFile.delete()
    }
    finalOutput.close()
    assert(tempDir.list().isEmpty, s"Temp directory should be empty but contained ${tempDir.list().mkString(",")}")
    if (!tempDir.delete())
      logWarning(s"Could not delete temporary directory '${tempDir}'")
  }
}
