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

import edu.ucr.cs.bdlab.beast.io.SpatialFilePartition2
import edu.ucr.cs.bdlab.beast.util.FileUtil
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.beast.CRSServer
import org.apache.spark.sql.catalyst.InternalRow
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope

import java.io.{DataInputStream, InputStream}

/**
 * Reads the rows of all the given Shapefile and related .dbf, .prj, or .shx files
 * @param conf the configuration that can be used to create [[org.apache.hadoop.fs.FileSystem]]
 * @param file the file to read
 */
class ShapefileReader(conf: Configuration, file: SpatialFilePartition2,
                      filter: Envelope = null, skipSHPFile: Boolean = false, skipDBFFile: Boolean = false)
  extends AbstractShapefileReader(skipSHPFile, skipDBFFile) {

  /** Reads DBF entries */
  var dbfReader: DBFReader = _

  /** Reads geometries from the Shapefile */
  var shpReader: ShapefileGeometryReader = _

  assert(FileUtil.getExtension(file.filePath).toLowerCase == ".shp", s"Expected a .shp file but found '${file.filePath}'")

  /** A flag that is raised after the file has been initialized */
  def initialized: Boolean = shpReader != null || dbfReader != null || remainingRecordsInFile != -1

  def moveToNextFile: Boolean = {
    if (initialized) // No next file
      return false
    val path: Path = new Path(file.filePath)
    val dir = path.getParent
    val fileSystem: FileSystem = path.getFileSystem(conf)
    // Find the corresponding files
    val dirContents: Array[FileStatus] = fileSystem.listStatus(dir)
    def locateFile(extension: String): FileStatus = {
      val filename: String = FileUtil.replaceExtension(path.getName, extension)
      val iEntry = dirContents.indexWhere(_.getPath.getName.equalsIgnoreCase(filename))
      if (iEntry == -1) null else dirContents(iEntry)
    }
    val dbfFile: FileStatus = locateFile(".dbf")
    val shxFile: FileStatus = locateFile(".shx")
    val prjFile: FileStatus = locateFile(".prj")
    if (skipSHPFile && skipDBFFile) {
      // Skip all attributes. All we need is the number of records in the file
      remainingRecordsInFile = if (file.numFeatures >= 0) {
        file.numFeatures
      } else if (shxFile != null) {
        // Determine the number of records from the SHX file
        // SHX file size = 100 (header) + 8 * numRecords
        // numRecords = (SHX file size - 100) / 8
        (shxFile.getLen - 100) / 8
      } else if (dbfFile != null) {
        // Determine number of entries from the header of the DBF file
        val dbfIn: DataInputStream = fileSystem.open(dbfFile.getPath)
        try {
          val dbfReader = new DBFReader(dbfIn)
          dbfReader.header.numRecords
        } finally {
          dbfIn.close()
        }
      } else {
        // Determine number of entries by reading the entire SHP file
        // Note: The header does not contain num of records and records are variable size
        val shpIn: DataInputStream = fileSystem.open(path)
        try {
          shpReader = new ShapefileGeometryReader(shpIn, 0)
          shpReader.size
        } finally {
          shpIn.close()
        }
      }
    }
    if (dbfFile != null && !skipDBFFile) {
      dbfReader = new DBFReader(fileSystem.open(dbfFile.getPath))
    }
    if (!skipSHPFile) {
      val srid: Int = if (prjFile != null) {
        // Read prj file to determine the CRS of geometries
        val prjIn: InputStream = fileSystem.open(prjFile.getPath)
        val prjData = IOUtils.toByteArray(prjIn)
        prjIn.close()
        val crsWKT = new String(prjData)
        val crs = CRS.parseWKT(crsWKT)
        CRSServer.crsToSRID(crs)
      } else {
        4326
      }
      val recordOffsets: Array[Int] = if (shxFile != null) {
        // Read .shx file
        val shxIn: DataInputStream = fileSystem.open(shxFile.getPath)
        try {
          ShapefileHelper.readSHXFile(shxIn)
        } finally {
          shxIn.close()
        }
      } else {
        null
      }
      val shpIn: DataInputStream = new DataInputStream(fileSystem.open(path))
      shpReader = if (skipSHPFile) null else new ShapefileGeometryReader(shpIn, srid = srid, offsets = recordOffsets, filter)
    }
    true
  }

  override def get(): InternalRow = currentRecord

  override def close(): Unit = {
    if (shpReader != null) {
      shpReader.close()
      shpReader = null
    }
    if (dbfReader != null) {
      dbfReader.close()
      dbfReader = null
    }
  }
}
