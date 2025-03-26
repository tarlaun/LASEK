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
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope

import java.io.{DataInputStream, File, InputStream}
import java.util.zip.{ZipEntry, ZipFile}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Reads the rows of all the Shapefiles contained in the given .zip file.
 * @param conf the configuration that can be used to create [[org.apache.hadoop.fs.FileSystem]]
 * @param file the file to read
 */
class CompressedShapefileReader(conf: Configuration, file: SpatialFilePartition2,
                                filter: Envelope = null, skipSHPFile: Boolean = false, skipDBFFile: Boolean = false)
  extends AbstractShapefileReader(skipSHPFile, skipDBFFile) with Logging {
  /** The reader of the underlying ZIP file. Set to null when the end-of-file is reached */
  var zipInput: ZipFile = _

  /** If the ZIP file needs to be copied locally, this will point to this temp file */
  var tempZipFile: File = _

  /** A list of all entries in the ZIP file if the input is compressed */
  var zipEntries: scala.collection.mutable.ArrayBuffer[ZipEntry] = _

  /** Reads DBF entries */
  var dbfReader: DBFReader = _

  /** Reads geometries from the Shapefile */
  var shpReader: ShapefileGeometryReader = _

  assert(FileUtil.getExtension(file.filePath).toLowerCase == ".zip",
    s"Expected a .zip file but found '${file.filePath}'")

  /** Move to the next Shapefile and return true if a file is opened. Otherwise, return false */
  def moveToNextFile: Boolean = {
    if (zipEntries == null)
      initializeZipFile()
    if (zipEntries.isEmpty)
      return false
    moveToNextZipEntry()
  }

  def initializeZipFile(): Unit = {
    // Initialize reader to read all .shp files inside this zip file
    // For efficiency, we need the file to be in the local file system for random read
    val path = new Path(file.filePath)
    val fileSystem: FileSystem = path.getFileSystem(conf)
    if (fileSystem.isInstanceOf[LocalFileSystem]) {
      // The file is local, read it directly
      zipInput = new ZipFile(new Path(file.filePath).toUri.getPath)
    } else {
      // The file is in another file system. Make a local copy to be able to use ZipFile
      tempZipFile = File.createTempFile("temp-read", path.getName)
      tempZipFile.delete()
      fileSystem.copyToLocalFile(false, path, new Path(tempZipFile.getPath), true)
      zipInput = new ZipFile(tempZipFile)
    }
    zipEntries = new ArrayBuffer[ZipEntry]()
    val acceptedExtensions = Array(".shp", ".shx", ".dbf", ".prj")
    zipEntries ++= zipInput.entries().asScala
      // Keep only relevant file extensions
      .filter(z => acceptedExtensions.contains(FileUtil.getExtension(z.getName).toLowerCase()))
      // Remove hidden files
      .filterNot(z => z.getName.startsWith("_") || z.getName.startsWith("."))
  }

  /**
   * If the input is compressed, move to the next ZIP entry and delete it from the list of
   * @return
   */
  private def moveToNextZipEntry(): Boolean = {
    var iEntry: Int = zipEntries.indexWhere(z => FileUtil.getExtension(z.getName).toLowerCase() == ".shp")
    if (iEntry == -1)
      return false
    val shpEntry: ZipEntry = zipEntries.remove(iEntry)
    def locateFile(extension: String): ZipEntry = {
      val filename: String = FileUtil.replaceExtension(shpEntry.getName, extension)
      iEntry = zipEntries.indexWhere(_.getName.equalsIgnoreCase(filename))
      if (iEntry == -1) null else zipEntries.remove(iEntry)
    }
    // Locate the corresponding .shx, .dbf, and .prj entries
    val shxEntry: ZipEntry = locateFile(".shx")
    val dbfEntry: ZipEntry = locateFile(".dbf")
    val prjEntry: ZipEntry = locateFile(".prj")
    if (skipSHPFile && skipDBFFile) {
      // Skip all attributes. All we need is the number of records in the file
      remainingRecordsInFile = if (file.numFeatures >= 0) {
        logInfo("Using number of records from the master file")
        file.numFeatures
      } else if (shxEntry != null) {
        logInfo("Determining number of records form the .shx file")
        // Determine the number of records from the SHX file
        val shxIn: DataInputStream = new DataInputStream(zipInput.getInputStream(shxEntry))
        try {
          ShapefileHelper.numOfRecordsFromSHXFile(shxIn)
        } finally {
          shxIn.close()
        }
      } else if (dbfEntry != null) {
        logInfo("Determining number of records form the .dbf file")
        // Determine number of entries from the header of the DBF file
        val dbfIn: DataInputStream = new DataInputStream(zipInput.getInputStream(dbfEntry))
        try {
          val dbfReader = new DBFReader(dbfIn)
          dbfReader.header.numRecords
        } finally {
          dbfIn.close()
        }
      } else {
        logInfo("Determining number of records form the .shp file")
        assert(shpEntry != null)
        // Determine number of entries by reading the entire SHP file
        val shpIn: DataInputStream = new DataInputStream(zipInput.getInputStream(shpEntry))
        try {
          shpReader = new ShapefileGeometryReader(shpIn, 0)
          shpReader.size
        } finally {
          shpIn.close()
        }
      }
    } else {
      if (skipDBFFile || dbfEntry == null) {
        dbfReader = null
      } else {
        // Read the corresponding DBF file for non-geometric attributes
        val dbfIn: DataInputStream = new DataInputStream(zipInput.getInputStream(dbfEntry))
        dbfReader = new DBFReader(dbfIn)
      }
      if (skipSHPFile || shpEntry == null) {
        shpReader = null
      } else {
        // Read the prj file and get the corresponding SRID to use when creating geometries
        val srid = if (prjEntry != null) {
          val prjIn: InputStream = zipInput.getInputStream(prjEntry)
          val prjData = IOUtils.toByteArray(prjIn)
          prjIn.close()
          val crsWKT = new String(prjData)
          val crs = CRS.parseWKT(crsWKT)
          CRSServer.crsToSRID(crs)
        } else {
          // If no .prj file exists, use the default SRID of 4326
          4326
        }
        // Read all entries in the .shx file
        // In some files, there could be gaps between geometries and only the .shx file will contain this information
        val recordOffsets: Array[Int] = if (shxEntry != null) {
          // Extract record locations from .shx file in case there are gaps in the .shp file
          val shxIn: DataInputStream = new DataInputStream(zipInput.getInputStream(shxEntry))
          try {
            ShapefileHelper.readSHXFile(shxIn)
          } finally {
            shxIn.close()
          }
        } else {
          null
        }
        val shpIn: DataInputStream = new DataInputStream(zipInput.getInputStream(shpEntry))
        shpReader = new ShapefileGeometryReader(shpIn, srid = srid, offsets = recordOffsets, filter)
      }
    }
    true
  }

  override def close(): Unit = {
    if (shpReader != null) {
      shpReader.close()
      shpReader = null
    }
    if (dbfReader != null) {
      dbfReader.close()
      dbfReader = null
    }
    if (zipInput != null) {
      zipInput.close()
      zipInput = null
    }
    if (tempZipFile != null) {
      tempZipFile.delete()
      tempZipFile = null
    }
  }
}
