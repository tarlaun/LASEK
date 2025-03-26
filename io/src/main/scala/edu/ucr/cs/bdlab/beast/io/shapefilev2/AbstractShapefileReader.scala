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

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

abstract class AbstractShapefileReader(skipSHPFile: Boolean = false, skipDBFFile: Boolean = false)
  extends PartitionReader[InternalRow] with AutoCloseable {

  /** Reader of non-geometric attributes */
  def dbfReader: DBFReader

  /** Reader of geometric attribute */
  def shpReader: ShapefileGeometryReader

  /** Number of records remaining in the current file. Only used when both SHP and DBF files are skipped */
  var remainingRecordsInFile: Long = -1

  /** A flag that is raised when the end of file is reached */
  def reachedEOF: Boolean = {
    while (true) {
      val eof: Boolean = if (skipSHPFile && skipDBFFile && remainingRecordsInFile <= 0)
        true
      else if (!skipSHPFile && (shpReader == null || !shpReader.hasNext))
        true
      else if (!skipDBFFile && (dbfReader == null || !dbfReader.hasNext))
        true
      else
        false
      if (!eof)
        return false
      if (eof && !moveToNextFile)
        return true
    }
    false
  }

  /** The last record that has been read or null if next() was never called */
  var currentRecord: InternalRow = InternalRow.empty

  def moveToNextFile: Boolean

  moveToNextFile

  override def next(): Boolean = {
    if (reachedEOF)
      return false
    if (skipSHPFile && skipDBFFile) {
      // Return an empty record
      assert(remainingRecordsInFile > 0)
      remainingRecordsInFile -= 1
      return true
    }
    val geometry: Geometry = if (skipSHPFile) null else {
      assert(shpReader.hasNext)
      shpReader.next()
    }
    val nonGeomAttributes: Array[Any] = if (skipDBFFile) Array() else {
      while (shpReader != null && dbfReader.iNextRecord < shpReader.iCurrentRecord)
        dbfReader.next()
      assert(dbfReader.hasNext)
      dbfReader.next().map {
        case s: String => UTF8String.fromString(s)
        case x => x
      }
    }
    val allValues: Array[Any] = (if (geometry == null) null else GeometryDataType.setGeometryInRow(geometry)) +: nonGeomAttributes
    currentRecord = InternalRow.apply(allValues: _*)
    true
  }

  override def get(): InternalRow = currentRecord
}
