/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.gpxv2

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.spark.beast.sql.GeometryDataType
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.Calendar

@RunWith(classOf[JUnitRunner])
class GPXReader2Test extends AnyFunSuite with BeastSpatialTest {

  test("ReadSample") {
    val input = getClass.getResourceAsStream("/001005279.gpx")
    val gpxReader = new GPXReader2(input, "001005279.gpx")
    try {
      var count = 0
      val schema = GPXReader2.schema
      for (r <- gpxReader) {
        count += 1
        if (count == 1) {
          // Check first record
          assertResult(8)(r.numFields)
          assertResult(188.92)(r.getDouble(schema.fieldIndex("elevation")))
          assertResult("001005279.gpx")(r.getString(schema.fieldIndex("filename")))
          assertResult(0)(r.getInt(schema.fieldIndex("tracknumber")))
          assertResult("Track 0")(r.getString(schema.fieldIndex("trackname")))
          assertResult(0)(r.getInt(schema.fieldIndex("segmentnumber")))
          val time = r.get(schema.fieldIndex("time"), schema(schema.fieldIndex("time")).dataType).asInstanceOf[java.sql.Timestamp]
          val calendar = Calendar.getInstance()
          calendar.setTime(time)
          assertResult(8)(calendar.get(Calendar.DAY_OF_MONTH))
          assertResult(2011)(calendar.get(Calendar.YEAR))
          assertResult(20)(calendar.get(Calendar.SECOND))
        } else if (count == 23) {
          // Last record should not have time but it should still have seven attributes (time will be null)
          assertResult(8)(r.numFields)
          assert(r.isNullAt(schema.fieldIndex("time")))
        }
      }
      assertResult(23)(count)
    } finally {
      gpxReader.close()
    }
  }

  test("ReadMultitrackOneSplit") {
    val input = getClass.getResourceAsStream("/multitrack.gpx")
    val gpxReader = new GPXReader2(input, "multitrack.gpx")
    try {
      val schema = GPXReader2.schema
      var pointCount = 0
      var lastTrackNumber = -1
      var trackCount = 0
      var lastSegmentNumber = -1
      var segmentCount = 0
      for (r <- gpxReader) {
        pointCount += 1
        assertResult("Point")(GeometryDataType.getGeometryFromRow(r, 0).getGeometryType)
        val tracknumber = r.getInt(schema.fieldIndex("tracknumber"))
        if (tracknumber != lastTrackNumber) {
          trackCount += 1
          lastTrackNumber = tracknumber
        }
        val segmentnumber = r.getInt(schema.fieldIndex("segmentnumber"))
        if (segmentnumber != lastSegmentNumber) {
          segmentCount += 1
          lastSegmentNumber = segmentnumber
        }
      }
      assertResult(23)(pointCount)
      assertResult(2)(trackCount)
      assertResult(3)(segmentCount)
    } finally {
      gpxReader.close()
    }
  }
}
