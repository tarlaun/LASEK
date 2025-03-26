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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.io.shapefile.DBFConstants
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.DataInputStream
import java.util.GregorianCalendar

@RunWith(classOf[JUnitRunner])
class DBFReaderTest extends AnyFunSuite with BeastSpatialTest {
  test("Read header of DBF file") {
    val in = new DataInputStream(getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.dbf"))
    val reader = new DBFReader(in)
    try {
      assertResult(4)(reader.header.fieldDescriptors.length)
      assertResult(120)(reader.header.numRecords)
      var recordCount: Int = 0
      for (record <- reader) {
        assertResult(4)(record.length)
        assert(record(3) != null)
        recordCount += 1
      }
      assertResult(120)(recordCount)
    } finally {
      reader.close()
    }
  }

  test("Parse fields") {
    val dbfFiles = Array("/usa-major-cities/usa-major-cities.dbf",
      "/usa-major-highways/usa-major-highways.dbf",
      "/simple-with-dates.dbf")
    for (dbfFile <- dbfFiles) {
      val reader = new DBFReader(new DataInputStream(getClass.getResourceAsStream(dbfFile)))
      try {
        for (record <- reader) {
          for (attribute <- record)
            assert(attribute != null)
        }
      } finally {
        reader.close()
      }
    }
  }

  test("Parse dates") {
    val in = new DataInputStream(getClass.getResourceAsStream("/simple-with-dates.dbf"))
    val reader = new DBFReader(in)
    try {
      assert(reader.hasNext, "Expected to find records in the file")
      val record = reader.next()
      val expected = new GregorianCalendar(DBFConstants.UTC)
      expected.clear()
      expected.set(2018, 12 - 1, 26)
      val expectedDate = new java.sql.Date(expected.getTimeInMillis)
      assertResult(expectedDate)(record(1))
    } finally {
      reader.close()
    }
  }

  test("Should not return null on empty strings") {
    val in = new DataInputStream(getClass.getResourceAsStream("/file_with_empty_numbers.dbf"))
    val reader = new DBFReader(in)
    try {
      reader.size
      // No assertions needed. Just make sure that no errors are raised
    } finally {
      reader.close()
    }
  }

  test("Should ignore trailing zeros in strings") {
    val in = new DataInputStream(getClass.getResourceAsStream("/nullstrings.dbf"))
    val reader = new DBFReader(in)
    try {
      assert(reader.hasNext)
      val record = reader.next()
      assertResult(null)(record(1))
      assert(reader.hasNext)
      val record2 = reader.next()
      assertResult("Hello")(record2(1))
    } finally {
      reader.close()
    }
  }
}
