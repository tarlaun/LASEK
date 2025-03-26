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

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.{Calendar, GregorianCalendar}

@RunWith(classOf[JUnitRunner])
class DBFWriterTest extends AnyFunSuite with BeastSpatialTest {
  test("simple writer") {
    // Read a file and write it back
    val dbfIn = new DataInputStream(getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.dbf"))
    val dbfFile = new DBFReader(dbfIn)
    val dbfOut = new ByteArrayOutputStream()
    val dbfWriter = new DBFWriter(dbfOut, dbfFile.schema)
    for (record <- dbfFile) {
      dbfWriter.write(record)
    }
    dbfWriter.close()
    dbfIn.close()

    // Read the file back and compare record-by-record
    val expectedOut = new DBFReader(new DataInputStream(getClass.getResourceAsStream("/usa-major-cities/usa-major-cities.dbf")))
    val actualOutput = new DBFReader(new DataInputStream(new ByteArrayInputStream(dbfOut.toByteArray)))
    try {
      // println(expectedOut.schema)
      // println(actualOutput.schema)
      var numRecords: Int = 0
      while (expectedOut.hasNext && actualOutput.hasNext) {
        val expectedRecord = expectedOut.next()
        val actualRecord = actualOutput.next()
        assertResult(actualRecord)(expectedRecord)
        numRecords += 1
      }
      assert(!expectedOut.hasNext, s"Read only $numRecords records but expected to read more")
      assert(!actualOutput.hasNext, s"Read all $numRecords records but some records are left")
    } finally {
      expectedOut.close()
      actualOutput.close()
    }

  }

  test("Write all attribute types") {
    // Create a DBF file with one feature that has all the possible types
    val date = new GregorianCalendar(DBFHelper.UTC)
    date.clear()
    date.set(2019, Calendar.OCTOBER, 9)
    val dateValue = new java.sql.Date(date.getTimeInMillis)
    val tags = Map("tag" -> "value")
    val schema = StructType(Seq(
      StructField("key1", BooleanType),
      StructField("key2", IntegerType),
      StructField("key3", ByteType),
      StructField("key4", ShortType),
      StructField("key5", LongType),
      StructField("key6", FloatType),
      StructField("key7", DoubleType),
      StructField("key8", StringType),
      StructField("key9", DateType),
      StructField("key10", MapType.apply(StringType, StringType)),
    ))
    val r: Array[Any] = Array[Any](true, 123, 12.toByte, 13.toShort, 6546543.toLong, 12.25f, 12.655847, "value9", dateValue, tags)
    val dbfOut = new ByteArrayOutputStream()
    val dbfWriter = new DBFWriter(dbfOut, schema)
    try {
      dbfWriter.write(r)
    } finally try dbfWriter.close()
    catch {
      case e: Exception => e.printStackTrace()
    }

    // Read the written file back and make sure it is similar to the original one
    val dbfReader = new DBFReader(new DataInputStream(new ByteArrayInputStream(dbfOut.toByteArray)))
    assert(dbfReader.hasNext)
    val r2 = dbfReader.next()
    for (i <- r.indices) {
      assert(r(i) == r2(i) || r(i).toString == r2(i).toString, s"Features attribute #$i should be equal")
    }
    assert(!dbfReader.hasNext, "The output should not have more than one record")
    dbfReader.close()
  }

  test("Write negative integers") {
    // Create a DBF file with one feature that has all the possible types
    val schema = StructType(Seq(
      StructField("key1", IntegerType),
      StructField("key2", LongType),
      StructField("key3", LongType),
    ))
    val r: Array[Any] = Array[Any](-123, Int.MinValue.toLong, Long.MaxValue)
    val dbfOut = new ByteArrayOutputStream()
    val dbfWriter = new DBFWriter(dbfOut, schema)
    try {
      dbfWriter.write(r)
    } finally try dbfWriter.close()
    catch {
      case e: Exception => e.printStackTrace()
    }

    // Read the written file back and make sure it is similar to the original one
    val dbfReader = new DBFReader(new DataInputStream(new ByteArrayInputStream(dbfOut.toByteArray)))
    assert(dbfReader.hasNext)
    val r2 = dbfReader.next()
    for (i <- r.indices) {
      assert(r(i) == r2(i) || r(i).toString == r2(i).toString, s"Features attribute #$i should be equal")
    }
    assert(!dbfReader.hasNext, "The output should not have more than one record")
    dbfReader.close()
  }

  test("Write null fields with known schema") {
    val schema = StructType(Seq(
      StructField("key1", BooleanType),
      StructField("key2", IntegerType),
      StructField("key3", LongType),
      StructField("key4", DoubleType),
      StructField("key5", StringType),
      StructField("key6", TimestampType),
    ))
    val dbfOut = new ByteArrayOutputStream()
    val dbfWriter = new DBFWriter(dbfOut, schema)
    val r = Array[Any](null, null, null, null, null, null)
    try {
      dbfWriter.write(r)
    } finally try dbfWriter.close()
    catch {
      case e: Exception => e.printStackTrace()
    }

    // Read it back
    val reader = new DBFReader(new DataInputStream(new ByteArrayInputStream(dbfOut.toByteArray)))
    assert(reader.hasNext, "The output should have one record")
    val r2 = reader.next()
    assert(r2.forall(_ == null), "All values should be null")
    val writtenSchema = reader.schema
    assertResult(BooleanType)(writtenSchema(0).dataType)
    assertResult(IntegerType)(writtenSchema(1).dataType)
    assertResult(IntegerType)(writtenSchema(2).dataType)
    assertResult(IntegerType)(writtenSchema(3).dataType)
    assertResult(StringType)(writtenSchema(4).dataType)
    assertResult(TimestampType)(writtenSchema(5).dataType)
    reader.close()
  }

  test("Write unicode string") {
    val schema = StructType(Seq(StructField("key1", StringType)))
    val dbfOut = new ByteArrayOutputStream()
    val dbfWriter = new DBFWriter(dbfOut, schema)
    val r = Array[Any]("\u0633\u0644\u0627\u0645")
    try {
      dbfWriter.write(r)
    } finally try dbfWriter.close()
    catch {
      case e: Exception => e.printStackTrace()
    }

    // Read it back
    val reader = new DBFReader(new DataInputStream(new ByteArrayInputStream(dbfOut.toByteArray)))
    assert(reader.hasNext, "The output should have one record")
    val r2 = reader.next()
    assertResult(r2(0))(r(0))
    reader.close()
  }
}
