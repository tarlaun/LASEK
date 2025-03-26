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
package edu.ucr.cs.bdlab.beast.io

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, FileInputStream, PrintStream}

@RunWith(classOf[JUnitRunner])
class CSVReaderLiteTest extends AnyFunSuite with ScalaSparkTest {
  test("Read master file") {
    val input = getClass.getResourceAsStream("/test.partitions")
    val reader = new CSVReaderLite(input, separator = '\t')
    try {
      val records = reader.toArray
      assertResult(44)(records.length)
      assertResult("0")(records(0).getAs[String]("ID"))
      assertResult("part-00021")(records(1).getAs[String]("File Name"))
    } finally {
      reader.close()
    }
  }

  test("Separator in quoted field") {
    val file = new File(scratchDir, "test.txt")
    try {
      val output = new PrintStream(file)
      output.println("ID,Name")
      output.println("1,'Doe,Jane'")
      output.close()
      val input = new FileInputStream(file)
      val reader = new CSVReaderLite(input, separator = ',')
      try {
        val records = reader.toArray
        assertResult(1)(records.length)
        assertResult("1")(records(0).getAs[String]("ID"))
        assertResult("Doe,Jane")(records(0).getAs[String]("Name"))
      } finally {
        reader.close()
      }
    } finally {
      if (file.exists())
        file.delete()
    }
  }
}
