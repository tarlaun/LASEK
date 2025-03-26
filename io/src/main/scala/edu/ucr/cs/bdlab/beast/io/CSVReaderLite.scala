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
package edu.ucr.cs.bdlab.beast.io

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.InputStream
import java.util.Scanner

/**
 * A light-weight reader for CSV files. The input is assumed to always contain a header in the first line
 * @param in the input stream to read from
 * @param separator the field separator. Default: ,
 * @param quotes quotes that could surround fields. Every pair of characters define start and end quotes
 */
class CSVReaderLite(in: InputStream, separator: Char = ',', quotes: String = "''\"\"") extends Iterator[Row] with AutoCloseable {

  val scanner = new Scanner(in)

  val schema: StructType = if (scanner.hasNextLine)
    StructType(parseLine(scanner.nextLine()).map(fieldName => StructField(fieldName, StringType)))
  else null

  var nextRecord: Row = prefetchNext

  private def prefetchNext: Row = {
    if (!scanner.hasNextLine)
      return null
    val nextLine: Array[String] = parseLine(scanner.nextLine())
    new GenericRowWithSchema(nextLine.map(x => x), schema)
  }

  override def hasNext: Boolean = nextRecord != null

  override def next(): Row = {
    val currentRecord = nextRecord
    nextRecord = prefetchNext
    currentRecord
  }

  override def close(): Unit = scanner.close()

  /**
   * Parses one line into fields according to the given parameters
   * @param line the line to split
   * @return the parts of the line
   */
  private def parseLine(line: String): Array[String] = {
    var parts = Array[String]()
    var i1 = 0
    while (i1 < line.length) {
      // Check if this is a quoted field
      var iQuote = 0
      while (iQuote < quotes.length && line(i1) != quotes(iQuote))
        iQuote += 2
      var i2 = i1 + 1
      if (iQuote < quotes.length) {
        // Found an open quote. Skip until the closing quote
        val stopChar = quotes(iQuote + 1)
        while (i2 < line.length && line(i2) != stopChar)
          i2 += 1
        // If did not reach end-of-line or next character is not the field separator, keep going
        if (i2 < line.length - 1 && line(i2+1) != separator) {
          while (i2 < line.length && line(i2) != separator)
            i2 += 1
          parts = parts :+ line.substring(i1, i2)
        } else {
          // Read the record in the quote
          parts = parts :+ line.substring(i1+1, i2)
          i2 += 1 // Skip the field separator as well
        }
      } else {
        // No quote. Skip until end-of-line or field separator
        while (i2 < line.length && line(i2) != separator)
          i2 += 1
        parts = parts :+ line.substring(i1, i2)
        i2 += 1 // Skip the field separator as well
      }
      i1 = i2
    }
    parts
  }
}
