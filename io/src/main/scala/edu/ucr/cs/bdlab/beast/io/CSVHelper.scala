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

import edu.ucr.cs.bdlab.beast.io.shapefilev2.DBFHelper

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.{Calendar, GregorianCalendar}


/**
 * A helper for CSV features
 */
object CSVHelper {
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  dateFormatter.setTimeZone(DBFHelper.UTC)
  val timestampFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  timestampFormatter.setTimeZone(DBFHelper.UTC)

  def encodeValue(value: Any, str: java.lang.StringBuilder): java.lang.StringBuilder = value match {
    case null => str // Do nothing
    case date: java.sql.Date =>
      str.append(dateFormatter.format(date))
    case timestamp: java.sql.Timestamp =>
      str.append(timestampFormatter.format(timestamp))
    case other => str.append(other.toString.replaceAll("\n", "\\\\n"))
  }
}
