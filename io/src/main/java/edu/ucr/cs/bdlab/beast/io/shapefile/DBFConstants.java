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
package edu.ucr.cs.bdlab.beast.io.shapefile;

import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public class DBFConstants {
  public static final byte TypeString = 'C';
  public static final byte TypeBlockNumber = 'B';
  public static final byte TypeNumeric = 'N';
  public static final byte TypeFloat = 'F';
  public static final byte TypeDouble = '0';
  public static final byte TypeBoolean = 'L';
  public static final byte TypeDate = 'D';
  public static final byte TypeDatetime = '@';
  /**Number of milliseconds in one day*/
  public static final long MillisInOneDay = 1000L * 60 * 60 * 24;
  /**UTC time zone*/
  public static final TimeZone UTC =new SimpleTimeZone(0, "UTC");
  /**The reference date for DBF dates*/
  public static final GregorianCalendar DBFEpoch;
  static {
    DBFEpoch = new GregorianCalendar(UTC);
    DBFEpoch.clear();
    DBFEpoch.set(-4713, 0, 1);
  }
}
