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

import edu.ucr.cs.bdlab.beast.io.shapefile.{FieldDescriptor, FieldProperties}

/**
 *
 * @param version Valid dBASE for Windows table file, bits 0-2 indicate version number: 3 for dBASE Level 5, 4 for dBASE Level 7.
 *                Bit 3 and bit 7 indicate presence of a dBASE IV or dBASE for Windows memo file; bits 4-6 indicate
 *                the presence of a dBASE IV SQL table; bit 7 indicates the presence of any .DBT memo file
 *                (either a dBASE III PLUS type or a dBASE IV or dBASE for Windows memo file).
 * @param dateLastUpdated Date of last update; in YYMMDD format. Each byte contains the number as a binary. YY is added to a base
 *     of 1900 decimal to determine the actual year. Therefore, YY has possible values from 0x00-0xFF, which allows
 *     for a range from 1900-2155.
 * @param numRecords Number of records in the table. (Least significant byte first.)
 * @param headerSize Number of bytes in the header. (Least significant byte first.)
 * @param recordSize Number of bytes in the record including the record marker. (Least significant byte first.)
 * @param incomplete Flag indicating incomplete dBASE IV transaction.
 * @param encryption dBASE IV encryption flag.
 * @param mdxFlag Production MDX flag; 0x01 if a production .MDX file exists for this table; 0x00 if no .MDX file exists.
 * @param driverID Language driver ID.
 * @param driverName Language driver name
 * @param fieldDescriptors Field Descriptor Array
 * @param fieldProperties Field Properties Structure
 */
case class DBFHeader(version: Byte,
                     dateLastUpdated: java.util.Date,
                     numRecords: Int,
                     headerSize: Int,
                     recordSize: Int,
                     incomplete: Byte,
                     encryption: Byte,
                     mdxFlag: Byte,
                     driverID: Byte,
                     driverName: String,
                     fieldDescriptors: Array[DBFFieldDescriptor],
                     fieldProperties: Array[DBFFieldProperties])
