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

/**
 * Descriptor for a single field in the DBF file
 * @param fieldName Field name in ASCII (zero-filled).
 * @param fieldType Field type in ASCII (B, C, D, N, L, M, @, I, +, F, 0 or G).
 * @param fieldLength Field length in binary.
 * @param decimalCount Field decimal count in binary. I think it means number of digits after the decimal point.
 * @param mdxFlag Production .MDX field flag; 0x01 if field has an index tag in the production .MDX file;
 *      0x00 if the field is not indexed.
 * @param nextAutoIncrementValue Next Autoincrement value, if the Field type is Autoincrement, 0x00 otherwise.
 */
case class DBFFieldDescriptor(fieldName: String,
                              fieldType: Short,
                              fieldLength: Short,
                              decimalCount: Byte,
                              mdxFlag: Byte,
                              nextAutoIncrementValue: Int)
