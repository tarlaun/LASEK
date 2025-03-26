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
package org.apache.spark.beast.sql

import org.apache.spark.beast.sql.EnvelopeDataType.{fromRow, toRow}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType, UserDefinedType}
import org.locationtech.jts.geom.Envelope

/**
 * A data type that represents a two-dimensional bounding box. Used internally for query optimization rules
 * that involve filter-refinement approach.
 */
class EnvelopeDataType extends UserDefinedType[Envelope] {
  override def sqlType: DataType = StructType(Seq(StructField("minx", DoubleType), StructField("miny", DoubleType),
    StructField("maxx", DoubleType), StructField("maxy", DoubleType)))

  override def serialize(obj: Envelope): Any = toRow(obj)

  override def deserialize(datum: Any): Envelope = {
    datum match {
      case x: InternalRow => fromRow(x)
    }
  }

  override def userClass: Class[Envelope] = classOf[Envelope]

  override def defaultSize: Int = 8 * 4
}

case object EnvelopeDataType extends EnvelopeDataType {
  def fromRow(x: InternalRow): Envelope =
    new Envelope(x.getDouble(0), x.getDouble(2), x.getDouble(1), x.getDouble(3))

  def toRow(e: Envelope): InternalRow = InternalRow.apply(e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)
}