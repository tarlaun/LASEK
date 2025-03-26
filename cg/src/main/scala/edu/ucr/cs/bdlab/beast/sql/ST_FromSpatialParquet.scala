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
package edu.ucr.cs.bdlab.beast.sql

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.locationtech.jts.geom._

/**
 * Converts a geometry encoded using the SpatialParquet format back into a geometry.
 * @param inputExpressions
 * @see [[ST_ToSpatialParquet]]
 */
case class ST_FromSpatialParquet(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length == 1 && inputExpressions.forall(_.dataType == SpatialParquetHelper.SpatialParquetGeometryType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects one encoded-geometry input")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = GeometryDataType

  override def eval(input: InternalRow): Any = {
    val encodedGeometry: InternalRow = inputExpressions.map(e => e.eval(input)).head.asInstanceOf[InternalRow]
    val srid = encodedGeometry.getInt(0)
    val factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), srid)
    val encodedSubGeometries: ArrayData = encodedGeometry.getArray(1)
    val subgeometries = new Array[Geometry](encodedSubGeometries.numElements)
    for (i <- 0 until encodedSubGeometries.numElements)
      subgeometries(i) = SpatialParquetHelper.decodeGeometry(encodedSubGeometries.getStruct(i, 5), factory)

    val geometry =  if (subgeometries.length == 1)
      subgeometries.head
    else if (subgeometries.forall(_.isInstanceOf[Point]))
      factory.createMultiPoint(subgeometries.map(_.asInstanceOf[Point]))
    else if (subgeometries.forall(_.isInstanceOf[LineString]))
      factory.createMultiLineString(subgeometries.map(_.asInstanceOf[LineString]))
    else if (subgeometries.forall(_.isInstanceOf[Polygon]))
      factory.createMultiPolygon(subgeometries.map(_.asInstanceOf[Polygon]))
    else
      factory.createGeometryCollection(subgeometries)
    GeometryDataType.setGeometryInRow(geometry)
  }

}
