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

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString}

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not
 *
 * @param inputExpressions
 */
case class ST_BreakLine(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {
  /** Geometry factory to create geometries */
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  val inputArity: Int = 1

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length == inputArity && inputExpressions.forall(_.dataType == GeometryDataType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects $inputArity geometry arguments")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = ArrayType(GeometryDataType)

  override def eval(input: InternalRow): Any = {
    val geometries: Seq[Geometry] = inputExpressions.map(e =>
      GeometryDataType.getGeometryFromArray(e.eval(input).asInstanceOf[ArrayData]))
    val segments = performFunction(geometries)
    val result = segments.map(e => GeometryDataType.setGeometryInRow(e))
    new GenericArrayData(result)
  }

  /**
   *
   * @param geometries geometry array
   * @return
   */
  def performFunction(geometries: Seq[Geometry]): Array[LineString] = {
    val coordinates = geometries.head.getCoordinates
    var result: Array[LineString] = null
    if (coordinates.length > 1) {
      result = new Array[LineString](coordinates.length - 1)
      for (i <- 0 to coordinates.length - 2) {
        result(i) = geometryFactory.createLineString(Array(coordinates(i), coordinates(i + 1)))
      }
    }
    result
  }
}