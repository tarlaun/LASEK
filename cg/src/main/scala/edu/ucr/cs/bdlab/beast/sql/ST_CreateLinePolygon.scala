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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

import scala.util.control.Breaks

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not
 *
 * @param inputExpressions
 */
case class ST_CreateLinePolygon(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {
  /** Geometry factory to create geometries */
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.nonEmpty && inputExpressions.length <= 3)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects 1 - 3 inputs but received ${inputExpressions.length}")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = GeometryDataType

  override def eval(input: InternalRow): Any = {
    val inputValues: Seq[Any] = inputExpressions.map(e => e.eval(input))
    val coordinates = inputValues.head.asInstanceOf[ArrayData]
    var pointIDs: ArrayData = null
    var pos: ArrayData = null
    if (inputValues.length > 1) {
      pointIDs = inputValues(1).asInstanceOf[ArrayData]
      if (inputValues.length == 3) {
        pos = inputValues(2).asInstanceOf[ArrayData]
      }
    }
    val result: Geometry = performFunction(coordinates, pointIDs, pos)
    GeometryDataType.setGeometryInRow(result)
  }

  /**
   *
   * @param pointIDs    point ID array
   * @param coordinates coordinates array
   * @param pos         position index array. If you do not need pos array, you could pass a null, we will handle that.
   * @return
   */
  def performFunction(coordinates: ArrayData, pointIDs: ArrayData, pos: ArrayData): Geometry = {
    val coordinatesArray = coordinates.toDoubleArray
    assert(coordinatesArray.length % 2 == 0)
    var posSeq: Seq[Int] = null
    if (pos == null) {
      posSeq = 0 until coordinatesArray.length / 2
    } else {
      posSeq = pos.toSeq(IntegerType)
      if (posSeq.max != posSeq.length - 1) {
        posSeq = posSeq.map(x => posSeq.count(y => y < x))
      }
    }
    assert(posSeq.max == posSeq.length - 1)
    val firstIndex = posSeq.indexOf(0)
    val lastIndex = posSeq.indexOf(posSeq.length - 1)
    var is_polygon = false
    if (pointIDs == null) {
      is_polygon = coordinatesArray(firstIndex * 2) == coordinatesArray(lastIndex * 2) &&
        coordinatesArray(firstIndex * 2 + 1) == coordinatesArray(lastIndex * 2 + 1)
    } else {
      val pointIdsArray = pointIDs.toLongArray
      is_polygon = pointIdsArray(firstIndex) == pointIdsArray(lastIndex)
    }
    val coords = new Array[Coordinate](posSeq.length)
    val loop = new Breaks
    for (i <- posSeq.indices) {
      loop.breakable {
        if (is_polygon) {
          if (firstIndex <= lastIndex && posSeq(i) == posSeq.length - 1) {
            coords(posSeq.length - 1) = coords(0)
            loop.break
          } else if (firstIndex > lastIndex && posSeq(i) == 0) {
            coords(0) = coords(posSeq.length - 1)
            loop.break
          }
        }
        coords(posSeq(i)) = new Coordinate(coordinatesArray(i * 2), coordinatesArray(i * 2 + 1))
      }
    }

    if (is_polygon) {
      if (coords.length == 1 || coords.length == 2) {
        geometryFactory.createPoint(coords(0))
      } else if (coords.length == 3) {
        geometryFactory.createLineString(coords.take(2))
      } else {
        geometryFactory.createPolygon(coords)
      }
    } else {
      geometryFactory.createLineString(coords)
    }
  }
}