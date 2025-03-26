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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom._

import scala.collection.mutable.ArrayBuffer
import scala.util.control._

/**
 * Takes a list of point locations and IDs and creates either a linestring or
 * polygon based on whether the last point is the same as the first point or not
 *
 * @param inputExpressions
 */
case class ST_Connect(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {
  /** Geometry factory to create geometries */
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  val inputArity: Int = 3

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputArity == -1 || inputExpressions.length == inputArity)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects $inputArity inputs but received ${inputExpressions.length}")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = GeometryDataType

  override def eval(input: InternalRow): Any = {
    val inputValues: Seq[Any] = inputExpressions.map(e => e.eval(input))
    val input1 = inputValues.head.asInstanceOf[UnsafeArrayData]
    val input2 = inputValues(1).asInstanceOf[UnsafeArrayData]
    val input3 = inputValues(2).asInstanceOf[UnsafeArrayData]
    val result: Geometry = performFunction(input1, input2, input3)
    GeometryDataType.setGeometryInRow(result)
  }

  def performFunction(input1: UnsafeArrayData, input2: UnsafeArrayData, input3: UnsafeArrayData): Geometry = {
    val firstIdsItr = input1.toLongArray.iterator
    val lastIdsItr = input2.toLongArray.iterator
    val geometriesItr = input3.toObjectArray(GeometryDataType).iterator

    val createdShapes = ArrayBuffer[Geometry]()
    val linestrings = ArrayBuffer[LineString]()
    val firstIds = ArrayBuffer[Long]()
    val lastIds = ArrayBuffer[Long]()

    while (firstIdsItr.hasNext && lastIdsItr.hasNext && geometriesItr.hasNext) {
      val firstId = firstIdsItr.next()
      val lastId = lastIdsItr.next()
      val geometry = GeometryDataType.deserialize(geometriesItr.next())

      geometry match {
        case _: Polygon =>
          createdShapes.append(geometry)
        case _: LineString =>
          linestrings.append(geometry.asInstanceOf[LineString])
          firstIds.append(firstId)
          lastIds.append(lastId)
        case _ => if (geometry.isEmpty) {
          // Skip empty geometries
        } else {
          throw new IllegalArgumentException("Cannot connect shapes of type " + geometry.getClass)
        }
      }
    }

    if (firstIdsItr.hasNext || lastIdsItr.hasNext || geometriesItr.hasNext) {
      throw new IllegalArgumentException("All parameters should be of the same size ()")
    }

    val connected_lines = ArrayBuffer[LineString]()
    var sumPoints = 0
    val reverse = ArrayBuffer[Boolean]()
    var first_point_id: Long = -1
    var last_point_id: Long = -1

    while (linestrings.nonEmpty) {
      val size_before = connected_lines.size
      var i = 0
      while (i < linestrings.size) {
        if (connected_lines.isEmpty) {
          // First linestring
          first_point_id = firstIds.remove(i)
          last_point_id = lastIds.remove(i)
          reverse.append(false)
          sumPoints += linestrings(i).getNumPoints
          connected_lines.append(linestrings.remove(i))
        } else if (lastIds(i) == first_point_id) {
          // This linestring goes to the beginning of the list as-is
          lastIds.remove(i)
          first_point_id = firstIds.remove(i)
          sumPoints += linestrings(i).getNumPoints
          connected_lines.prepend(linestrings.remove(i))
          reverse.prepend(false)
        } else if (firstIds(i) == first_point_id) {
          // Should go to the beginning after being reversed
          firstIds.remove(i)
          first_point_id = lastIds.remove(i)
          sumPoints += linestrings(i).getNumPoints
          connected_lines.prepend(linestrings.remove(i))
          reverse.prepend(true)
        } else if (firstIds(i) == last_point_id) {
          // This linestring goes to the end of the list as-is
          firstIds.remove(i)
          last_point_id = lastIds.remove(i)
          sumPoints += linestrings(i).getNumPoints
          connected_lines.append(linestrings.remove(i))
          reverse.append(false)
        } else if (lastIds(i) == last_point_id) {
          // Should go to the end after being reversed
          lastIds.remove(i)
          last_point_id = firstIds.remove(i)
          sumPoints += linestrings(i).getNumPoints
          connected_lines.append(linestrings.remove(i))
          reverse.append(true)
        } else {
          i += 1
        }
      }

      if (connected_lines.length == size_before || linestrings.isEmpty) {
        // Cannot connect any more lines to the current block. Emit as a shape
        val isPolygon = first_point_id == last_point_id
        val points: Array[Coordinate] = new Array[Coordinate](sumPoints - connected_lines.length + 1)
        var n = 0
        for (i <- connected_lines.indices) {
          val linestring = connected_lines(i)
          val isReverse = reverse(i)
          val last_i = if (i == connected_lines.length - 1) linestring.getNumPoints else linestring.getNumPoints - 1
          var i_point = 0
          while (i_point < last_i) {
            points(n) = linestring.getPointN(if (isReverse) linestring.getNumPoints - 1 - i_point else i_point).getCoordinate
            n += 1
            i_point += 1
          }
        }

        createdShapes.append(
          if (isPolygon && points.length >= 4) geometryFactory.createPolygon(points)
          else geometryFactory.createLineString(points))

        // Re-initialize all data structures to connect remaining lines
        if (linestrings.nonEmpty) {
          connected_lines.clear
          reverse.clear
          sumPoints = 0
        }
      }
    }

    if (createdShapes.length == 1) {
      createdShapes.head
    } else if (createdShapes.length > 1) {
      val loop = new Breaks
      var allPolygons = false
      var allLineStrings = false
      loop.breakable {
        for (i <- createdShapes.indices) {
          if (i == 0) {
            if (createdShapes(i).isInstanceOf[Polygon]) {
              allPolygons = true
            } else {
              allLineStrings = true
            }
          } else {
            if (createdShapes(i).isInstanceOf[Polygon] && allLineStrings ||
              createdShapes(i).isInstanceOf[LineString] && allPolygons) {
              allLineStrings = false
              allPolygons = false
              loop.break
            }
          }
        }
      }
      if (allLineStrings) {
        val createdLineStrings = ArrayBuffer[LineString]()
        for (geometry <- createdShapes) {
          createdLineStrings.append(geometry.asInstanceOf[LineString])
        }
        geometryFactory.createMultiLineString(createdLineStrings.toArray)
      } else if (allPolygons) {
        val createdPolygons = ArrayBuffer[Polygon]()
        for (geometry <- createdShapes) {
          createdPolygons.append(geometry.asInstanceOf[Polygon])
        }
        geometryFactory.createMultiPolygon(createdPolygons.toArray)
      } else {
        geometryFactory.createGeometryCollection(createdShapes.toArray)
      }
    } else {
      throw new IllegalArgumentException("No shapes to connect")
    }
  }
}