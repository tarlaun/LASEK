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
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom._

/**
 * Converts a geometry to a format compatible with SpatialParquet. SpatialParquet geometry records are written as:
 * <pre>
 * message geometry {
 *   // The spatial reference identifier of this geometry
 *   required int srid;
 *   // The list of sub geometries. In case of geometry collection, all goemetries will be stored here.
 *   // Otherwise, this group will contain only one value
 *   repeated group subgeometries {
 *     // The type of the geometry (1: Point, 2: LineString, 3: Polygon)
 *     required int geomtype;
 *     repeated double xs;
 *     repeated double ys;
 *     repeated double zs;
 *     repeated double ms;
 *   }
 * }
 * </pre>
 * Here is how various geometry are encoded using this format.
 *
 * - Empty: Zero subgeometries
 * - Point: One subgeometry that has geomtype=1 and the coordinates of that point.
 * - LineString: One subgeometry with geomtype=2 and the coordinates of all points in the corresponding columns.
 * - Polygon: One subgeometry with geomtype=3. The end of each ring is recognized by the repeated coordinates.
 * - MultiPoint: One subgeometry with geomtype=1. All the coordinates are stored in the lists.
 * - MultiLineString: Multiple subgeometries all with geomtype=2. Each one represents one of the linestrings.
 * - MultiPolygon: Each polygon is stored as a separate subgeometry with geomtype=3 as described above.
 * - GeometryCollection: Flattened into a list of non-collection geometries and then each is stored as described above.
 * @param inputExpressions
 */
case class ST_ToSpatialParquet(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length == 1 && inputExpressions.forall(_.dataType == GeometryDataType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects one geometry input")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = SpatialParquetHelper.SpatialParquetGeometryType

  override def eval(input: InternalRow): Any = {
    val geometry: Geometry = inputExpressions.map(e =>
      GeometryDataType.getGeometryFromArray(e.eval(input).asInstanceOf[ArrayData])).head
    val srid = geometry.getSRID
    val subgeometries: Seq[InternalRow] = SpatialParquetHelper.encodeGeometry(geometry)
    InternalRow.apply(srid, new GenericArrayData(subgeometries))
  }
}
