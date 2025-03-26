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
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}


object SpatialPredicates {
  /** Tests if a geometry is valid, e.g., not intersecting itself */
  val ST_IsValid: Geometry => Boolean = (geom: Geometry) => geom.isValid

  /** Tests if a geometry is simple, e.g., not intersecting itself */
  val ST_IsSimple: Geometry => Boolean = (geom: Geometry) => geom.isSimple

  /** Tests if a geometry is empty, e.g., containing no points at all */
  val ST_IsEmpty: Geometry => Boolean = (geom: Geometry) => geom.isEmpty

  /** Tests if a geometry is an orthogonal rectangle */
  val ST_IsRectangle: Geometry => Boolean = (geom: Geometry) => geom.isRectangle

  /** Tests if second geometry is completely contained in the first geometry */
  val ST_Contains: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.contains(geom2)

  /** Tests if the first geometry is within the second geometry. */
  val ST_Within: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.within(geom2)

  /** Tests if the two geometries are disjoint */
  val ST_Disjoint: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.disjoint(geom2)

  /** Tests if the two geometries overlap */
  val ST_Overlaps: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.overlaps(geom2)

  /** Tests if the first geometry covers the second geometry */
  val ST_Covers: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.covers(geom2)

  /** Tests if the first geometry is covered by the second geometry */
  val ST_CoveredBy: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.coveredBy(geom2)

  /** Tests if the first geometry crosses the second geometry */
  val ST_Crosses: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.crosses(geom2)

  /** Tests if the first geometry touches the second geometry */
  val ST_Touches: (Geometry, Geometry) => Boolean = (geom1: Geometry, geom2: Geometry) => geom1.touches(geom2)

  /** Tests if the distance between the two geometries is within the given range */
  val ST_WithinDistance: (Geometry, Geometry, Double) => Boolean =
    (geom1: Geometry, geom2: Geometry, distance: Double) => geom1.isWithinDistance(geom2, distance)

  trait AbstractBinaryPredicate extends Expression with CodegenFallback {
    /**Geometry factory to create geometries*/
    val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

    val inputExpressions: Seq[Expression]

    override def checkInputDataTypes(): TypeCheckResult = {
      if (inputExpressions.length == 2)
        TypeCheckResult.TypeCheckSuccess
      else
        TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects two inputs but received ${inputExpressions.length}")
    }

    override def children: Seq[Expression] = inputExpressions

    override def nullable: Boolean = false

    override def foldable: Boolean = inputExpressions.forall(_.foldable)

    override def dataType: DataType = BooleanType

    override def eval(input: InternalRow): Any = {
      val inputValues: Seq[Any] = inputExpressions.map(e => e.eval(input))
      val geometries = inputValues.map(x => GeometryDataType.getGeometryFromArray(x.asInstanceOf[ArrayData]))
      val result: Boolean = performFunction(geometries(0), geometries(1))
      result
    }

    def performFunction(geom1: Geometry, geom2: Geometry): Boolean
  }

  /** Tests if two geometries intersect, i.e., are not disjoint */
  case class ST_Intersects(override val inputExpressions: Seq[Expression]) extends AbstractBinaryPredicate {

    override def performFunction(geom1: Geometry, geom2: Geometry): Boolean = geom1.intersects(geom2)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
  }

  /**
   * A user-defined function that tests if two envelops overlap. This method is not supposed to be called directly
   * by the user but it is created internally to apply some query optimization rules, e.g., filter push-down
   * or spatial join.
   */
  case class ST_EnvelopeIntersects(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {
    override def nullable: Boolean = false

    override def foldable: Boolean = inputExpressions.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
      val envelopes = inputExpressions.map(_.eval(input).asInstanceOf[Envelope])
      envelopes(0).intersects(envelopes(1))
    }

    override def dataType: DataType = BooleanType

    override def children: Seq[Expression] = inputExpressions

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
  }
}
