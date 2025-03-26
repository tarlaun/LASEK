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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, GeometryReader}
import org.apache.spark.beast.sql.{EnvelopeDataType, GeometryDataType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader}

/**
 * Create a geometry from a WKT representation
 */
object CreationFunctions {
  /** A common geometry factory to use by functions */
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  /** A common WKT reader to use by functions */
  val wktParser: WKTReader = new WKTReader(geometryFactory)

  /** A common WKB parser to use by functions */
  val wkbParser = new WKBReader(geometryFactory)

  val wkbWriter = new WKBWriter()

  /** Create a geometry from a WKT representation */
  val ST_FromWKT: String => Geometry = wkt => wktParser.read(wkt)

  /** Parse WKB into a geometry */
  val ST_FromWKB: Array[Byte] => Geometry = wkb => wkbParser.read(wkb)

  val ST_ToWKB: Geometry => Array[Byte] = geometry => wkbWriter.write(geometry)

  /** Create a 2d-point from (x, y) coordinates */
  val ST_CreatePoint: (Double, Double) => Geometry = (x: Double, y: Double) => geometryFactory.createPoint(new CoordinateXY(x, y))

  /** Create a 3d-point from (x, y, z) coordinates */
  val ST_CreatePointZ: (Double, Double, Double) => Geometry = (x: Double, y: Double, z: Double) => geometryFactory.createPoint(new Coordinate(x, y, z))

  /** Create a 3d-point from (x, y, m) coordinates */
  val ST_CreatePointM: (Double, Double, Double) => Geometry = (x: Double, y: Double, m: Double) => geometryFactory.createPoint(new CoordinateXYM(x, y, m))

  /** Create a 4d-point from (x, y, z, m) coordinates */
  val ST_CreatePointZM: (Double, Double, Double, Double) => Geometry = (x: Double, y: Double, z: Double, m: Double) =>
    geometryFactory.createPoint(new CoordinateXYZM(x, y, z, m))

  /**Create a 2D line string from an array of coordinates in the from (x1, y1, x2, y2, ..., xn, yn)*/
  val ST_CreateLine: Array[Double] => Geometry = (coords: Array[Double]) => {
    require(coords.length % 2 == 0, s"ST_CreateLine takes an even number of coordinates but got ${coords.length}")
    val coordinateSequence = geometryFactory.getCoordinateSequenceFactory.create(coords.length, 2)
    for (i <- coords.indices by 2) {
      coordinateSequence.setOrdinate(0, i >> 1, coords(i))
      coordinateSequence.setOrdinate(1, i >> 1, coords(i + 1))
    }
    geometryFactory.createLineString(coordinateSequence)
  }

  /**
   * Create a 2D polygon from an array of coordinates in the from (x1, y1, x2, y2, ..., xn, yn).
   * Where (x1, y1) = (xn, yn)
   */
  val ST_CreatePolygon: Array[Double] => Geometry = (coords: Array[Double]) => {
    require(coords.length % 2 == 0, s"ST_CreatePolygon takes an even number of coordinates but got ${coords.length}")
    require(coords(0) == coords(coords.length - 2) && coords(1) == coords(coords.length - 1),
      s"First and last points should be the same but got ${(coords(0), coords(1))} " +
        s"and ${(coords(coords.length - 2), coords(coords.length - 1))}")
    val coordinateSequence = geometryFactory.getCoordinateSequenceFactory.create(coords.length, 2)
    for (i <- coords.indices by 2) {
      coordinateSequence.setOrdinate(0, i >> 1, coords(i))
      coordinateSequence.setOrdinate(1, i >> 1, coords(i + 1))
    }
    geometryFactory.createPolygon(coordinateSequence)
  }
}

trait AbstractCreationFunction extends Expression with CodegenFallback {
  /**Geometry factory to create geometries*/
  val geometryFactory: GeometryFactory = GeometryReader.DefaultGeometryFactory

  val inputExpressions: Seq[Expression]

  val inputArity: Int

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
    val result: Geometry = performFunction(inputValues)
    GeometryDataType.setGeometryInRow(result)
  }

  def performFunction(inputs: Seq[Any]): Geometry
}

/** Create a 2D box from two corners defined by (x1, y1) and (x2, y2)*/
case class ST_CreateBox(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {
  override val inputArity: Int = 4

  override def performFunction(inputs: Seq[Any]): Geometry = {
    val coordinates: Array[Double] = inputs.map(x => x.asInstanceOf[Number].doubleValue()).toArray
    geometryFactory.toGeometry(new Envelope(coordinates(0), coordinates(2), coordinates(1), coordinates(3)))
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}

/**
 * Creates an n-dimensional envelope from a list of numeric values
 * @param inputExpressions
 */
case class ST_CreateEnvelopeN(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {
  override val inputArity: Int = -1
  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length > 0 && inputExpressions.length % 2 == 0)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects even number of inputs but received ${inputExpressions.length}")
  }
  override def performFunction(inputs: Seq[Any]): Geometry = {
    val coordinates: Array[Double] = inputs.map(x => x.asInstanceOf[Number].doubleValue()).toArray
    new EnvelopeND(geometryFactory, coordinates.length / 2, coordinates:_*)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}

/**
 * Finds the minimal bounding rectangle (MBR) of a set of shapes
 * @param inputExpressions
 */
case class ST_Extent(override val inputExpressions: Seq[Expression])
  extends AbstractCreationFunction {

  override val inputArity: Int = -1

  override def performFunction(inputs: Seq[Any]): Geometry = {
    val geometries = inputs.map(x => x.asInstanceOf[Geometry]).toArray
    val geometryCollection = geometryFactory.createGeometryCollection(geometries)
    geometryCollection.getEnvelope
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}

case class ST_MBR(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputExpressions.length == 1)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Function $prettyName expects one input but received ${inputExpressions.length}")
  }

  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  override def foldable: Boolean = inputExpressions.forall(_.foldable)

  override def dataType: DataType = EnvelopeDataType

  override def eval(input: InternalRow): Any = {
    val geometry = GeometryDataType.getGeometryFromArray(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
    val envelope = geometry.getEnvelopeInternal
    InternalRow.apply(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}