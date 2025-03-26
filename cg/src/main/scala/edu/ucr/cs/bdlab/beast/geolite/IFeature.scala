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
package edu.ucr.cs.bdlab.beast.geolite

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

import java.util
import scala.collection.GenTraversableOnce

/**
 * An interface to a geometric feature (geometry + other attributes)
 */
trait IFeature extends Serializable with Row {

  /**
   * The index of the geometry field
   */
  lazy val iGeom: Int = schema.indexWhere(_.dataType == GeometryDataType)

  /**
   * A traversable sequence of indices of all non-geometry attributes, i.e., [0, length[ - {[[iGeom]]}
   */
  lazy val iNonGeom: GenTraversableOnce[Int] =
    if (iGeom == 0) {
      1 until length
    } else {
      (0 until iGeom) ++ ((iGeom + 1) until length)
    }

  /**
   * An iterable over non geometry attributes indexes for Java.
   * Can be used with simplified Java loops such as `for (int i : feature.iNonGeomJ) { ... }`
   */
  lazy val iNonGeomJ: java.lang.Iterable[java.lang.Integer] = () => new util.Iterator[java.lang.Integer] {
    var i: Int = -1
    val lastI: Int = if (iGeom == length - 1) length - 2 else length - 1

    override def hasNext: Boolean = i < lastI

    override def next(): Integer = {
      i += 1
      if (i == iGeom)
        i += 1
      i
    }
  }

  /**
   * The geometry contained in the feature.
   *
   * @return the geometry in this attribute
   */
  def getGeometry: Geometry = if (iGeom == -1) EmptyGeometry.instance else { get(iGeom).asInstanceOf[Geometry] }

  /**
   * Return the SparkSQL data type of the given attribute.
   * @param i the index of the attribute in the range [0, length[
   * @return the type of the attribute or null if unknown
   */
  def getDataType(i: Int): DataType = schema(i).dataType

  /**
   * Return the name of the given attribute.
   * @param i the index of the attribute in the range [0, length[
   * @return the type of the attribute or null if unknown
   */
  def getName(i: Int): String = schema(i).name

  /**
   * If names are associated with attributes, this function returns the name of the attribute at the given position
   * (0-based).
   *
   * @param i the index of the attribute to return its name
   * @return the name of the given attribute index or `null` if it does not exist
   */
  def getAttributeName(i: Int): String = schema(i + 1).name

  /**
   * The estimated total size of the feature in bytes including the geometry and features
   *
   * @return the storage size in bytes
   */
  def getStorageSize: Int = {
    var size: Int = 0
    for (i <- 0 until length) {
      if (!isNullAt(i))
        size += IFeature.getStorageSize(get(i), schema(i).dataType)
    }
    size
  }

  override def toString(): String = IFeature.toString(this)
}

object IFeature {
  def getStorageSize(value: Any, dataType: DataType): Int = dataType match {
    case StringType => value.asInstanceOf[String].length
    case IntegerType => 4
    case LongType | DoubleType | TimestampType => 8
    case BooleanType => 1
    case GeometryDataType => GeometryHelper.getGeometryStorageSize(value.asInstanceOf[Geometry])
    case ArrayType(k, true) =>
      val list = value.asInstanceOf[Seq[Any]]
      if (list.isEmpty)
        0
      else
        list.size * getStorageSize(list.head, k)
    case MapType(keyType, valueType, true) =>
      val map = value.asInstanceOf[Map[Any, Any]]
      map.size * (getStorageSize(map.keys.head, keyType) + getStorageSize(map.values.head, valueType))
  }

  def toString(feature: IFeature): String = {
    val b: StringBuilder = new StringBuilder
    for ($i <- 0 until feature.length) {
      if ($i != 0)
        b.append(';')
      val value: Any = feature.get($i)
      b.append(value)
    }
    b.toString()
  }

  def equals(f1: IFeature, f2: IFeature): Boolean = {
    if (f1.length != f2.length) return false
    for (iAttr <- 0 until f1.length) {
      val name1 = f1.getName(iAttr)
      val name2 = f2.getName(iAttr)
      val namesEqual = ((name1 == null || name1.isEmpty) && (name2 == null || name2.isEmpty)) || (name1 != null && name1 == name2)
      if (!namesEqual) return false
      if (!(f1.get(iAttr) == f2.get(iAttr))) return false
    }
    true
  }

}