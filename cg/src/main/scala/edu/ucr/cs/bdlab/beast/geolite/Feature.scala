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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import edu.ucr.cs.bdlab.beast.geolite.Feature.{readType, readValue, writeType, writeValue}
import edu.ucr.cs.bdlab.beast.util.{BitArray, KryoInputToObjectInput, KryoOutputToObjectOutput}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.{Calendar, SimpleTimeZone, TimeZone}

/**
 * A Row that contains a geometry
 * @param _values an initial list of values that might or might not contain a [[Geometry]]
 * @param _schema the schema of the given values or `null` to auto-detect the types from the values
 */
class Feature(private var _values: Array[Any], private var _schema: StructType)
  extends IFeature with Externalizable with KryoSerializable {

  override def schema: StructType = _schema
  private var values : Array[Any] =  if(_values == null) { new Array[Any](0) } else {_values.map{
        case string : UTF8String => string.toString
        case geom : UnsafeArrayData => GeometryDataType.deserialize(geom)
        case other : Any => other
        case _ => null
      }}
  if (_schema == null && values != null)
    _schema = Feature.inferSchema(values)
  /**
   * Default constructor for serialization/deserialization
   */
  def this() {
    this(_values = null, _schema = null)
  }

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)

  /**
   * Efficient Java serialization/deserialization. A feature is serialized as follows.
   *  - Total number of attributes including the geometry, i.e., [[length]]
   *  - The names of the attributes in order. If the name does not exist, an empty string is written.
   *  - The types of the attributes, each written as a single byte.
   *  - A compact bit mask of which attribute values are null.
   *  - The values of the attributes in Java serialization form.
   *  - The geometry is written using GeometryWriter with the SRID in its position in the list of values
   *  - null values are skipped
   * @param out the output to write to
   */
  override def writeExternal(out: ObjectOutput): Unit = {
    // Number of attributes
    out.writeShort(length)
    if (length > 0) {
      // Attribute names
      for (field <- schema)
        out.writeUTF(if (field.name == null) "" else field.name)
      // Attribute types
      for (field <- schema)
        writeType(field.dataType, out)
      // Attribute exists (bit mask)
      val attributeExists = new BitArray(length)
      for (i <- 0 until length)
        attributeExists.set(i, !isNullAt(i))
      attributeExists.writeBitsMinimal(out)
      // Attribute values
      for (i <- 0 until length; if !isNullAt(i)) {
        val value = values(i)
        writeValue(out, value, schema(i).dataType)
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    // Read number of attributes
    val recordLength: Int = in.readShort()
    val attributeNames = new Array[String](recordLength)
    val attributeTypes = new Array[DataType](recordLength)
    // Read attribute names
    for (i <- 0 until recordLength)
      attributeNames(i) = in.readUTF()
    // Read attribute types
    for (i <- 0 until recordLength)
      attributeTypes(i) = readType(in)
    this._schema = StructType((0 until recordLength).map(i => StructField(attributeNames(i), attributeTypes(i))))
    // Read attribute exists
    val attributeExists = new BitArray(recordLength)
    attributeExists.readBitsMinimal(in)
    // Read attribute values
    this.values = new Array[Any](recordLength)
    for (i <- 0 until recordLength; if attributeExists.get(i))
      values(i) = readValue(in, attributeTypes(i))
  }

  override def write(kryo: Kryo, out: Output): Unit = writeExternal(new KryoOutputToObjectOutput(kryo, out))
  override def read(kryo: Kryo, in: Input): Unit = readExternal(new KryoInputToObjectInput(kryo, in))

  override def length: Int = if (values == null) 0 else values.length

  override def get(i: Int): Any = values(i)

  /**
   * Make a copy of this row. Since Feature is immutable, we just return the same object.
   * @return the same object
   */
  override def copy(): Row = this

  /**
   * Convert it to an [[InternalRow]] to work with the DataSource API.
   * Notice that [[Row]] and [[InternalRow]] are not compatible with each other due to the conflicting
   * return data type of the function copy(). So, there is no way to create one class that implements both.
   *
   * @return an [[InternalRow]] representation of this feature, i.e., values without schema
   */
  def toInternalRow: InternalRow = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values.map {
    case string : String => UTF8String.fromString(string)
    case geom : Geometry => GeometryDataType.serialize(geom)
    case other : Any => other
    case _ => null
  })
}

object Feature {

  val UTC: TimeZone = new SimpleTimeZone(0, "UTC")

  /**
   * Maps each data type to its ordinal number
   */
  val typeOrdinals: Map[DataType, Int] = Map(
    ByteType -> 0,
    ShortType -> 1,
    IntegerType -> 2,
    LongType -> 3,
    FloatType -> 4,
    DoubleType -> 5,
    StringType -> 6,
    BooleanType -> 7,
    GeometryDataType -> 8,
    DateType -> 9,
    TimestampType -> 10,
    MapType(BinaryType, BinaryType, valueContainsNull = true) -> 11,
    ArrayType(BinaryType) -> 12
  )

  /**
   * Maps each integer value to the corresponding data type
   */
  val ordinalTypes: Map[Int, DataType] = typeOrdinals.map(kv => (kv._2, kv._1))

  /**
   * Writes the given data type to the given output so that it can be read back using the function [[readType()]]
   * @param t the SparkSQL data type of the value to write
   * @param out the output to write the type to.
   */
  def writeType(t: DataType, out: ObjectOutput): Unit = t match {
    case mp: MapType =>
      out.writeByte(11)
      writeType(mp.keyType, out)
      writeType(mp.valueType, out)
    case ap: ArrayType =>
      out.writeByte(12)
      writeType(ap.elementType, out)
    case _ => out.writeByte(typeOrdinals.getOrElse(t, -1))
  }

  /**
   * Read a data type from an input stream that was written with the function [[writeType()]]
   * @param in the input stream to read from
   * @return the created data type
   */
  def readType(in: ObjectInput): DataType = {
    val typeOrdinal = in.readByte()
    if (typeOrdinal == 11) {
      // Indicates a map type
      val keyType: DataType = readType(in)
      val valueType: DataType = readType(in)
      MapType(keyType, valueType, valueContainsNull = true)
    } else if (typeOrdinal == 12) {
      // Indicates an array type
      val elementType: DataType = readType(in)
      ArrayType(elementType, containsNull = true)
    } else {
      ordinalTypes.getOrElse(typeOrdinal, BinaryType)
    }
  }

  /**
   * Writes the given value with the corresponding data type to the output stream
   * @param out the output stream to write to
   * @param t the data type of the given value as specified in the Row schema
   */
  def writeValue(out: ObjectOutput, value: Any, t: DataType): Unit = t match {
    case ByteType => out.writeByte(value.asInstanceOf[Number].byteValue())
    case ShortType => out.writeShort(value.asInstanceOf[Number].shortValue())
    case IntegerType => out.writeInt(value.asInstanceOf[Number].intValue())
    case LongType => out.writeLong(value.asInstanceOf[Number].longValue())
    case FloatType => out.writeFloat(value.asInstanceOf[Number].floatValue())
    case DoubleType => out.writeDouble(value.asInstanceOf[Number].doubleValue())
    case StringType => out.writeUTF(value.asInstanceOf[String])
    case BooleanType => out.writeBoolean(value.asInstanceOf[Boolean])
    case GeometryDataType => new GeometryWriter().write(value.asInstanceOf[Geometry], out, true)
    case mapType: MapType =>
      val map = value.asInstanceOf[Map[Any, Any]]
      out.writeInt(map.size)
      for ((k, v) <- map) {
        writeValue(out, k, mapType.keyType)
        writeValue(out, v, mapType.valueType)
      }
    case _ => out.writeObject(value)
  }

  /**
   * Read a single value from an input stream according to the given data type.
   * @param in the input stream to read from
   * @param t the type of the attribute
   * @return the attribute value read from the input stream
   */
  def readValue(in: ObjectInput, t: DataType): Any = t match {
    case ByteType => in.readByte()
    case ShortType => in.readShort()
    case IntegerType => in.readInt()
    case LongType => in.readLong()
    case FloatType => in.readFloat()
    case DoubleType => in.readDouble()
    case StringType => in.readUTF()
    case BooleanType => in.readBoolean()
    case GeometryDataType => GeometryReader.DefaultInstance.parse(in)
    case mt: MapType =>
      val size = in.readInt()
      val entries = new Array[(Any, Any)](size)
      for (i <- 0 until size) {
        val key = readValue(in, mt.keyType)
        val value = readValue(in, mt.valueType)
        entries(i) = (key, value)
      }
      entries.toMap
    case _ => in.readObject()
  }

  /**
   * Initialize the schema from the given parameters where the first field is always the geometry.
   * If names and types are not null, they are simply padded
   * together to create the schema. If any of the types is null, the value is used to detect the type.
   * If the value is also null, the type is set to [[StringType]] by default
   * @param names the list of names. Can be null and can contain nulls.
   * @param types the list of types. Can be null and can contain nulls.
   * @param values the list of values. Can be null and can contain nulls.
   * @return
   */
  private def makeSchema(names: Array[String], types: Array[DataType], values: Array[Any]): StructType = {
    val numAttributes: Int = if (names != null) names.length
    else if (types != null) types.length
    else if (values != null) values.length
    else 0
    val fields = new Array[StructField](numAttributes + 1)
    fields(0) = StructField("g", GeometryDataType)
    for (i <- 0 until numAttributes) {
      var fieldType: DataType = null
      if (types != null && types(i) != null) {
        fieldType = types(i)
      } else if (values != null && values(i) != null) {
        fieldType = inferType(values(i))
      } else if (values(i) == null) {
        fieldType = NullType
      } else {
        fieldType = StringType
      }
      val name: String = if (names == null) null else names(i)
      fields(i + 1) = StructField(name, fieldType)
    }
    StructType(fields)
  }

  protected def inferType(value: Any): DataType = value match {
    case null => NullType
    case _: String => StringType
    case _: Integer | _: Int | _: Byte | _: Short => IntegerType
    case _: java.lang.Long | _: Long => LongType
    case _: java.lang.Double | _: Double | _: Float => DoubleType
    case _: java.sql.Timestamp => TimestampType
    case _: java.sql.Date => DateType
    case _: java.lang.Boolean | _: Boolean => BooleanType
    case _: Geometry => GeometryDataType
    case map: scala.collection.immutable.HashMap[Object, Object] =>
      // Detect the type of the value based on the first value
      val keyType: DataType = inferType(map.keys.head)
      val valueType: DataType = inferType(map.values.head)
      DataTypes.createMapType(keyType, valueType)
    case list: scala.collection.Seq[Object] =>
      // Infer the type of the list based on the first element
      DataTypes.createArrayType(if (list.isEmpty) BinaryType else inferType(list.head))
  }

  /**
   * Create an array of values that contains the given geometry.
   * The list of values is not expected to include a geometry field.
   * @param geometry the geometry element to include in the array of values
   * @param types the list of data types. Can be null
   * @param values the list of values. Can be null
   * @return a list of values with the given geometry included in it
   */
  private def makeValuesArray(geometry: Geometry, types: Array[DataType], values: Array[Any]): Array[Any] = {
    val numAttributes = if (types != null) types.length
    else if (values != null) values.length
    else 0
    if (values != null && numAttributes == values.length)
      geometry +: values
    else {
      val retVal = new Array[Any](numAttributes + 1)
      retVal(0) = geometry
      if (values != null)
        System.arraycopy(values, 0, retVal, 1, values.length)
      retVal
    }
  }
  /**
   * Infer schema from the values. If a value is `null`, the type is inferred as [[BinaryType]]
   * @param values the array of values
   * @return
   */
  private def inferSchema(values: Array[Any]): StructType = StructType(values.zipWithIndex.map(vi => StructField(s"$$${vi._2}", detectType(vi._1))))

  /**
   * Detect the data type for the given value.
   * @param value A value to detect its type
   * @return a detected data type for the given value.
   */
  private def detectType(value: Any): DataType = value match {
    case null => BinaryType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: String => StringType
    case x: java.math.BigDecimal => DecimalType(x.precision(), x.scale())
    case _: java.sql.Date | _: java.time.LocalDate => DateType
    case _: java.sql.Timestamp | _: java.time.Instant => TimestampType
    case _: Array[Byte] => BinaryType
    case r: Row => r.schema
    case _: Geometry => GeometryDataType
    case m: Map[Any, Any] =>
      val keyType: DataType = if (m.isEmpty) StringType else detectType(m.head._1)
      val valueType: DataType = if (m.isEmpty) StringType else detectType(m.head._2)
      MapType(keyType, valueType, valueContainsNull = true)
    case _ => BinaryType
  }

  /**
   * Create a [[Feature]] from the given row and the given geometry.
   * If the row already contains a geometry field, it is overridden.
   * If the row does not contain a geometry field, the geometry is prepended.
   * If the given geometry is null, the original geometry is kept intact.
   * @param row and existing row that might or might not contain a geometry
   * @param geometry the new geometry to use in the created feature
   * @return a [[Feature]] with the given values and geometry
   */
  def create(row: Row, geometry: Geometry): Feature =
    if (row == null) {
      new Feature(Array(geometry), StructType(Seq(StructField("g", GeometryDataType))))
    } else {
      val rowValues: Array[Any] = Row.unapplySeq(row).get.toArray
      val rowSchema: StructType = if (row.schema != null) row.schema else inferSchema(rowValues)
      val iGeom: Int = rowSchema.indexWhere(_.dataType == GeometryDataType)
      if (iGeom == -1) {
        // No geometry field, prepend it
        val values: Array[Any] = geometry +: rowValues
        val schema: Seq[StructField] = Seq(StructField("g", GeometryDataType)) ++ rowSchema
        new Feature(values, StructType(schema))
      } else {
        // A geometry field already exists, replace the geometry
        rowValues(iGeom) = geometry
        new Feature(rowValues, rowSchema)
      }
    }

  /**
   * Concatenates two rows together to form a feature.
   * @param feature a row that contains a geometry
   * @param row another row to append at the end of the feature
   * @return a new feature that combines the values and schema from both
   */
  def concat(feature: IFeature, row: Row): IFeature = {
    val values = Row.unapplySeq(feature).get ++ Row.unapplySeq(row).get
    val schema = feature.schema ++ row.schema
    new Feature(values.toArray, StructType(schema))
  }

  /**
   * Appends an additional attribute to the given feature and returns a new feature
   * @param feature the feature to append to. This feature is not modified.
   * @param value the value to append.
   * @param name (Optional) the name of the new attribute
   * @param dataType (Optional) the type of the additional attribute.
   * @return a new feature that contains the geometry and all attributes of the input feature + the new attribute.
   */
  def append(feature: IFeature, value: Any, name: String = null, dataType: DataType = null): IFeature = {
    // Appends a single value to an existing feature
    val values: Seq[Any] = Row.unapplySeq(feature).get :+ value
    val schema: Seq[StructField] = feature.schema :+ StructField(name, if (dataType != null) dataType else detectType(value))
    new Feature(values.toArray, StructType(schema))
  }

  def create(geometry: Geometry, _names: Array[String], _types: Array[DataType], _values: Array[Any]): Feature =
    new Feature(Feature.makeValuesArray(geometry, _types, _values), Feature.makeSchema(_names, _types, _values))

}