/*
 * Copyright 2021 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import org.apache.hadoop.conf.Configuration
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTWriter

import java.io.OutputStream

@FeatureWriter.Metadata(extension = ".json", shortName = "jsonwkt")
class JsonWKTWriter extends MultipartFeatureWriter {

  private var jsonGenerator: JsonGenerator = _

  private val writer = new WKTWriter()

  override def initialize(out: OutputStream, conf: Configuration): Unit = {
    super.initialize(out, conf)
    jsonGenerator = new JsonFactory().createGenerator(out)
  }

  override protected def isCompressible: Boolean = true

  override def writeMiddle(feature: IFeature): Unit = {
    jsonGenerator.writeStartObject()
    for (i <- 0 until feature.length; if !feature.isNullAt(i)) {
      val name = feature.schema(i).name
      jsonGenerator.writeFieldName(if (name != null) name else s"attr#$i")
      val value = feature.get(i)
      feature.schema(i).dataType match {
        case _: BooleanType => jsonGenerator.writeBoolean(value.asInstanceOf[Boolean])
        case _: ByteType => jsonGenerator.writeNumber(value.asInstanceOf[Byte])
        case _: ShortType => jsonGenerator.writeNumber(value.asInstanceOf[Short])
        case _: IntegerType => jsonGenerator.writeNumber(value.asInstanceOf[Integer])
        case _: LongType => jsonGenerator.writeNumber(value.asInstanceOf[Long])
        case _: FloatType => jsonGenerator.writeNumber(value.asInstanceOf[Float])
        case _: DoubleType => jsonGenerator.writeNumber(value.asInstanceOf[Double])
        case _: StringType => jsonGenerator.writeString(value.asInstanceOf[String])
        case _: GeometryDataType => jsonGenerator.writeString(writer.write(value.asInstanceOf[Geometry]))
        case _ => jsonGenerator.writeString(value.toString)
      }
      // TODO write StructType, MapType and ArrayType as proper JSON data
    }
    jsonGenerator.writeEndObject()
    jsonGenerator.writeRaw('\n')
  }

  override def close(): Unit = {
    super.close()
    jsonGenerator.close()
  }
}
