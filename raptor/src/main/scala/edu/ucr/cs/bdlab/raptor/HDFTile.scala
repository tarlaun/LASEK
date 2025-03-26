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
package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{ITile, ITileSerializer, RasterMetadata}
import org.apache.spark.sql.types.{DataType, FloatType}

/**
 * A tile from an HDF file.
 * @param tileID the ID of this tile in the raster metadata
 * @param data the array that contains all HDF data
 * @param valueSize the size of the value
 * @param fillValue the value used to mark empty pixels
 * @param scaleFactor the scale factor used to convert raw values to float
 * @param metadata the metadata of the underlying raster
 */
@DefaultSerializer(classOf[ITileSerializer[Any]])
class HDFTile(tileID: Int, data: Array[Byte], valueSize: Int,
              fillValue: Int, scaleFactor: Double, metadata: RasterMetadata) extends ITile[Float](tileID, metadata) {

  lazy val resolution: Int = tileWidth

  override def getPixelValue(i: Int, j: Int): Float = {
    val intVal = valueSize match {
      case 1 => data((j * resolution + i) * valueSize) & 0xff
      case 2 => getShort((j * resolution + i) * valueSize)
      case 4 => getInt((j * resolution + i) * valueSize)
    }
    (intVal * scaleFactor).toFloat
  }

  override def isEmpty(i: Int, j: Int): Boolean = {
    valueSize match {
      case 1 =>
        data((j * resolution + i) * valueSize) == fillValue
      case 2 =>
        getShort((j * resolution + i) * valueSize) == fillValue
      case 4 =>
        getInt((j * resolution + i) * valueSize) == fillValue
    }
  }

  override def numComponents: Int = 1

  override def componentType: DataType = FloatType

  def getShort(i: Int): Short = {
    (((data(i) & 0xff) << 8) | ((data(i+1) & 0xff))).toShort
  }

  def getInt(i: Int): Short = {
    (data(i+3) & 0xff |
      (data(i+2) & 0xff << 8) |
      (data(i+1) & 0xff << 16) |
      (data(i) & 0xff << 24)).toShort
  }
}
