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

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.AbstractTiffTile

/**
 * An abstract class for GeoTIFF tiles. Subclasses define what value and type to return for each pixel.
 * @param tileID the ID of the tile within the tile grid
 * @param tiffTile the underlying tiff tile
 * @param fillValue the fill value in the tiff tile that marks empty pixels
 * @param pixelBitMask a bit mask that tells which bits are defined (1) or empty (0)
 * @param metadata the metadata of the raster file
 */
abstract class AbstractGeoTiffTile[T](tileID: Int, private[raptor] val tiffTile: AbstractTiffTile,
                                      private[raptor] val fillValue: Int, private[raptor] val pixelBitMask: Array[Byte],
                                      metadata: RasterMetadata)
  extends ITile[T](tileID, metadata) {

  override def isEmpty(i: Int, j: Int): Boolean = {
    if (pixelBitMask != null) {
      val pixelLocation = (j - y1) * tileWidth + (i - x1)
      val byteIndex = pixelLocation >> 3
      val bitOffset = pixelLocation & 7
      (pixelBitMask(byteIndex) & (0x80.toByte >>> bitOffset)) == 0
    } else {
      for (c <- 0 until numComponents) {
        if (tiffTile.getSampleValueAsInt(i, j, c) == fillValue)
          return true
      }
      false
    }
  }

  override def numComponents: Int = tiffTile.getNumSamples
}
