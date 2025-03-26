/*
 * Copyright 2022 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.spark.sql.types.DataType

/**
 * A wrapper that makes a tile look like it is a single tile in a file.
 * The resulting tile will always have a tile ID of zero since it is the only tile in the metadata.
 * The x and y ranges will always start at zero.
 * @param tile the tile that we want to make look like its own tile.
 * @tparam T the type of measurement values in tiles
 */
class ExplodedTile[T](tile: ITile[T]) extends ITile[T](0, tile.rasterMetadata.forOneTile(tile.tileID)) {
  override def getPixelValue(i: Int, j: Int): T = tile.getPixelValue(i + tile.x1, j + tile.y1)

  override def isEmpty(i: Int, j: Int): Boolean = tile.isEmpty(i + tile.x1, j + tile.y1)

  override def numComponents: Int = tile.numComponents

  override def componentType: DataType = tile.componentType
}
