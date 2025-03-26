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

import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}

/**
 * Results of raptor join that also contains the raster metadata which puts the pixel in spatial context.
 * @param featureID the ID of the feature
 * @param rasterMetadata the metadata of the raster file that contains the pixel
 * @param x the x-coordinate of the pixel (index of the column in the grid space)
 * @param y the y-coordinate of the pixel (index of the row in the grid space)
 * @param m the value of the pixel from the raster file
 * @tparam T the type of the pixel value
 */
case class RaptorJoinResult[T](featureID: Long, rasterMetadata: RasterMetadata, x: Int, y: Int, m: T)

case class RaptorJoinResultTile[T](featureID: Long, tile: ITile[T])

case class RaptorJoinFeature[T](feature: IFeature, rasterMetadata: RasterMetadata, x: Int, y: Int, m: T)