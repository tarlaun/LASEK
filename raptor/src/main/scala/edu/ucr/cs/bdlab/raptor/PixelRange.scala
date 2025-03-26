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

/**
 * A range of pixels that matches a geometry
 * @param geometryID the ID of the geometry
 * @param y the index of the row (scanline) in the raster file
 * @param x1 the index of the first column inclusive
 * @param x2 the index of the last column inclusive
 */
case class PixelRange(geometryID: Long, y:  Int, x1: Int, x2: Int)
