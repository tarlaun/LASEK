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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.ITile
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{CoGroupedRDD, RDD}

import scala.annotation.varargs
import scala.reflect.ClassTag

/**
 * Raster local operations
 */
object RasterOperationsLocal {
  /**
   * Apply a user-defined function for each pixel in the input raster to produce the output raster
   * @param inputRaster the input raster RDD
   * @param f the function to apply on each input pixel value to produce the output pixel value
   * @tparam T the type of pixels in the input
   * @tparam U the type of pixels in the output
   * @return the resulting RDD
   */
  def mapPixels[T: ClassTag, U: ClassTag](inputRaster: RasterRDD[T], f: T => U): RasterRDD[U] =
    inputRaster.map(inputTile => new MapPixelsTile(inputTile, f))

  /**
   * Retains only the pixels that pass the user-defined filter and clears all other pixels (set to empty)
   * @param inputRaster the input raster
   * @param filter the filter function that tells which pixel values to keep in the output
   * @tparam T the thpe of the pixels in the input
   * @return a new raster where only pixels that pass the test are retained
   */
  def filterPixels[T: ClassTag](inputRaster: RasterRDD[T], filter: T => Boolean): RasterRDD[T] =
    inputRaster.map(inputTile => new FilterTile(inputTile, filter))

  /**
   * Overlays two raster layers that have equivalent metadata.
   * If the two inputs are not compatible, i.e., with different metadata, this function will fail at execution time.
   * An output pixel is defined if the corresponding inputs in the two rasters are both defined.
   * In that case, the pixel value is the concatenation of both values.
   * @param inputs the RDDs to overlay
   * @return a raster with the same metadata of the inputs where output pixels are the concatenation of input pixels.
   */
  def overlay[T: ClassTag, V](@varargs inputs: RDD[ITile[T]]*)
                                               (implicit v: ClassTag[V]): RasterRDD[Array[V]] = {
    val inputsByID: Seq[RDD[(Int, ITile[T])]] = inputs.map(i => i.map(t => (t.tileID, t)))
    val partitioner = Partitioner.defaultPartitioner(inputsByID.head, inputsByID:_*)
    val grouped: CoGroupedRDD[Int] = new CoGroupedRDD(inputsByID, partitioner)
    grouped.map(ts => {
      val tiles: Array[ITile[T]] = ts._2.flatMap(x => x.map(_.asInstanceOf[ITile[T]]))
      new StackedTile[V](tiles:_*)
    })
  }

  /**
   * Returns a new RasterRDD where each tile is in its own raster.
   * @param inputRaster the raster data to explore
   * @tparam T
   * @return a new raster RDD with the same number of tiles but each tile is in a separate raster
   */
  def explode[T](inputRaster: RasterRDD[T]): RasterRDD[T] = inputRaster.map(tile => new ExplodedTile(tile))
}
