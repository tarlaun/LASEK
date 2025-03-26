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
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import org.apache.spark.rdd.RDD

import scala.language.postfixOps
import scala.reflect.ClassTag

/**
 * Raster global operations
 */
object RasterOperationsGlobal {
  /**
   * Create a raster from a set of pixel values
   * @param pixels the list of pixel locations and values
   * @param metadata the raster metadata that defines the geography of the pixels
   * @return a raster that contains all the pixels
   */
  def rasterizePixels[T: ClassTag](pixels: RDD[(Int, Int, T)], metadata: RasterMetadata): RasterRDD[T] = {
    val pixelsAssignedToTiles: RDD[(Int, (Int, Int, T))] = pixels.map(p => {
      val tileID = metadata.getTileIDAtPixel(p._1, p._2)
      (tileID, p)
    })
    val zeroValues = Array[(Int, Int, T)]()
    pixelsAssignedToTiles.aggregateByKey(zeroValues)(
      (u: Array[(Int, Int, T)], v: (Int, Int, T)) => u :+ ((v._1, v._2, v._3)),
      (u1, u2) => {
        u1 :+ u2
        u1
      }
    ).map(t => {
      val tile = new MemoryTile[T](t._1, metadata)
      for (p <- t._2)
        tile.setPixelValue(p._1, p._2, p._3)
      tile
    })
  }

  /**
   * Creates a raster from a list of point locations and values.
   * @param points point locations and raster values
   * @param metadata the metadata that describes the raster location
   * @tparam T the type of raster values
   * @return a raster that contains the given point locations
   */
  def rasterizePoints[T: ClassTag](points: RDD[(Double, Double, T)], metadata: RasterMetadata): RasterRDD[T] = {
    rasterizePixels(points.map(p => {
      val geolocation = Array(p._1, p._2)
      metadata.g2m.inverseTransform(geolocation, 0, geolocation, 0, 1)
      (geolocation(0).toInt, geolocation(1).toInt, p._3)
    }), metadata)
  }

  /**
   * Extract all pixel values into an RDD
   * @param raster the raster to extract its pixels
   * @tparam T the type of pixel values
   * @return an RDD that contains all pixel locations and values
   */
  def flatten[T](raster: RasterRDD[T]): RDD[(Int, Int, RasterMetadata, T)] = {
    raster.flatMap(tile => {
      for (y <- tile.y1 to tile.y2 iterator; x <- tile.x1 to tile.x2 iterator; if tile.isDefined(x, y))
        yield (x, y, tile.rasterMetadata, tile.getPixelValue(x, y))
    })
  }
}
