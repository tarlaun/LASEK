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
package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{RasterRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.raptor.RasterOperationsFocal.InterpolationMethod
import org.apache.spark.SparkContext
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.reflect.ClassTag

trait RaptorMixin {

  implicit class RasterReadMixinFunctions(sc: SparkContext) {
    /**
     * Loads a GeoTIFF file as an RDD of tiles
     * @param path the path of the file
     * @param iLayer the index of the band to load (0 by default)
     * @param opts additional options for loading the file
     * @return a [[RasterRDD]] that represents all tiles in the file
     */
    def geoTiff[T](path: String, iLayer: Int = 0, opts: BeastOptions = new BeastOptions): RDD[ITile[T]] = {
      if (!opts.contains(IRasterReader.RasterLayerID))
        opts.set(IRasterReader.RasterLayerID, iLayer)
      new RasterFileRDD[T](sc, path, opts)
    }

    def hdfFile(path: String, layer: String, opts: BeastOptions = new BeastOptions()): RDD[ITile[Float]] = {
      opts.set(IRasterReader.RasterLayerID, layer)
      new RasterFileRDD(sc, path, opts)
    }

    /**
     * Creates a [[RasterRDD]] from a given set of pixel locations and values.
     * @param pixels the pixel values
     * @param metadata the metadata that describes the raster
     * @tparam T the type of pixels
     * @return a raster RDD that holds the given pixels
     */
    def rasterizePixels[T: ClassTag](pixels: RDD[(Int, Int, T)], metadata: RasterMetadata): RasterRDD[T] =
      RasterOperationsGlobal.rasterizePixels(pixels, metadata)

    /**
     * Creates a raster from a list of point locations and values.
     * @param points point locations and raster values
     * @param metadata the metadata that describes the raster location
     * @tparam T the type of raster values
     * @return a raster that contains the given point locations
     */
    def rasterizePoints[T: ClassTag](points: RDD[(Double, Double, T)], metadata: RasterMetadata): RasterRDD[T] =
      RasterOperationsGlobal.rasterizePoints(points, metadata)
  }

  implicit class RaptorMixinOperations3[T](raster: RasterRDD[T]) {
    /**
     * Returns the spatial reference ID (SRID) of the raster data.
     * This function returns the SRID of the first tile assuming that all tiles have the same SRID.
     * @return the SRID of the raster data or 0 if SRID is unknown or undefined
     */
    def getSRID: Int = raster.first().rasterMetadata.srid

    /**
     * Performs a Raptor join operation with a set of vector features.
     * @param features the set of features to join with
     * @param opts additional options to configure the operation
     * @tparam T the type of the value in the result. Should be compatible with the pixel type of the raster
     * @return all overlaps between the given features and the pixels.
     */
    def raptorJoin(features: SpatialRDD, opts: BeastOptions = new BeastOptions)(implicit t: ClassTag[T]):
      RDD[RaptorJoinFeature[T]] =
      RaptorJoin.raptorJoinFeature[T](raster, features, opts, null)

    /**
     * Overlays this raster RDD on top other ones
     * @param rasters the other rasters to stack this raster on
     * @return a new RasterRDD which contains the stack of this raster on top of the given ones
     */
    def overlay[V](rasters: RasterRDD[T]*)(implicit t: ClassTag[T], v: ClassTag[V]): RasterRDD[Array[V]] =
      RasterOperationsLocal.overlay((Seq(raster) ++ rasters):_*)

    /**
     * Save this RasterRDD as a set of geoTIFF files.
     * @param path the path to write the output geotiff files to
     * @param opts additional options for saving the GeoTIFF file
     */
    def saveAsGeoTiff(path: String, opts: BeastOptions = new BeastOptions): Unit =
      GeoTiffWriter.saveAsGeoTiff[T](raster, path, opts)

    /**
     * Returns an array of all unique metadata in this RDD
     * @return
     */
    def allMetadata: Array[RasterMetadata] = RasterMetadata.allMetadata(raster)

    /**
     * Applies a mathematical function to each pixel in the input and produce the output raster.
     * @param f the function to apply on each pixel of the input
     * @tparam U the type of the output parameter
     * @return a new raster after the function is applied on each pixel
     */
    def mapPixels[U: ClassTag](f: T => U)(implicit t: ClassTag[T]): RasterRDD[U] =
      RasterOperationsLocal.mapPixels(raster, f)

    /**
     * Removes pixels that do not pass the given filter. Each pixel in the input raster is tested using the given
     * function. If it returns true, the pixel is kept as-is. If it returns false, the filter is removed.
     * @param f the filter that tests which pixels to keep
     * @return a new raster with the same input resolution after removing non-matching pixels.
     */
    def filterPixels(f: T => Boolean)(implicit t: ClassTag[T]): RasterRDD[T] =
      RasterOperationsLocal.filterPixels(raster, f)

    /**
     * Regrids the given raster to the target tile width and height
     * @param tileWidth the new tile width in pixels
     * @param tileHeight the new tile height in pixels
     * @return a new raster with the given tile width and height
     */
    def retile(tileWidth: Int, tileHeight: Int)(implicit t: ClassTag[T]): RasterRDD[T] =
      RasterOperationsFocal.retile(raster, tileWidth, tileHeight)

    /**
     * Reproject a raster to a target coordinate reference system.
     * This method uses the same resolution (number of pixels) of the first tile in the source raster.
     * @param targetSRID the spatial reference identifier (SRID) of the desired coordinate reference system.
     * @return a new raster RDD with the desired coordinate reference system.
     */
    def reproject(targetSRID: Int)(implicit t: ClassTag[T]): RasterRDD[T] =
      RasterOperationsFocal.reproject(raster, CRSServer.sridToCRS(targetSRID))

    /**
     * Reproject a raster to a target coordinate reference system.
     * This method uses the same resolution (number of pixels) of the first tile in the source raster.
     * @param targetCRS the desired coordinate reference system
     * @return a new raster RDD with the desired coordinate reference system.
     */
    def reproject(targetCRS: CoordinateReferenceSystem, unifiedRaster: Boolean = false,
                  interpolationMethod: InterpolationMethod.InterpolationMethod =
        InterpolationMethod.NearestNeighbor)(implicit t: ClassTag[T]): RasterRDD[T] =
      RasterOperationsFocal.reproject(raster, targetCRS, unifiedRaster, interpolationMethod)

    /**
     * Changes the resolution of the raster to the desired resolution without changing tile size or CRS.
     * @param rasterWidth the new raster width in terms of pixels
     * @param rasterHeight the new height of the raster layer in terms of pixels
     * @return a new raster RDD with the desired width and height
     */
    def rescale(rasterWidth: Int, rasterHeight: Int, unifiedRaster: Boolean = false,
                interpolationMethod: InterpolationMethod.InterpolationMethod =
        InterpolationMethod.NearestNeighbor)(implicit t: ClassTag[T]): RasterRDD[T] =
      RasterOperationsFocal.rescale(raster, rasterWidth, rasterHeight, unifiedRaster, interpolationMethod)

    /**
     * Extract all pixel values into an RDD
     * @return an RDD that contains all pixel locations and values
     */
    def flatten: RDD[(Int, Int, RasterMetadata, T)] = RasterOperationsGlobal.flatten(raster)

    /**
     * Explodes the given raster RDD into separate tiles that are ready to be written to separate files.
     * @return
     */
    def explode: RasterRDD[T] = RasterOperationsLocal.explode(raster)
  }
}

object RaptorMixin extends RaptorMixin
