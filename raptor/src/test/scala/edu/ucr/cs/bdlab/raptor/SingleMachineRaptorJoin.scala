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

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

/**
 * A single-machine implementation of Raptor join. This function runs as follows:
 *  - Read the entire vector data and keep it in memory
 *  - Create the JIT-index based on the metadata of the raster files
 *  - Process the JIT-index with the raster file to produce pixels
 * This method is used mainly for to compare the Raptor join approach to optimized single-machine implementations.
 */
object SingleMachineRaptorJoin {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val t0: Long = System.nanoTime()
    val vectorFileName: String = "tl_2018_us_state.zip"
    val rasterFileNames: Array[String] = Array("glc2000_v1_1.tif")
    val inputVector: Array[IFeature] = SpatialFileRDD.readLocal(vectorFileName, "shapefile",
      new BeastOptions(), conf).toArray
    val t1: Long = System.nanoTime()
    val intersections: Array[Intersections] = rasterFileNames.map(rasterFileName => {
      val rasterFS: FileSystem = new Path(rasterFileName).getFileSystem(conf)
      val rasterReader = RasterHelper.createRasterReader(rasterFS, new Path(rasterFileName), new BeastOptions())
      val intersections = new Intersections()
      intersections.compute(inputVector.map(_.getGeometry), rasterReader.metadata)
      intersections
    })
    val t2: Long = System.nanoTime()
    val intersectionIterator: Iterator[(Long, PixelRange)] = new IntersectionsIterator(rasterFileNames.indices.toArray, intersections)
    val pixelIterator: Iterator[RaptorJoinResult[Float]] = new PixelIterator[Float](intersectionIterator, rasterFileNames, "0")
    val size = pixelIterator.length
    val t3: Long = System.nanoTime()
    println(s"Result size is ${size}. Vector reading time ${(t1-t0)*1E-9}. JIT index built in ${(t2-t1)*1E-9} second. Total time ${(t3-t0)*1E-9} seconds")
  }
}
