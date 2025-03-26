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

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry

import java.util
import scala.reflect.ClassTag

object RaptorJoin {

  /**
   * Performs a join between the given raster and vector data. The vector data contains a unique ID for each feature
   * which is returned by this function.
   * The return value is a distributed set that contains pairs of feature ID and raster measure value for each
   * pixel that matches a feature.
   * @param raster a set of tiles in an RDD[ITile]
   * @param featureIDs a set of (ID, feature) pairs.
   * @param opts additional options for the algorithm
   * @param numTiles an optional accumulator that counts the total number of tiles read
   * @return a set of (Feature ID, pixel) pairs in the form of (FeatureID, RasterFileID, x, y, m)
   */
  def raptorJoinIDM[T](raster: RasterRDD[T], featureIDs: RDD[(Long, IFeature)], opts: BeastOptions = new BeastOptions(),
                     numTiles: LongAccumulator = null) : RDD[(Long, T)] =
    raptorJoinIDFull[T](raster, featureIDs, opts, numTiles)
      .map(x => (x.featureID, x.m))

  /**
   * A shortcut for Raptor join of raster and vector RDDs to Java.
   * @param raster the raster RDD
   * @param vector a vector RDD that contains pairs of unique feature ID and the feature.
   * @param opts additional options to pass to the query processes
   * @tparam T the type of the return value which is the same as the type of the pixel value
   * @return an RDD of type [[RaptorJoinResult]] which contains all pixels that match with the given features.
   */
  def raptorJoinIDMJ[T](raster: JavaRDD[ITile[T]], vector: JavaPairRDD[java.lang.Long, IFeature], opts: BeastOptions)
      : JavaPairRDD[java.lang.Long, T] = {
    implicit val ctagK: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
    JavaPairRDD.fromRDD(raptorJoinIDM[T](raster.rdd, vector.rdd.map(p => (p._1.longValue(), p._2)), opts)
      .map(p => (java.lang.Long.valueOf(p._1), p._2)))
  }

  /**
   * Performs a raptor join between a raster RDD and a set of features. The output contains information about all
   * pixels that match with the set of features.
   * @param raster the raster RDD that contains all the tiles to test
   * @param features the set of features to join with the raster data
   * @param opts additional options for the query processor
   * @param numTiles an optional accumulator to count the number of tiles accesses during the query processing.
   * @tparam T the type of the pixel values
   * @return the set of overlaps between pixels and features
   */
  def raptorJoinFeature[T](raster: RasterRDD[T], features: RDD[IFeature], opts: BeastOptions = new BeastOptions(),
                           numTiles: LongAccumulator = null): RDD[RaptorJoinFeature[T]] = {
    val featureIDs: RDD[(Long, IFeature)] = features.zipWithUniqueId().map(x => (x._2, x._1))
    val joinResults = raptorJoinIDFull[T](raster, featureIDs, opts, numTiles)
      .map(x => (x.featureID, x))
    val partitioner = new HashPartitioner(joinResults.getNumPartitions max featureIDs.getNumPartitions max featureIDs.sparkContext.defaultParallelism)
    featureIDs.partitionBy(partitioner)
      .zipPartitions(joinResults.partitionBy(partitioner))((fs, rjs) => {
      val fsMap = fs.toMap
      rjs.map(rj => {
        RaptorJoinFeature(fsMap(rj._1), rj._2.rasterMetadata, rj._2.x, rj._2.y, rj._2.m)
      })
    })
  }

  /**
   * Java shortcut.
   * Performs a raptor join between a raster RDD and a set of features. The output contains information about all
   * pixels that match with the set of features.
   * @param raster the raster RDD that contains all the tiles to test
   * @param features the set of features to join with the raster data
   * @param opts additional options for the query processor
   * @tparam T the type of the pixel values
   * @return the set of overlaps between pixels and features
   */
  def raptorJoinFeatureJ[T](raster: JavaRDD[ITile[T]], features: JavaRDD[IFeature], opts: BeastOptions):
      JavaRDD[RaptorJoinFeature[T]] = {
    JavaRDD.fromRDD(raptorJoinFeature[T](raster, features.rdd, opts, null))
  }

  def raptorJoinFeatureM[T](raster: RasterRDD[T], features: RDD[IFeature], opts: BeastOptions = new BeastOptions(),
                            numTiles: LongAccumulator = null)(implicit t: ClassTag[T]) : RDD[(IFeature, T)] = {
    val featureIDs: RDD[(Long, IFeature)] = features.zipWithUniqueId().map(x => (x._2, x._1))
    val rjResult: RDD[(Long, T)] = raptorJoinIDM[T](raster, featureIDs, opts, numTiles)
    val partitioner = new HashPartitioner(featureIDs.getNumPartitions max rjResult.getNumPartitions max raster.sparkContext.defaultParallelism)
    featureIDs.partitionBy(partitioner).zipPartitions(rjResult.partitionBy(partitioner))((fs, rjs) =>{
      val allFeatures = fs.toMap
      rjs.map(rj => {
        val f = allFeatures(rj._1)
        (f, rj._2)
      })
    })
  }

  /**
   * A Raptor join implementation that returns all the matches between features and pixels along with the raster
   * metadata that puts the pixel in context.
   * @param raster the RDD that contains the raster tiles
   * @param vector the RDD that contains the vector features and their unique IDs
   * @param opts additional options for the query processor
   * @tparam T the type of the pixel values
   * @return RDD that contains all overlaps between pixels and geometries
   */
  def raptorJoinIDFullTile[T](raster: RDD[ITile[T]], vector: RDD[(Long, IFeature)], opts: BeastOptions,
                          numTiles: LongAccumulator = null, numRanges: LongAccumulator = null)
                             (implicit @transient t: ClassTag[T])
  : RDD[RaptorJoinResultTile[T]] = {
    import edu.ucr.cs.bdlab.raptor.RaptorMixin._
    val allMetadata: Array[RasterMetadata] = raster.allMetadata
    val intersectionsRDD: RDD[(Long, CompactIntersections)] = vector
      .partitionBy(new HashPartitioner(vector.getNumPartitions max raster.getNumPartitions max raster.sparkContext.defaultParallelism))
      .mapPartitions { idFeatures: Iterator[(Long, IFeature)] => {
        val ids = new util.ArrayList[java.lang.Long]()
        val geoms: Array[Geometry] = idFeatures.map(x => {
          ids.add(x._1)
          x._2.getGeometry
        }).toArray

        new CompactIntersectionsTileBreaker(allMetadata.indices.iterator.map(i => {
          val rasterMetadata = allMetadata(i)
          val intersections = new Intersections
          intersections.compute(ids, geoms, rasterMetadata)
          if (intersections.getNumIntersections == 0)
            (-1, null)
          else
            (i, intersections)
        }).filterNot(_._1 == -1))
      }}

    val partitioner = new HashPartitioner(intersectionsRDD.getNumPartitions max raster.getNumPartitions max raster.sparkContext.defaultParallelism)
    // Same format (Tile ID, (Geometry ID, Y, X1, X2))
    val orderedIntersections: RDD[(Long, CompactIntersections)] =
      intersectionsRDD.repartitionAndSortWithinPartitions(partitioner)

    // Partition tiles to match the intersections
    val metadataToID: Map[RasterMetadata, Int] = allMetadata.zipWithIndex.toMap
    val orderedTiles: RDD[(Long, ITile[T])] = raster.map(tile => {
      assert(metadataToID.contains(tile.rasterMetadata), s"Unexpected! Metadata ${tile.rasterMetadata} " +
        s"not found in ${metadataToID.keys.mkString("\n")}")
      val rasterID = metadataToID(tile.rasterMetadata)
      val rasterTileID = (rasterID.toLong << 32) | tile.tileID
      (rasterTileID, tile)
    }).repartitionAndSortWithinPartitions(partitioner)

    orderedIntersections.zipPartitions(orderedTiles, true)( (intersections, tiles) =>
      new TileIterator[T](new CompactIntersectionsIterator(intersections), tiles, numTiles, numRanges))
  }

  /**
   * A Raptor join implementation that returns all the matches between features and pixels along with the raster
   * metadata that puts the pixel in context.
   * @param raster the RDD that contains the raster tiles
   * @param vector the RDD that contains the vector features and their unique IDs
   * @param opts additional options for the query processor
   * @tparam T the type of the pixel values
   * @return RDD that contains all overlaps between pixels and geometries
   */
  def raptorJoinIDFull[T](raster: RDD[ITile[T]], vector: RDD[(Long, IFeature)], opts: BeastOptions,
                          numTiles: LongAccumulator = null, numRanges: LongAccumulator = null)
                     : RDD[RaptorJoinResult[T]] = {
    import edu.ucr.cs.bdlab.raptor.RaptorMixin._
    val allMetadata: Array[RasterMetadata] = raster.allMetadata
    val intersectionsRDD: RDD[(Long, CompactIntersections)] = vector
      .partitionBy(new HashPartitioner(vector.getNumPartitions max raster.getNumPartitions max raster.sparkContext.defaultParallelism))
      .mapPartitions { idFeatures: Iterator[(Long, IFeature)] => {
        val ids = new util.ArrayList[java.lang.Long]()
        val geoms: Array[Geometry] = idFeatures.map(x => {
          ids.add(x._1)
          x._2.getGeometry
        }).toArray

        new CompactIntersectionsTileBreaker(allMetadata.indices.iterator.map(i => {
          val rasterMetadata = allMetadata(i)
          val intersections = new Intersections
          intersections.compute(ids, geoms, rasterMetadata)
          if (intersections.getNumIntersections == 0)
            (-1, null)
          else
            (i, intersections)
        }).filterNot(_._1 == -1))
      }}

    val partitioner = new HashPartitioner(intersectionsRDD.getNumPartitions max raster.getNumPartitions max raster.sparkContext.defaultParallelism)
    // Same format (Tile ID, (Geometry ID, Y, X1, X2))
    val orderedIntersections: RDD[(Long, CompactIntersections)] =
      intersectionsRDD.repartitionAndSortWithinPartitions(partitioner)

    // Partition tiles to match the intersections
    val metadataToID: Map[RasterMetadata, Int] = allMetadata.zipWithIndex.toMap
    val orderedTiles: RDD[(Long, ITile[T])] = raster.map(tile => {
      assert(metadataToID.contains(tile.rasterMetadata), s"Unexpected! Metadata ${tile.rasterMetadata} " +
        s"not found in ${metadataToID.keys.mkString("\n")}")
      val rasterID = metadataToID(tile.rasterMetadata)
      val rasterTileID = (rasterID.toLong << 32) | tile.tileID
      (rasterTileID, tile)
    }).repartitionAndSortWithinPartitions(partitioner)

    orderedIntersections.zipPartitions(orderedTiles, true)( (intersections, tiles) =>
      new PixelIterator3[T](new CompactIntersectionsIterator(intersections), tiles, numTiles, numRanges))
  }

  def raptorJoinIDFullJ[T](raster: JavaRDD[ITile[T]], vector: JavaPairRDD[java.lang.Long, IFeature], opts: BeastOptions):
    JavaRDD[RaptorJoinResult[T]] = {
    JavaRDD.fromRDD(raptorJoinIDFull[T](raster.rdd, vector.rdd.map(x => (x._1.longValue(), x._2)), opts, null))
  }

  /**
   * Runs a local version of Raptor join that does not require any Spark or RDD processing.
   * This method should run faster for small datasets since it does not incur the parallelization overhead.
   * However, it runs on a single machine and require more memory.
   * @param rasters the list of raster files to process
   * @param features the list of features
   * @tparam T the type of pixels
   * @return an iterator over all results
   */
  def raptorJoinLocal[T](rasters: Array[IRasterReader[T]], features: Array[IFeature]): Iterator[RaptorJoinFeature[T]] =
    raptorJoinLocal(rasters, features.map(_.getGeometry))
      .map(result =>
        RaptorJoinFeature(features(result.featureID.toInt), result.rasterMetadata, result.x, result.y, result.m))

  /**
   * Runs a local version of Raptor join that does not require any Spark or RDD processing.
   * This method should run faster for small datasets since it does not incur the parallelization overhead.
   * However, it runs on a single machine and require more memory.
   * @param rasters the list of raster files to process
   * @param geometries the list of geometries
   * @tparam T the type of pixels
   * @return an iterator over all results
   */
  def raptorJoinLocal[T](rasters: Array[IRasterReader[T]], geometries: Array[Geometry]): Iterator[RaptorJoinResult[T]] = {
    // Since features are in one array, we will use the array index as the unique ID for each geometry
    val rastersByMetadata = rasters.groupBy(_.metadata)
    rastersByMetadata.iterator.flatMap(metadataRasters => {
      // Compute intersections
      val intersections = new Intersections
      intersections.compute(geometries, metadataRasters._1)
      // Retrieve the corresponding rasters and process all of them
      metadataRasters._2.iterator.flatMap(raster => {
        // Now process the intersections
        import scala.collection.JavaConverters._
        var tile: ITile[T] = null
        intersections.iterator().asScala.flatMap(range => {
          // Read the tile if needed
          if (tile == null || tile.tileID != range.tileID)
            tile = raster.readTile(range.tileID)
          // Return all pixels in the given range
          (range.x1 to range.x2).iterator.map(x => {
            val m: T = tile.getPixelValue(x, range.y)
            RaptorJoinResult(range.geometryID, raster.metadata, x, range.y, m)
          })
        })
      })
    })
  }

  /**
   * Runs a local version of Raptor join that does not require any Spark or RDD processing.
   * This method should run faster for small datasets since it does not incur the parallelization overhead.
   * However, it runs on a single machine and require more memory.
   * @param rasters the list of raster files to process
   * @param geometries the list of geometries
   * @tparam T the type of pixels
   * @return an iterator over all results
   */
  def raptorJoinLocalJ[T](rasters: Array[IRasterReader[T]], geometries: Array[Geometry]):
      java.util.Iterator[RaptorJoinResult[T]] = {
    import scala.collection.JavaConverters._
    raptorJoinLocal(rasters, geometries).asJava
  }

  /**
   * Runs a local version of Raptor join that does not require any Spark or RDD processing.
   * This method should run faster for small datasets since it does not incur the parallelization overhead.
   * However, it runs on a single machine and require more memory.
   * @param rasters the list of raster files to process
   * @param features the list of features
   * @tparam T the type of pixels
   * @return an iterator over all results
   */
  def raptorJoinLocalJ[T](rasters: Array[IRasterReader[T]], features: Array[IFeature]):
    java.util.Iterator[RaptorJoinFeature[T]] = {
    import scala.collection.JavaConverters._
    raptorJoinLocal(rasters, features).asJava
  }
}
