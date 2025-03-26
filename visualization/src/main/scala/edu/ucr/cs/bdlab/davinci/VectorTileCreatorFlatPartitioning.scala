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
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.util.LRUCache
import org.apache.spark.internal.Logging
import org.geotools.referencing.operation.transform.AffineTransform2D

import scala.collection.mutable

/**
 * Creates vector tiles progressively on partitioned features. The input is an iterator of (TileID: Long, Feature)
 * and the output is an iterator of (TileID: Long, IntermediateVectorTile).
 *
 * It is assumed that the partitioned features have already been mapped to the image space within the specified
 * resolution and buffer.
 *
 * @param partitionedFeatures the features that are already sorted by TileID
 * @param resolution the resolution of the vector tiles in pixels
 * @param buffer the additional buffer around each vector tile in pixels
 */
class VectorTileCreatorFlatPartitioning(partitionedFeatures: Iterator[(Long, IFeature)], resolution: Int, buffer: Int)
    extends Iterator[(Long, IntermediateVectorTile)] with Logging {

  /**
   * Tiles that need to be returned
   */
  var tilesToReturn: mutable.Buffer[(Long, IntermediateVectorTile)] =
    new mutable.ListBuffer[(Long, IntermediateVectorTile)]()

  val tilesInProgress: LRUCache[Long, IntermediateVectorTile] = new LRUCache[Long, IntermediateVectorTile](
    cacheSize=500, (tileID, tile) => if (tile.nonEmpty) tilesToReturn += ((tileID, tile))
  )

  /** A counter for the number of tiles returned */
  @transient var numTilesReturned: Int = 0

  def processMore: Unit = {
    // Process until there is a value to return or there are no more features to process
    while (partitionedFeatures.hasNext && tilesToReturn.isEmpty) {
      val (tileID: Long, feature: IFeature) = partitionedFeatures.next()
      val tile: IntermediateVectorTile = tilesInProgress.getOrElseUpdate(tileID, {
        // First time to encounter this tile, create the corresponding canvas
        val dataToImage = MVTDataVisualizer.dataToImageTransformer(tileID, resolution)
        new IntermediateVectorTile(resolution, buffer, new AffineTransform2D(dataToImage))
      })
      tile.addFeature(feature)
    }
    if (!partitionedFeatures.hasNext) {
      // Done processing! Move all canvases to the return value
      tilesInProgress.foreach(kv => {
        if (kv._2.nonEmpty)
          tilesToReturn += ((kv._1, kv._2))
      })
      tilesInProgress.clear()
    }
  }

  // Process first batch
  processMore

  override def hasNext: Boolean = {
    if (tilesToReturn.isEmpty)
      logDebug(s"Returned a total of $numTilesReturned tiles")
    tilesToReturn.nonEmpty
  }

  override def next(): (Long, IntermediateVectorTile) = {
    val returnValue: (Long, IntermediateVectorTile) = tilesToReturn.remove(0)
    if (partitionedFeatures.hasNext)
      processMore
    numTilesReturned += 1
    returnValue
  }
}
