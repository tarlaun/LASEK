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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable

/**
 * Creates tiles progressively on partitioned features. The input is an iterator of (TileID: Long, Feature)
 * and the output is an iterator of (TileID: Long, Canvas). The input is sorted on TileID which corresponds
 * to a partition.
 *
 * @param sortedFeatures the features that are already sorted by TileID
 * @param partitioner the partitioner that defines the tiles that can be generated
 */
class TileCreatorPyramidPartitioning(sortedFeatures: Iterator[(Long, IFeature)], partitioner: PyramidPartitioner,
                                     plotter: Plotter, tileWidth: Int, tileHeight: Int)
    extends Iterator[Canvas] {
  var iGeneratedTiles: Iterator[(Long, Canvas)] = _
  val inputMBR: EnvelopeNDLite = partitioner.getInputMBR
  var firstTupleOfNextBatch: (Long, IFeature) = if (sortedFeatures.hasNext) sortedFeatures.next() else null
  val granularity: Int = partitioner.granularity

  /**
   * Generate all tiles in the next batch and returns an iterator to them
   * @return an iterator to all tiles in the next batch
   */
  def generateNextBatch(): Iterator[(Long, Canvas)] = {
    var iTiles: Iterator[(Long, Canvas)] = null
    do {
      // Initialize the partitioner and the range of tiles to generate based on the root tile and granularity
      // rootTileID (and rootTileIndex) is the ID of the top tile that contains all features
      val rootTileID: Long = firstTupleOfNextBatch._1
      val rootTileIndex: TileIndex = TileIndex.decode(firstTupleOfNextBatch._1, null)
      // Number of levels to generate under the root tile
      val numLevelsToGenerate: Int = (partitioner.getMaxLevel - rootTileIndex.z) % granularity + 1
      // The pyramid that defines the region to generate under the root tile
      val pyramid: SubPyramid = new SubPyramid(inputMBR, rootTileIndex.z, rootTileIndex.z + numLevelsToGenerate - 1,
        rootTileIndex.x << (numLevelsToGenerate - 1), rootTileIndex.y << (numLevelsToGenerate - 1),
        (rootTileIndex.x + 1) << (numLevelsToGenerate - 1), (rootTileIndex.y + 1) << (numLevelsToGenerate - 1))
      val subPartitioner: PyramidPartitioner = new PyramidPartitioner(partitioner, pyramid)
      // Create some objects to reuse for all features
      val shapeMBR = new EnvelopeNDLite(2)
      val tileMBR = new Envelope()
      val partitionedTileIndex = new TileIndex
      val tempTiles = new mutable.HashMap[Long, CanvasModified]
      // Process all records in the current batch
      var recordCount = 0
      do {
        recordCount += 1
        val feature = firstTupleOfNextBatch._2
        shapeMBR.setEmpty()
        shapeMBR.merge(feature.getGeometry)
        // Find all overlapping tiles
        val overlappingTiles = subPartitioner.overlapPartitions(shapeMBR)
        for (i <- 0 until overlappingTiles.size) {
          val overlappingTileID: Long = overlappingTiles(i)

          val c: CanvasModified = tempTiles.getOrElseUpdate(overlappingTileID, {
            // First time to encounter this tile, create the corresponding canvas
            TileIndex.decode(overlappingTileID, partitionedTileIndex)
            TileIndex.getMBR(inputMBR, partitionedTileIndex.z, partitionedTileIndex.x, partitionedTileIndex.y, tileMBR)
            CanvasModified(plotter.createCanvas(tileWidth, tileHeight, tileMBR, overlappingTileID))
          })
          c.modified = plotter.plot(c.canvas, feature) || c.modified
        }
        // Fetch next tile and continue if it belongs to the same tile ID
        firstTupleOfNextBatch = if (sortedFeatures.hasNext) sortedFeatures.next() else null
      } while (firstTupleOfNextBatch != null && firstTupleOfNextBatch._1 == rootTileID)
      iTiles = tempTiles.filter(kv => kv._2.modified).map(kv => (kv._1, kv._2.canvas)).iterator
    } while (iTiles.isEmpty && firstTupleOfNextBatch != null)
    iTiles
  }

  /**Generate the first batch of tiles*/
  iGeneratedTiles = if (firstTupleOfNextBatch == null) null else generateNextBatch()

  override def hasNext: Boolean = iGeneratedTiles != null && iGeneratedTiles.hasNext

  override def next(): Canvas = {
    val returnTuple: Canvas = iGeneratedTiles.next()._2
    if (!iGeneratedTiles.hasNext && firstTupleOfNextBatch != null)
      iGeneratedTiles = generateNextBatch()
    returnTuple
  }
}
