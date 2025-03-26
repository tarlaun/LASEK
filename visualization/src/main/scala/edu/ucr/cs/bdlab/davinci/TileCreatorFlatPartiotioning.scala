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
import edu.ucr.cs.bdlab.beast.util.LRUCache
import org.apache.spark.internal.Logging
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable

/**
 * Creates tiles progressively on partitioned features. The input is an iterator of (TileID: Long, Feature)
 * and the output is an iterator of (TileID: Long, Canvas).
 *
 * @param partitionedFeatures the features that are already sorted by TileID
 */
class TileCreatorFlatPartiotioning(partitionedFeatures: Iterator[(Long, IFeature)],
                                   inputMBR: EnvelopeNDLite,
                                   plotter: Plotter, tileWidth: Int, tileHeight: Int)
    extends Iterator[(Long, Canvas)] with Logging {

  /**
   * Tiles that need to be returned
   */
  var tilesToReturn: mutable.Buffer[(Long, Canvas)] = new mutable.ListBuffer[(Long, Canvas)]()

  val tilesInProgress: LRUCache[Long, CanvasModified] = new LRUCache[Long, CanvasModified](500,
    (tileID, canvas) => if (canvas.modified) tilesToReturn += ((tileID, canvas.canvas))
  )

  /**Temporary variable to decode tile index*/
  @transient val tileIndex: TileIndex = new TileIndex()

  /**Temporary variable to decode tile MBR*/
  @transient val tileMBR: Envelope = new Envelope()

  /** A counter for the number of tiles returned */
  @transient var numTilesReturned: Int = 0

  def processMore: Unit = {
    // Process until there is a value to return or there are no more features to process
    while (partitionedFeatures.hasNext && tilesToReturn.isEmpty) {
      val (tileID: Long, feature: IFeature) = partitionedFeatures.next()
      val c: CanvasModified = tilesInProgress.getOrElseUpdate(tileID, {
        // First time to encounter this tile, create the corresponding canvas
        TileIndex.decode(tileID, tileIndex)
        TileIndex.getMBR(inputMBR, tileIndex.z, tileIndex.x, tileIndex.y, tileMBR)
        CanvasModified(plotter.createCanvas(tileWidth, tileHeight, tileMBR, tileID))
      })
      c.modified = plotter.plot(c.canvas, feature) || c.modified
    }
    if (!partitionedFeatures.hasNext) {
      // Done processing! Move all canvases to the return value
      tilesInProgress.foreach(kv => {
        if (kv._2.modified)
          tilesToReturn += ((kv._1, kv._2.canvas))
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

  override def next(): (Long, Canvas) = {
    val returnValue: (Long, Canvas) = tilesToReturn.remove(0)
    if (partitionedFeatures.hasNext)
      processMore
    numTilesReturned += 1
    returnValue
  }
}
