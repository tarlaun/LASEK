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
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.hadoop.fs.FileSystem

/**
 * An interface for reading raster files for Spark.
 * Raster readers do not need to be serialized since they are created and processed on the same node.
 */
trait IRasterReader[T] extends AutoCloseable {

  /**The path of the file that this reader reads*/
  private var path: String = _

  /**The name of the layer that this reader reader*/
  private var layer: String = _

  /**Additional options for reading the file*/
  protected var opts: BeastOptions = _

  /**
   * Initialize the raster reader to read a specific layer from the given file
   * @param fileSystem the file system that contains the file
   * @param path the path of the raster file
   * @param layer the name of the layer
   */
  def initialize(fileSystem: FileSystem, path: String, layer: String, opts: BeastOptions): Unit = {
    this.path = path
    this.layer = layer
    this.opts = opts
  }

  /**Access the metadata of this raster file*/
  def metadata: RasterMetadata

  /**
   * Reads a tile from the file. Accessors must be able to read multiple tiles concurrently by calling
   * this method several times. The tile does not necessarily need to be accessed if the raster reader is closed
   * but it is not against the interface to keep the returned tile independent so it stays valid even after
   * the raster reader is closed.
   * @param tileID the ID of the tile
   * @return the object that contains the tile information
   */
  def readTile(tileID: Int): ITile[T]

  /**
   * Returns the path of the file accessed by this reader.
   * @return
   */
  final def getFilePath: String = path

  /**
   * Return the name of the layer being accessed by this reader.
   * @return the name of the layer as being initialized
   */
  final def getLayerName: String = layer

  /**
   * Returns the offset of the given tile in the file
   * @param tileID the ID of the tile in the range [0, metadata.numTiles)
   * @return the offset of the tile in the first
   */
  def getTileOffset(tileID: Int): Long

  /**
   * Returns the offsets of all tiles in the file
   * @return an array of all tile offsets in the file
   */
  def getTileOffsets: Array[Long] = (0 until metadata.numTiles).map(getTileOffset).toArray

  /**
   * If a tile is known to be empty (invalid) in the input file, this function can be defined to return
   * false for those tiles which can save time in skipping over those tiles.
   * @param tileID the ID of the tile to check
   * @return `false` if the tile is known to be empty or `true` if the tile is known to be non-empty
   *         or if this information is not available.
   */
  def isValidTile(tileID: Int): Boolean = true
}

object IRasterReader {
  /**The ID or the name of the layer to reader from the raster file*/
  val RasterLayerID: String = "layer"

  /**Override the SRID of the input file to a specific one. Useful when the CRS is not set in the file.*/
  val OverrideSRID: String = "RasterFile.OverrideSRID"
}