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

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType}
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.AffineTransform
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Raster focal operations
 */
object RasterOperationsFocal extends Logging {
  /**
   * Converts the metadata of the input raster to the target metadata. This might involve any combination of
   * the following:
   *  - reproject: Change the coordinate reference system (CRS)
   *  - regrid: Change the tiling of the raster
   *  - rescale: Change the resolution, i.e., number of pixels, in the raster
   *  - subset: Retrieve a subset of the data, even though, it would be an inefficient way to do just the subsetting
   *
   * Note: This is a low-level method that is normally not called directly by users.
   * Instead, we provide higher-level easier to use functions for real use cases that all call this low-level function.
   *
   * Note: This method uses the average method to determine the final value of each pixel. If one pixel in the answer
   * overlaps multiple pixels in the source, their average is computed. This method should only be used when pixels
   * represent continuous values, e.g., red, infrared, temperature, or vegetation. If the pixels represent
   * categorical values, e.g., land type, then the nearest neighbor method [[reshapeNN]] should be used instead.
   * @param raster the input raster that should be reshaped
   * @param targetMetadataConv a function that returns the desired metadata for source metadata
   * @param _numPartitions the number of partitions of the produces RDD. If not set, it will be the same as the input
   * @return the new raster with the target metadata
   */
  def reshapeAverage[T: ClassTag](raster: RasterRDD[T], targetMetadataConv: RasterMetadata => RasterMetadata,
                                  _numPartitions: Int = 0): RasterRDD[T] = {
    val intermediateTiles: RDD[(Int, MemoryTile[Array[Float]])] = raster.flatMap(sourceTile => {
      val targetMetadata: RasterMetadata = targetMetadataConv(sourceTile.rasterMetadata)
      val numIntermediateValues = sourceTile.numComponents + 1
      val accumulator: (Array[Float], T) => Unit = accumulators(sourceTile.pixelType).asInstanceOf[(Array[Float], T) => Unit]

      // We need a transformer that transforms a pixel in source to the target pixel in target and vice-versa
      // These are the steps needed to transform from source to target:
      // 1- Use source metadata grid-to-model to convert from source raster space to source model space
      // 2- Use CRS transformation to transform from source model space to target model space
      // 3- Use target metadata model-to-grid to convert from target model space to target grid space
      // To do all these steps efficiently, we concatenate all these transformations into one
      val sourceG2M = new AffineTransform2D(sourceTile.rasterMetadata.g2m)
      val transformationInfo = Reprojector.findTransformationInfo(sourceTile.rasterMetadata.srid, targetMetadata.srid)
      val targetM2G = new AffineTransform2D(targetMetadata.g2m).inverse()
      val sourceToTarget = concatenateTransforms(sourceG2M, transformationInfo.mathTransform, targetM2G)
      val targetToSource = sourceToTarget.inverse()

      // First, convert the bounding box of the entire source tile to know the range of target tiles
      val tileCorners = Array[Double](sourceTile.x1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y2 + 1,
        sourceTile.x1, sourceTile.y2 + 1,
      )
      sourceToTarget.transform(tileCorners, 0, tileCorners, 0, 4)
      // Convert to a range of pixels in the target. This range is *exclusive*
      // The target range contains all pixels that partially overlap this pixel
      val targetRasterX1: Int = ((tileCorners(0) min tileCorners(2) min tileCorners(4) min tileCorners(6)) max targetMetadata.x1).toInt
      val targetRasterX2: Int = (((tileCorners(0) max tileCorners(2) max tileCorners(4) max tileCorners(6))+1.0) min targetMetadata.x2).toInt
      val targetRasterY1: Int = ((tileCorners(1) min tileCorners(3) min tileCorners(5) min tileCorners(7)) max targetMetadata.y1).toInt
      val targetRasterY2: Int = (((tileCorners(1) max tileCorners(3) max tileCorners(5) max tileCorners(7))+1.0) min targetMetadata.y2).toInt


      // The most general (and heavy) method for creating intermediate tiles
      val createIntermediateTilesGeneral: Int => (Int, MemoryTile[Array[Float]]) = targetTileID => {
        var emptyTile = true
        val targetTile = new MemoryTile[Array[Float]](targetTileID, targetMetadata)
        // (x1,y1)-(x2,y2) is an inclusive range of target pixels that match with this tile
        val targetTileX1 = targetTile.x1 max targetRasterX1
        val targetTileX2 = targetTile.x2 min targetRasterX2
        val targetTileY1 = targetTile.y1 max targetRasterY1
        val targetTileY2 = targetTile.y2 min targetRasterY2
        for (targetY <- targetTileY1 to targetTileY2; targetX <- targetTileX1 to targetTileX2) {
          // Convert this pixel to a box in the source dataset
          val pixelCorners = Array[Double](targetX, targetY, targetX + 1, targetY + 1)
          targetToSource.transform(pixelCorners, 0, pixelCorners, 0, 2)
          // Now, the range is still inclusive of the first pixel and exclusive of the last pixel
          // If the first and last pixels are the same, we still read the first pixel
          // Now, we can now loop over all source pixels and aggregate them.
          val sourceX1: Int = pixelCorners(0).toInt max sourceTile.x1
          // The -0.5 in the following equations is to ensure that we include up-to the last pixel that has
          // its center in the converted pixel coordinates.
          val sourceX2: Int = (pixelCorners(2) - 0.5).toInt min sourceTile.x2
          val sourceY1: Int = pixelCorners(1).toInt max sourceTile.y1
          val sourceY2: Int = (pixelCorners(3) - 0.5).toInt min sourceTile.y2
          var sourceX = sourceX1
          var sourceY = sourceY1
          val accumulate = new Array[Float](numIntermediateValues)
          while (sourceX <= sourceX2 && sourceY <= sourceY2) {
            if (sourceTile.isDefined(sourceX, sourceY))
              accumulator(accumulate, sourceTile.getPixelValue(sourceX, sourceY))
            sourceX += 1
            if (sourceX > sourceX2) {
              sourceX = sourceX1
              sourceY += 1
            }
          }
          if (accumulate.last > 0) {
            // Found at least one pixel
            targetTile.setPixelValue(targetX, targetY, accumulate)
            emptyTile = false
          }
        }
        if (emptyTile)
          (targetTileID, null)
        else
          (targetTileID, targetTile)
      }

      // Create intermediate tiles when the transformation is simple and a source pixel is bigger than a target one
      // In this case, each target pixel has at most one value
      val createIntermediateTilesSimpleSourcePixelBigger: Int => (Int, MemoryTile[Array[Float]]) = targetTileID => {
        var emptyTile = true
        val targetTile = new MemoryTile[Array[Float]](targetTileID, targetMetadata)
        // An inclusive range of all target pixels that will be calculated for this tile
        val allTargetPixelRange = Array[Double](
          targetTile.x1 max targetRasterX1, targetTile.y1 max targetRasterY1,
          targetTile.x2 min targetRasterX2, targetTile.y2 min targetRasterY2
        )
        var targetPixelX1: Int = allTargetPixelRange(0).toInt
        var targetPixelY1: Int = allTargetPixelRange(1).toInt
        // The source pixel being read
        val sourcePixelRange = new Array[Double](4)
        targetToSource.transform(allTargetPixelRange, 0, sourcePixelRange, 0, 2)
        sourcePixelRange(2) = sourcePixelRange(2) min sourceTile.x2
        sourcePixelRange(3) = sourcePixelRange(3) min sourceTile.y2
        var sourceX = sourcePixelRange(0).toInt
        var sourceY = sourcePixelRange(1).toInt
        while (sourceX <= sourcePixelRange(2) && sourceY <= sourcePixelRange(3)) {
          // Find the end of the target tiles that are covered by this pixel
          val targetPixelEndRange = Array[Double](sourceX + 1, sourceY + 1)
          sourceToTarget.transform(targetPixelEndRange, 0, targetPixelEndRange, 0, 1)
          if (sourceTile.isDefined(sourceX, sourceY)) {
            emptyTile = false
            // Source pixel is defined. Read it and copy it to all matching target pixels
            val accumulate = new Array[Float](numIntermediateValues)
            accumulator(accumulate, sourceTile.getPixelValue(sourceX, sourceY))
            // The -0.5 is to include up-to the last pixel with a center in the transformed range
            for (targetY <- targetPixelY1 to (targetPixelEndRange(1) - 0.5).toInt;
                 targetX <- targetPixelX1 to (targetPixelEndRange(0) - 0.5).toInt) {
              targetTile.setPixelValue(targetX, targetY, accumulate)
            }
          }
          // Move to the next pixel
          sourceX += 1
          // Update the target pixel. Note: This is only possible for non-shear affine transforms
          targetPixelX1 = (targetPixelEndRange(0) + 0.5).toInt
          if (sourceX > sourcePixelRange(2)) {
            // Reached the end of this line, move to the next line
            sourceX = sourcePixelRange(0).toInt
            sourceY += 1
            // Note: The following two lines are only possible for non-shear affine transforms
            targetPixelX1 = allTargetPixelRange(0).toInt
            targetPixelY1 = (targetPixelEndRange(1) + 0.5).toInt
          }
        }
        if (emptyTile)
          (targetTileID, null)
        else
          (targetTileID, targetTile)
      }

      // Create intermediate tiles when the transformation is simple and a target pixel is bigger than a source one
      // In this case, one source tile can contain many source pixels an they should be accumulated
      val createIntermediateTilesSimpleTargetPixelBigger: Int => (Int, MemoryTile[Array[Float]]) = targetTileID => {
        var emptyTile = true
        val targetTile = new MemoryTile[Array[Float]](targetTileID, targetMetadata)
        // An inclusive range of all target pixels that will be calculated for this tile
        val allTargetPixelRange = Array[Double](
          targetTile.x1 max targetRasterX1, targetTile.y1 max targetRasterY1,
          targetTile.x2 min targetRasterX2, targetTile.y2 min targetRasterY2
        )
        // The first source pixel that matches the current target pixel
        val sourcePixelRange = new Array[Double](4)
        targetToSource.transform(allTargetPixelRange, 0, sourcePixelRange, 0, 1)
        sourcePixelRange(0) = sourcePixelRange(0).toInt max sourceTile.x1
        sourcePixelRange(1) = sourcePixelRange(1).toInt max sourceTile.y1
        var targetX: Int = allTargetPixelRange(0).toInt
        var targetY: Int = allTargetPixelRange(1).toInt
        while (targetX <= allTargetPixelRange(2) && targetY <= allTargetPixelRange(3)) {
          // Find the end of the source pixels that cover this target pixel
          sourcePixelRange(2) = targetX + 1
          sourcePixelRange(3) = targetY + 1
          targetToSource.transform(sourcePixelRange, 2, sourcePixelRange, 2, 1)
          // The -0.5 in the following condition is to ensure that we include up-to the last source pixel
          // with a center in the transformed range
          sourcePixelRange(2) = (sourcePixelRange(2) - 0.5).toInt min sourceTile.x2
          sourcePixelRange(3) = (sourcePixelRange(3) - 0.5).toInt min sourceTile.y2
          // Loop over all source pixels until finding the first non-empty pixel
          var sourceX: Int = sourcePixelRange(0).toInt
          var sourceY: Int = sourcePixelRange(1).toInt
          val accumulate = new Array[Float](numIntermediateValues)
          while (sourceX <= sourcePixelRange(2) && sourceY <= sourcePixelRange(3)) {
            if (sourceTile.isDefined(sourceX, sourceY))
              accumulator(accumulate, sourceTile.getPixelValue(sourceX, sourceY))
            sourceX += 1
            if (sourceX > sourcePixelRange(2)) {
              sourceX = sourcePixelRange(0).toInt
              sourceY += 1
            }
          }
          if (accumulate.last > 0) {
            emptyTile = false
            targetTile.setPixelValue(targetX, targetY, accumulate)
          }
          // Advance to the next pixel
          targetX += 1
          // Move the box that covers source pixels.
          // Notice that this statement is only valid for non-shear affine transformations.
          sourcePixelRange(0) = sourcePixelRange(2).toInt + 1
          if (targetX > allTargetPixelRange(2)) {
            // Move to the next line
            targetX = allTargetPixelRange(0).toInt
            targetY += 1
            // Move the box that covers source pixels.
            // Notice that this statement is only valid for non-shear affine transformations.
            targetToSource.transform(allTargetPixelRange, 0, sourcePixelRange, 0, 1)
            sourcePixelRange(0) = sourcePixelRange(0).toInt max sourceTile.x1
            sourcePixelRange(1) = sourcePixelRange(3).toInt + 1
          }
        }
        if (emptyTile)
          (targetTileID, null)
        else
          (targetTileID, targetTile)
      }

      // The simplest version where there is a one-to-one mapping between source and target pixels
      val createIntermediateTilesOneToOne: Int => (Int, MemoryTile[Array[Float]]) = targetTileID => {
        var emptyTile = true
        val targetTile = new MemoryTile[Array[Float]](targetTileID, targetMetadata)
        // An inclusive range of all target pixels that will be calculated for this tile
        val targetX1: Int = targetTile.x1 max targetRasterX1
        val targetY1: Int = targetTile.y1 max targetRasterY1
        val targetX2: Int = targetTile.x2 min targetRasterX2
        val targetY2: Int = targetTile.y2 min targetRasterY2
        // The first source pixel that matches the current target pixel
        val sourcePixelRange = new Array[Double](4)
        targetToSource.transform(Array[Double](targetX1, targetY1, targetX2, targetY2), 0, sourcePixelRange, 0, 2)
        val sourceX1: Int = sourcePixelRange(0).toInt max sourceTile.x1
        val sourceY1: Int = sourcePixelRange(1).toInt max sourceTile.y1
        val sourceX2: Int = sourcePixelRange(2).toInt min sourceTile.x2
        val sourceY2: Int = sourcePixelRange(3).toInt min sourceTile.y2
        var sourceX: Int = sourceX1
        var sourceY: Int = sourceY1
        var targetX: Int = targetX1
        var targetY: Int = targetY1
        while (sourceX <= sourceX2 && sourceY <= sourceY2) {
          if (sourceTile.isDefined(sourceX, sourceY)) {
            val accumulate = new Array[Float](numIntermediateValues)
            accumulator(accumulate, sourceTile.getPixelValue(sourceX, sourceY))
            targetTile.setPixelValue(targetX, targetY, accumulate)
            emptyTile = false
          }
          sourceX += 1
          targetX += 1
          if (sourceX > sourceX2) {
            sourceX = sourceX1
            targetX = targetX1
            sourceY += 1
            targetY += 1
          }
        }

        if (emptyTile)
          (targetTileID, null)
        else
          (targetTileID, targetTile)
      }

      if (targetRasterX1 >= targetRasterX2 || targetRasterY1 >= targetRasterY2) {
        // Empty result. Return empty array
        Seq[(Int, MemoryTile[Array[Float]])]().iterator
      } else {
        // Compute the range of tiles that contain all target tiles. This range is *inclusive*
        val tileCol1 = (targetRasterX1 - targetMetadata.x1) / targetMetadata.tileWidth
        val tileCol2 = (targetRasterX2 - 1 - targetMetadata.x1) / targetMetadata.tileWidth
        val tileRow1 = (targetRasterY1 - targetMetadata.y1) / targetMetadata.tileHeight
        val tileRow2 = (targetRasterY2 - 1 - targetMetadata.y1) / targetMetadata.tileHeight
        // Create an iterator over all target tile IDs
        val targetTileIDs: Iterator[Int] = for (tileRow <- (tileRow1 to tileRow2).iterator;
                                 tileCol <- (tileCol1 to tileCol2).iterator)
          yield tileRow * targetMetadata.numTilesX + tileCol

        // Choose the most efficient creation method based on the transformation
        val simpleTransform = sourceToTarget.isInstanceOf[AffineTransform] &&
          sourceToTarget.asInstanceOf[AffineTransform].getShearX == 0 &&
          sourceToTarget.asInstanceOf[AffineTransform].getShearY == 0
        val creationFunction = if (simpleTransform &&
          sourceToTarget.asInstanceOf[AffineTransform].getScaleX.abs == 1.0 &&
          sourceToTarget.asInstanceOf[AffineTransform].getScaleY.abs == 1.0)
          createIntermediateTilesOneToOne
        else if (simpleTransform && sourceToTarget.asInstanceOf[AffineTransform].getScaleX >= 1)
        // An affine transform with no shearing where a source pixel is bigger than a target pixel
          createIntermediateTilesSimpleSourcePixelBigger
        else if (simpleTransform && sourceToTarget.asInstanceOf[AffineTransform].getScaleX <= 1)
        // An affine transform with no shearing where a target pixel is bigger than a source pixel
          createIntermediateTilesSimpleTargetPixelBigger
        else
        // None of the above, choose the least efficient but most general method
          createIntermediateTilesGeneral
        val interTiles: Iterator[(Int, MemoryTile[Array[Float]])] = targetTileIDs.map(creationFunction)
        interTiles.filter(_._2 != null)
      }
    })
    val numTargetPartitions = if (_numPartitions == 0) raster.getNumPartitions else _numPartitions
    val partitioner = new HashPartitioner(numTargetPartitions)
    // Merge non-empty pixels from intermediate tiles
    intermediateTiles.reduceByKey(partitioner, (t1, t2) => {
      for (y <- t2.y1 to t2.y2; x <- t2.x1 to t2.x2) {
        if (t2.isDefined(x, y)) {
          if (!t1.isDefined(x, y))
            t1.setPixelValue(x, y, t2.getPixelValue(x, y))
          else {
            // The pixel is defined in both, sum the intermediate values
            val v1: Array[Float] = t1.getPixelValue(x, y)
            val v2: Array[Float] = t2.getPixelValue(x, y)
            for (i <- v2.indices)
              v1(i) += v2(i)
            t1.setPixelValue(x, y, v1)
          }
        }
      }
      t1
    }).map(t => {
      // Compute the average of each pixel
      val intermediateTile = t._2
      val finalTile = new MemoryTile[T](t._1, intermediateTile.rasterMetadata)
      val computeAverage: Array[Float] => T = averageComputer(finalTile.pixelType).asInstanceOf[Array[Float] => T]
      for ((x, y) <- finalTile.pixelLocations) {
        if (intermediateTile.isDefined(x, y))
          finalTile.setPixelValue(x, y, computeAverage(intermediateTile.getPixelValue(x, y)))
      }
      finalTile.asInstanceOf[ITile[T]]
    })
  }

  /**
   * Converts the metadata of the input raster to the target metadata. This might involve any combination of
   * the following:
   *  - reproject: Change the coordinate reference system (CRS)
   *  - regrid: Change the tiling of the raster
   *  - rescale: Change the resolution, i.e., number of pixels, in the raster
   *  - subset: Retrieve a subset of the data, even though, it would be an inefficient way to do just the subsetting
   *
   * Note: This is a low-level method that is normally not called directly by users.
   * Instead, we provide higher-level easier to use functions for real use cases that all call this low-level function.
   *
   * Note: This method uses the nearest neighbor method to match source to target pixels. Each target pixel gets its
   * value from the nearest source pixel. If that source pixel is empty or outside the range of the source raster,
   * the target pixel will be empty.
   * @param raster the input raster that should be reshaped
   * @param targetMetadataConv a function that converts a source RasterMetadata to target RasterMetadata
   * @param numPartitions the number of partitions in the output RDD. If not set, the input numPartitions is used
   * @return the new raster with the target metadata
   */
  def reshapeNN[T: ClassTag](raster: RasterRDD[T], targetMetadataConv: RasterMetadata => RasterMetadata,
                             numPartitions: Int = 0): RasterRDD[T] = {
    // A localized cache in each executor to avoid creating the source <-> target transforms for each tile
    val cache = new mutable.HashMap[Int, (MathTransform, MathTransform)]()

    val intermediateTiles: RDD[(Int, MemoryTile[T])] = raster.flatMap(sourceTile => {
      val targetMetadata: RasterMetadata = targetMetadataConv(sourceTile.rasterMetadata)
      // We need a transformer that transforms a pixel in source to the target pixel in target and vice-versa
      // These are the steps needed to transform from source to target:
      // 1- Use source metadata grid-to-model to convert from source raster space to source model space
      // 2- Use CRS transformation to transform from source model space to target model space
      // 3- Use target metadata model-to-grid to convert from target model space to target grid space
      // To do all these steps efficiently, we concatenate all these transformations into one
      val (sourceToTarget, targetToSource) = cache.getOrElseUpdate(sourceTile.rasterMetadata.srid, {
        val sourceG2M = new AffineTransform2D(sourceTile.rasterMetadata.g2m)
        val transformationInfo: Reprojector.TransformationInfo =
          Reprojector.findTransformationInfo(sourceTile.rasterMetadata.srid, targetMetadata.srid)
        val targetM2G = new AffineTransform2D(targetMetadata.g2m).inverse()
        val sourceToTarget: MathTransform = concatenateTransforms(sourceG2M, transformationInfo.sourceSnap,
          transformationInfo.mathTransform, targetM2G)
        val targetToSource: MathTransform = sourceToTarget.inverse()
        (sourceToTarget, targetToSource)
      })

      // First, convert the bounding box of the entire source tile to know the range of target tiles
      val tileCorners = Array[Double](sourceTile.x1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y2 + 1,
        sourceTile.x1, sourceTile.y2 + 1,
      )
      sourceToTarget.transform(tileCorners, 0, tileCorners, 0, 4)
      // Convert to a range of pixels in the target. This range is *exclusive*
      // The target range contains all pixels whose centers are in the converted range
      val targetRasterX1: Int = (((tileCorners(0) min tileCorners(2) min tileCorners(4) min tileCorners(6))+0.5) max targetMetadata.x1).toInt
      val targetRasterX2: Int = (((tileCorners(0) max tileCorners(2) max tileCorners(4) max tileCorners(6))+0.5) min targetMetadata.x2).toInt
      val targetRasterY1: Int = (((tileCorners(1) min tileCorners(3) min tileCorners(5) min tileCorners(7))+0.5) max targetMetadata.y1).toInt
      val targetRasterY2: Int = (((tileCorners(1) max tileCorners(3) max tileCorners(5) max tileCorners(7))+0.5) min targetMetadata.y2).toInt

      val createIntermediateTilesNN: Int => (Int, MemoryTile[T]) = targetTileID => {
        var emptyTile = true
        val targetTile = new MemoryTile[T](targetTileID, targetMetadata)
        // (x1,y1)-(x2,y2) is an inclusive range of target pixels that match with this tile
        val targetTileX1 = targetTile.x1 max targetRasterX1
        val targetTileX2 = targetTile.x2 min targetRasterX2
        val targetTileY1 = targetTile.y1 max targetRasterY1
        val targetTileY2 = targetTile.y2 min targetRasterY2
        for (targetY <- targetTileY1 to targetTileY2; targetX <- targetTileX1 to targetTileX2) {
          // Project the center of the target pixel to the source
          val sourcePixel = Array[Double](targetX + 0.5, targetY + 0.5)
          targetToSource.transform(sourcePixel, 0, sourcePixel, 0, 1)
          val sourceX: Int = sourcePixel(0).toInt
          val sourceY: Int = sourcePixel(1).toInt
          if (sourceX >= sourceTile.x1 && sourceX <= sourceTile.x2 &&
            sourceY >= sourceTile.y1 && sourceY <= sourceTile.y2 && sourceTile.isDefined(sourceX, sourceY)) {
            emptyTile = false
            targetTile.setPixelValue(targetX, targetY, sourceTile.getPixelValue(sourceX, sourceY))
          }
        }
        if (emptyTile) (targetTileID, null) else (targetTileID, targetTile)
      }
      if (targetRasterX1 >= targetRasterX2 || targetRasterY1 >= targetRasterY2) {
        // Empty result. Return empty array
        Seq[(Int, MemoryTile[T])]().iterator
      } else {
        // Compute the range of tiles that contain all target tiles. This range is *inclusive*
        val tileCol1 = (targetRasterX1 - targetMetadata.x1) / targetMetadata.tileWidth
        val tileCol2 = (targetRasterX2 - 1 - targetMetadata.x1) / targetMetadata.tileWidth
        val tileRow1 = (targetRasterY1 - targetMetadata.y1) / targetMetadata.tileHeight
        val tileRow2 = (targetRasterY2 - 1 - targetMetadata.y1) / targetMetadata.tileHeight
        // Create an iterator over all target tile IDs
        val targetTileIDs: Iterator[Int] = for (tileRow <- (tileRow1 to tileRow2).iterator;
                                                tileCol <- (tileCol1 to tileCol2).iterator)
        yield tileRow * targetMetadata.numTilesX + tileCol

        val interTiles: Iterator[(Int, MemoryTile[T])] = targetTileIDs.map(createIntermediateTilesNN)
        interTiles.filter(_._2 != null)
      }
    })

    val numTargetPartitions: Int = if (numPartitions == 0) raster.getNumPartitions else numPartitions
    val partitioner = new HashPartitioner(numTargetPartitions)
    intermediateTiles.reduceByKey(partitioner, (t1, t2) => {
      for ((x, y) <- t2.pixelLocations) {
        if (t2.isDefined(x, y))
          t1.setPixelValue(x, y, t2.getPixelValue(x, y))
      }
      t1
    }).map(_._2.asInstanceOf[ITile[T]])
  }

  object InterpolationMethod extends Enumeration {
    type InterpolationMethod = Value

    val NearestNeighbor: InterpolationMethod = Value("NearestNeighbor")
    val Average: InterpolationMethod = Value("Average")
  }

  /**
   * Reproject a raster to a target coordinate reference system.
   * This method uses the same resolution (number of pixels) of the first tile in the source raster.
   * You can use the other [[reshapeAverage()]] method that takes [[RasterMetadata]] to change all the information.
   *
   * @param raster the raster layer to reproject
   * @param targetCRS the target coordinate reference system
   * @param unifiedRaster if set to true, all output tiles will belong to a single RasterMetadata
   * @param interpolationMethod how to handle a target pixel that overlaps multiple source pixels
   * @tparam T the type of the pixels
   * @return
   */
  def reproject[T: ClassTag](raster: RasterRDD[T], targetCRS: CoordinateReferenceSystem, unifiedRaster: Boolean = false,
                             interpolationMethod: InterpolationMethod.InterpolationMethod =
                             InterpolationMethod.NearestNeighbor): RasterRDD[T] = {
    val targetMetadata: RasterMetadata => RasterMetadata = if (!unifiedRaster) {
      _.reproject(targetCRS)
    } else {
      val unifiedMetadata = RasterMetadata.reproject(RasterMetadata.allMetadata(raster), targetCRS)
      logInfo(s"Reprojecting with unified metadata ${unifiedMetadata}")
      _ => unifiedMetadata
    }
    interpolationMethod match {
      case InterpolationMethod.Average => reshapeAverage(raster, targetMetadata)
      case InterpolationMethod.NearestNeighbor => reshapeNN(raster, targetMetadata)
    }
  }

  /**
   * Changes the resolution of the raster to the desired resolution without changing tile size or CRS.
   * @param raster the raster to rescale
   * @param rasterWidth the new raster width in terms of pixels
   * @param rasterHeight the new height of the raster layer in terms of pixels
   * @param unifiedRaster if set to true, all output tiles will belong to a single RasterMetadata
   * @param interpolationMethod how to handle a target pixel that overlaps multiple source pixels
   * @return a new raster RDD with the desired width and height
   */
  def rescale[T: ClassTag](raster: RasterRDD[T], rasterWidth: Int, rasterHeight: Int, unifiedRaster: Boolean = false,
                           interpolationMethod: InterpolationMethod.InterpolationMethod =
                           InterpolationMethod.NearestNeighbor): RasterRDD[T] = {
    val targetMetadata: RasterMetadata => RasterMetadata = if (!unifiedRaster) {
      _.rescale(rasterWidth, rasterHeight)
    } else {
      val unifiedMetadata = RasterMetadata.rescale(RasterMetadata.allMetadata(raster), rasterWidth, rasterHeight)
      logInfo(s"Rescaling with unified metadata ${unifiedMetadata}")
      _ =>  unifiedMetadata
    }
    interpolationMethod match {
      case InterpolationMethod.Average => reshapeAverage(raster, targetMetadata)
      case InterpolationMethod.NearestNeighbor => reshapeNN(raster, targetMetadata)
    }
  }

  /**
   * Regrids the given raster to the target tile width and height
   * @param raster the raster to regrid
   * @param tileWidth the new tile width in pixels
   * @param tileHeight the new tile height in pixels
   * @tparam T the type of the pixel values in the raster
   * @return a new raster with the given tile width and height
   */
  def retile[T: ClassTag](raster: RasterRDD[T], tileWidth: Int, tileHeight: Int): RasterRDD[T] = {
    // Notice that for retile, there is a one-to-one correspondence in pixels so we use the more efficient NN method
    reshapeNN(raster, _.retile(tileWidth, tileHeight))
  }

  /**
   * Get a list of valid tiles that are within `w` pixels of the given tile.
   * @param tileID the ID of the tile at the center
   * @param metadata the raster metadata that describes the raster layer
   * @param w the size of the window in pixels
   * @return all valid tile IDs that are within `w` pixels of the given tile.
   */
  def getTilesInWindow(tileID: Int, metadata: RasterMetadata, w: Int): Array[Int] = {
    val minX: Int = metadata.getTileX1(tileID) - w
    val maxX: Int = metadata.getTileX2(tileID) + w
    val minY: Int = metadata.getTileY1(tileID) - w
    val maxY: Int = metadata.getTileY2(tileID) + w
    var x = minX
    var y = minY
    var results = Array[Int]()
    while (y <= maxY && x <= maxX) {
      if (x >= metadata.x1 && x < metadata.x2 && y >= metadata.y1 && y < metadata.y2) {
        val tid = metadata.getTileIDAtPixel(x, y)
        results = results :+ tid
      }
      x = (x + metadata.tileWidth) / metadata.tileWidth * metadata.tileWidth
      if (x > maxX) {
        x = minX
        y = (y + metadata.tileHeight) / metadata.tileHeight * metadata.tileHeight
      }
    }
    results
  }

  def slidingWindow2[T: ClassTag, U: ClassTag](raster: RasterRDD[T], w: Int, winFunc: (Array[T], Array[Boolean]) => U):
  RasterRDD[U] = {
    assert(RasterMetadata.allMetadata(raster).length == 1,
      "Raster input to window calculation function cannot have mixed metadata")
    val intermediateTiles: RDD[(Int, SlidingWindowTile[T, U])] = raster.flatMap(sourceTile => {
      val affectedTiles: Array[Int] = getTilesInWindow(sourceTile.tileID, sourceTile.rasterMetadata, w)
      affectedTiles.iterator.map(tid => {
        val wt = new SlidingWindowTile(tid, sourceTile.rasterMetadata, w, winFunc)
        wt.addTile(sourceTile)
        (wt.tileID, wt)
      })
    })
    val partitioner: Partitioner = if (raster.partitioner.isEmpty || raster.getNumPartitions < raster.sparkContext.defaultParallelism)
      new HashPartitioner(raster.sparkContext.defaultParallelism) else raster.partitioner.get
    intermediateTiles.reduceByKey(partitioner, (wt1, wt2) => wt1.merge(wt2))
      .map(x => x._2.resultTile)
  }

  def convolution[T: ClassTag, U: ClassTag](raster: RasterRDD[T], w: Int, weights: Array[Float]): RasterRDD[U] = {
    assert(RasterMetadata.allMetadata(raster).length == 1,
      "Raster input to convolution function cannot have mixed metadata")
    require(weights.length == (2*w + 1) * (2*w+1),
      s"Expected ${(2*w+1)*(2*w+1)} weights but received ${weights.length}")
    val intermediateTiles: RDD[(Int, AbstractConvolutionTile[U])] = raster.flatMap(sourceTile => {
      val affectedTiles: Array[Int] = getTilesInWindow(sourceTile.tileID, sourceTile.rasterMetadata, w)
      affectedTiles.iterator.map(tid => {
        val convTile = if (sourceTile.numComponents == 1)
          new ConvolutionTileSingleBand(tid, sourceTile.rasterMetadata, w, weights, sourceTile.tileID)
        else
          new ConvolutionTileMultiBand(tid, sourceTile.rasterMetadata, sourceTile.numComponents, w, weights, sourceTile.tileID)
        convTile.addTile(sourceTile)
        (tid, convTile.asInstanceOf[AbstractConvolutionTile[U]])
      })
    })
    val partitioner: Partitioner = if (raster.partitioner.isEmpty || raster.getNumPartitions < raster.sparkContext.defaultParallelism)
      new HashPartitioner(raster.sparkContext.defaultParallelism)
    else
      raster.partitioner.get
    intermediateTiles.reduceByKey(partitioner, (ct1, ct2) => ct1.merge(ct2))
      .map(_._2.asInstanceOf[ITile[U]])
  }

  /**
   * Performs a sliding window calculation for a window of size (2w + 1) &times; (2w + 1) given an integer value w.
   * The user-defined window calculation function takes all values in the window ordered in row-major order.
   * Additionally, a Boolean array of the same size is passed to indicate which values are defined and which are not.
   * The Boolean array is useful for two scenarios.
   *  1. When the window is near the edge of the raster, there must be some undefined values outside the raster.
   *  2. Some pixel values in raster might be undefined, e.g., due to cloud coverage.
   *
   * *Note*: This function will only work correctly if all input tiles have the same raster metadata.
   * @param raster the input raster to process
   * @param w the radius of the square window. The window size will be (2w + 1) &times; (2w + 1)
   * @param f the function to perform the calculation.
   * @tparam T the type of values in the input raster
   * @tparam U the type of output values (the result of the user-defined function).
   * @return a new raster with the same dimensions as the input after applying the window function.
   */
  def slidingWindow[T: ClassTag, U: ClassTag](raster: RasterRDD[T], w: Int, f: (Array[T], Array[Boolean]) => U):
    RasterRDD[U] = {
    import RaptorMixin._
    assert(raster.allMetadata.length == 1, "Raster input to window calculation function cannot have mixed metadata")
    val numValues = (2 * w + 1) * (2 * w + 1)
    val intermediateTiles: RDD[(Int, MemoryTileWindow[T])] = raster.flatMap(sourceTile => {
      val metadata = sourceTile.rasterMetadata
      // Compute the window of pixels that will be affected by current tile *inclusive*
      val windowX1 = (sourceTile.x1 - w) max metadata.x1
      val windowY1 = (sourceTile.y1 - w) max metadata.y1
      val windowX2 = (sourceTile.x2 + w) min (metadata.x2 - 1)
      val windowY2 = (sourceTile.y2 + w) min (metadata.y2 - 1)
      // Compute the *inclusive* range of tiles that will be affected by current tile
      val tileCol1 = (windowX1 - metadata.x1) / metadata.tileWidth
      val tileCol2 = (windowX2 - metadata.x1) / metadata.tileWidth
      val tileRow1 = (windowY1 - metadata.y1) / metadata.tileHeight
      val tileRow2 = (windowY2 - metadata.y1) / metadata.tileHeight
      // Create an array of all target tile IDs
      val targetTileIDs = new Array[Int]((tileCol2 - tileCol1 + 1) * (tileRow2 - tileRow1 + 1))
      var i = 0
      for (tileRow <- tileRow1 to tileRow2; tileCol <- tileCol1 to tileCol2) {
        targetTileIDs(i) = tileRow * metadata.numTilesX + tileCol
        i += 1
      }
      // Now, compute the target tiles and return them
      targetTileIDs.iterator.map(targetTileID => {
        val targetTile = MemoryTileWindow.create[T](targetTileID, metadata, numValues)
        // Calculate the source *inclusive* range of pixels that might affect this tile
        val sourceX1 = (metadata.getTileX1(targetTileID) - w) max sourceTile.x1
        val sourceY1 = (metadata.getTileY1(targetTileID) - w) max sourceTile.y1
        val sourceX2 = (metadata.getTileX2(targetTileID) + w) min sourceTile.x2
        val sourceY2 = (metadata.getTileY2(targetTileID) + w) min sourceTile.y2
        for (sourceY <- sourceY1 to sourceY2; sourceX <- sourceX1 to sourceX2) {
          if (sourceTile.isDefined(sourceX, sourceY)) {
            val value = sourceTile.getPixelValue(sourceX, sourceY)
            val targetX1 = (sourceX - w) max metadata.getTileX1(targetTileID)
            val targetY1 = (sourceY - w) max metadata.getTileY1(targetTileID)
            val targetX2 = (sourceX + w) min metadata.getTileX2(targetTileID)
            val targetY2 = (sourceY + w) min metadata.getTileY2(targetTileID)
            for (targetY <- targetY1 to targetY2; targetX <- targetX1 to targetX2) {
              val dx = targetX - sourceX
              val dy = targetY - sourceY
              val position = (2 * w + 1) * (dy + w) + dx + w
              targetTile.setValue(targetX, targetY, position, value)
            }
          }
        }
        (targetTileID, targetTile)
      })
    })

    // Merge all values from intermediate tiles
    val finalWindowTiles: RDD[(Int, MemoryTileWindow[T])] = intermediateTiles.reduceByKey((t1, t2) => {
      t1.mergeWith(t2)
      t1
    })
    // Apply the user-defined function to calculate the final tile
    finalWindowTiles.map(windowTile => {
      val finalTile = new MemoryTile[U](windowTile._1, windowTile._2.metadata)
      val values: Array[T] = new Array[T](numValues)
      val defined: Array[Boolean] = new Array[Boolean](numValues)
      for (y <- finalTile.y1 to finalTile.y2; x <- finalTile.x1 to finalTile.x2) {
        val count = windowTile._2.readValues(x, y, values, defined)
        if (count > 0)
          finalTile.setPixelValue(x, y, f(values, defined))
      }
      finalTile
    })
  }

  // A helper function that concatenates a set of MathTransforms into one
  private def concatenateTransforms(transforms: MathTransform*): MathTransform =
    transforms.reduceLeft(ConcatenatedTransform.create)

  // A set of function implementations that accumulates a pixel value into an accumulate value of type Array[Float]
  private val accumulators: Map[DataType, (Array[Float], _) => Unit] = Map(
    (ByteType, (acc: Array[Float], v: Byte) => {
      acc(0) += v
      acc(1) += 1
    }),
    (ShortType, (acc: Array[Float], v: Short) => {
      acc(0) += v
      acc(1) += 1
    }),
    (IntegerType, (acc: Array[Float], v: Int) => {
      acc(0) += v
      acc(1) += 1
    }),
    (LongType, (acc: Array[Float], v: Long) => {
      acc(0) += v
      acc(1) += 1
    }),
    (FloatType, (acc: Array[Float], v: Float) => {
      acc(0) += v
      acc(1) += 1
    }),
    (DoubleType, (acc: Array[Float], v: Double) => {
      acc(0) += v.toFloat
      acc(1) += 1
    }),
    (new ArrayType(ByteType, false), (acc: Array[Float], v: Array[Byte]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i)
      acc(v.length) += 1
    }),
    (new ArrayType(ShortType, false), (acc: Array[Float], v: Array[Short]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i)
      acc(v.length) += 1
    }),
    (new ArrayType(IntegerType, false), (acc: Array[Float], v: Array[Int]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i)
      acc(v.length) += 1
    }),
    (new ArrayType(LongType, false), (acc: Array[Float], v: Array[Long]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i)
      acc(v.length) += 1
    }),
    (new ArrayType(FloatType, false), (acc: Array[Float], v: Array[Float]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i)
      acc(v.length) += 1
    }),
    (new ArrayType(DoubleType, false), (acc: Array[Float], v: Array[Double]) => {
      // Handle any larger array with a for loop
      for (i <- v.indices)
        acc(i) += v(i).toFloat
      acc(v.length) += 1
    }),
  )

  // A set of functions to compute the average from the accumulate value and convert it to the desired type
  private val averageComputer: Map[DataType, Array[Float] => _] = Map(
    (ByteType, (acc: Array[Float]) => (acc(0) / acc(1)).round.toByte),
    (ShortType, (acc: Array[Float]) => (acc(0) / acc(1)).round.toShort),
    (IntegerType, (acc: Array[Float]) => (acc(0) / acc(1)).round),
    (LongType, (acc: Array[Float]) => (acc(0).toDouble / acc(1)).round),
    (FloatType, (acc: Array[Float]) => acc(0) / acc(1)),
    (DoubleType, (acc: Array[Float]) => acc(0).toDouble / acc(1)),
    (new ArrayType(ByteType, false), (acc: Array[Float]) => {
      val result = new Array[Byte](acc.length - 1)
      for (i <- result.indices)
        result(i) = (acc(i) / acc(result.length)).round.toByte
      result
    }),
    (new ArrayType(ShortType, false), (acc: Array[Float]) => {
      val result = new Array[Short](acc.length - 1)
      for (i <- result.indices)
        result(i) = (acc(i) / acc(result.length)).round.toShort
      result
    }),
    (new ArrayType(IntegerType, false), (acc: Array[Float]) => {
      val result = new Array[Integer](acc.length - 1)
      for (i <- result.indices)
        result(i) = (acc(i) / acc(result.length)).round
      result
    }),
    (new ArrayType(LongType, false), (acc: Array[Float]) => {
      val result = new Array[Long](acc.length - 1)
      for (i <- result.indices)
        result(i) = (acc(i).toDouble / acc(result.length)).round
      result
    }),
    (new ArrayType(FloatType, false), (acc: Array[Float]) => {
      val result = new Array[Float](acc.length - 1)
      for (i <- result.indices)
        result(i) = acc(i) / acc(result.length)
      result
    }),
    (new ArrayType(DoubleType, false), (acc: Array[Float]) => {
      val result = new Array[Double](acc.length - 1)
      for (i <- result.indices)
        result(i) = acc(i).toDouble / acc(result.length)
      result
    }),
  )

}
