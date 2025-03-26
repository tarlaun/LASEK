/*
 * Copyright 2018 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EmptyGeometry, EnvelopeNDLite, Feature, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{GridPartitioner, IndexHelper}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses.{AbstractHistogram, GeometryToPoints, HistogramOP, Prefix2DHistogram}
import edu.ucr.cs.bdlab.beast.util._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskFailureListener
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.{Envelope, Geometry, TopologyException}

import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.util.zip.ZipOutputStream

/**
  * Creates a multilevel visualization using the flat partitioning algorithm. The input is split in a non-spatial way
  * into splits. Each split is visualized as a full pyramid which contains all the tiles but the tiles are not final
  * because they do not cover all the data. Finally, the partial tiles are merged to produce final tile images which
  * are then written to the output.
  */
@OperationMetadata(
  shortName =  "mplot",
  description = "Plots the input file as a multilevel pyramid image",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat],
    classOf[CommonVisualizationHelper], classOf[MultilevelPyramidPlotHelper])
)
object MultilevelPlot extends CLIOperation with Logging {

  /** The width of the tile in pixels */
  @OperationParam(description = "The width of the tile in pixels", defaultValue = "256")
  val TileWidth = "width"
  /** The height of the tile in pixels */
  @OperationParam(description = "The height of the tile in pixels", defaultValue = "256")
  val TileHeight = "height"

  /** The number of levels for multilevel plot */
  @OperationParam(description =
"""The total number of levels for multilevel plot.
Can be specified as a range min..max (inclusive of both) or as a single number which indicates the number of levels starting at zero."""",
    defaultValue = "7") val NumLevels = "levels"

  /** Data tile threshold. Any tile that is larger than this size is considered an image tile */
  @OperationParam(description =
    """Image tile threshold for adaptive multilevel plot.
    Any tile that is strictly larger than (not equal) this threshold is considered an image tile""", defaultValue = "1m")
  val ImageTileThreshold = "threshold"

  @OperationParam(description = "The maximum size for the histogram used in adaptive multilevel plot",
    defaultValue = "32m",
    showInUsage = false)
  val MaximumHistogramSize = "MultilevelPlot.MaxHistogramSize"

  /**The name of the file that contains the hashtable of the tiles when the image tiles are concatenated*/
  val MasterTileFileName: String = "_tiles.hashtable"

  /** Java shortcut */
  def plotFeatures(features: JavaSpatialRDD, minLevel: Int, maxLevel: Int, plotterClass: Class[_ <: Plotter],
                   inputPath: String, outputPath: String, opts: BeastOptions): Unit =
    plotFeatures(features.rdd, minLevel to maxLevel, plotterClass, inputPath, outputPath, opts)

  /**
    * Plots the given features and writes the plot results to the output in a pyramid structure
    * @param features the set of features to plot
    * @param levels the range of levels to plot in the pyramid
    * @param plotterClass the plotter class used to generate the tiles
    * @param inputPath the input path. Used to add a link back to this path when the output is partial.
    * @param outputPath the output path where the tiles will be written.
    * @param opts user options for initializing the plotter and controlling the behavior of the operation
    */
  def plotFeatures(features: SpatialRDD, levels: Range, plotterClass: Class[_ <: Plotter],
                   inputPath: String, outputPath: String, opts: BeastOptions): Unit = {
    // Extract plot parameters
    val mbr = new EnvelopeNDLite(2)
    val maxHistogramSize = opts.getSizeAsBytes(MaximumHistogramSize, 32 * 1024 * 1024)
    val binSize = 8
    val gridDimension = MultilevelPyramidPlotHelper.computeHistogramDimension(maxHistogramSize, levels.max, binSize)
    logInfo(s"Creating a histogram with dimensions $gridDimension x $gridDimension")

    var featuresToPlot: SpatialRDD = features
    // Check if vflip is enabled
    val mercator = opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, defaultValue = false)
    // Check if the Web Mercator option is enabled
    if (mercator) {
      // Web mercator is enabled
      // Apply mercator projection on all features
      featuresToPlot = VisualizationHelper.toWebMercator(featuresToPlot)
      // Enforce an MBR that covers the entire world in Mercator projection
      val worldMBR = new EnvelopeNDLite(
        Reprojector.reprojectEnvelope(CommonVisualizationHelper.MercatorMapBoundariesEnvelope, DefaultGeographicCRS.WGS84,
        CommonVisualizationHelper.MercatorCRS))
      mbr.set(worldMBR)
      opts.set(CommonVisualizationHelper.VerticalFlip, value = true)
    } else {
      // No web mercator. Compute the MBR of the input
      // Compute the MBR to know the bounds of the input region
      logInfo("Computing geometric summary")
      val summary = featuresToPlot.summary
      mbr.set(summary)
      // Expand the MBR to keep the ratio if needed
      if (opts.getBoolean(CommonVisualizationHelper.KeepRatio, defaultValue = true)) {
        // To keep the aspect ratio, expand the MBR to make it a square
        if (mbr.getSideLength(0) > mbr.getSideLength(1)) {
          val diff = mbr.getSideLength(0) - mbr.getSideLength(1)
          mbr.setMinCoord(1, mbr.getMinCoord(1) - diff / 2.0)
          mbr.setMaxCoord(1, mbr.getMaxCoord(1) + diff / 2.0)
        } else if (mbr.getSideLength(1) > mbr.getSideLength(0)) {
          val diff = mbr.getSideLength(1) - mbr.getSideLength(0)
          mbr.setMinCoord(0, mbr.getMinCoord(0) - diff / 2.0)
          mbr.setMaxCoord(0, mbr.getMaxCoord(0) + diff / 2.0)
        }
      }
    }
    opts.set("mbr", mbr.encodeAsString())
    val sc = featuresToPlot.context

    // Now, the parameters have been adjusted, start the actual visualization logic
    val threshold: Long = opts.getSizeAsBytes(ImageTileThreshold, "1m")
    val tileWidth: Int = opts.getInt(TileWidth, 256)
    val tileHeight: Int = opts.getInt(TileHeight, 256)

    // Compute the uniform histogram and convert it to prefix sum for efficient sum of rectangles
    sc.setJobGroup("Histogram",
      s"Compute histogram of dimension $gridDimension x $gridDimension for visualization")
    logInfo(s"Computing a histogram of dimension $gridDimension x $gridDimension")
    val allPoints: SpatialRDD = featuresToPlot
      .flatMap(f => new GeometryToPoints(f.getGeometry))
      .map(g => Feature.create(null, g))
    val h: AbstractHistogram = if (threshold == 0) null else
      new Prefix2DHistogram(HistogramOP.computePointHistogramSparse(allPoints, _.getStorageSize, mbr, gridDimension, gridDimension))

    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val bufferSize: Double = plotter.getBufferSize / tileWidth.toDouble

    val outPath: Path = new Path(outputPath)
    val outFS: FileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf(null))

    // Determine the deepest level with image tile to limit the depth of the partitioning
    val maxLevelImageTile: Int = if (threshold == 0) levels.max
      else MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, threshold)
    logInfo(s"Maximum level with image tile $maxLevelImageTile")

    // Spatially partition the data to speed up the pyramid partitioning and visualization
    if (!features.isSpatiallyPartitioned || featuresToPlot.getNumPartitions < sc.defaultParallelism) {
      // Spatially repartition the data to increase parallelism and efficiency
      // The *10 is to account for data skewness with the uniform grid partitioning
      val desiredNumPartitions = (featuresToPlot.getNumPartitions max sc.defaultParallelism) * 10
      var gridDepth: Int = maxLevelImageTile
      while (1L << ((gridDepth - 1) * 2) > desiredNumPartitions )
        gridDepth -= 1
      val gridPartitioner = new GridPartitioner(mbr, Array[Int](1 << gridDepth, 1 << gridDepth))
      gridPartitioner.setDisjointPartitions(false)
      featuresToPlot = IndexHelper.partitionFeatures2(featuresToPlot, gridPartitioner)
    }

    val fullPartitioner = if (threshold == 0)
      new PyramidPartitioner(new SubPyramid(mbr, levels.min, maxLevelImageTile min levels.max))
    else
      new PyramidPartitioner(new SubPyramid(mbr, levels.min, maxLevelImageTile min levels.max), h, threshold + 1, Long.MaxValue)
    fullPartitioner.setBuffer(bufferSize)

    // Create all image tiles using flat partitioning
    val imageTiles = createImageTilesWithFlatPartitioning(featuresToPlot, mbr, plotterClass,
      tileWidth, tileHeight, fullPartitioner, opts)

    // Add information needed to write the index.html and _visualization.properties files
    opts.setClass(CommonVisualizationHelper.PlotterClassName, plotterClass, classOf[Plotter])
    opts.set(CommonVisualizationHelper.PlotterName, plotterClass.getAnnotation(classOf[Plotter.Metadata]).shortname())
    opts.set(NumLevels, rangeToString(levels))
    if (inputPath != null) {
      val inPath: Path = new Path(inputPath)
      opts.set("data", FileUtil.relativize(inPath, outPath).toString)
    }
    // Delete output path if already exists
    if (outFS.exists(outPath) && opts.getBoolean(SpatialWriter.OverwriteOutput, defaultValue = false))
      outFS.delete(outPath, true)
    saveImageTilesCompact(imageTiles, plotter, outPath.toString, opts)
  }

  /**
   * Save the given RDD of tiles to disk by writing each all tiles to a single file and adding a hashtable file
   * that points to where each tile is.
   * @param tiles the set of tiles to write
   * @param plotter the plotter that will write canvases to disk
   * @param outPath the output path (directory) to write the tiles file (single file) and hashtable file
   */
  def saveImageTilesCompact(tiles: RDD[Canvas], plotter: Plotter, outPath: String, _opts: BeastOptions): Unit = {
    val opts = new BeastOptions(tiles.context.hadoopConfiguration).mergeWith(_opts)
    val plotterBC = tiles.sparkContext.broadcast(plotter)
    // Write partial files to a temporary path first and then combine them into a single file with the desired name
    val tempPath = outPath+"_temp"
    // Writes tiles in each partition to a single ZIP file. Return the name of the ZIP file and the number of tiles
    val interimFiles: Array[(String, Int)] = tiles.sparkContext
      .runJob(tiles, (context: TaskContext, canvases: Iterator[Canvas]) => {
        // Each task writes a single ZIP file that contains all tiles in this partition
        val localZipFile: Path = new Path(tempPath, f"tiles-${context.partitionId()}-${context.taskAttemptId()}.zip")
        val outFS = localZipFile.getFileSystem(opts.loadIntoHadoopConf())
        // Delete the partial ZIP file on task failure
        context.addTaskFailureListener(new TaskFailureListener() {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            if (outFS.exists(localZipFile))
              outFS.delete(localZipFile, false)
          }
        })
        // Write all tiles to a single file
        val plotter = plotterBC.value
        val imgExt: String = plotter.getClass.getAnnotation(classOf[Plotter.Metadata]).imageExtension()
        val zipOut: ZipOutputStream = new ZipOutputStream(outFS.create(localZipFile))
        val vflip: Boolean = opts.getBoolean(CommonVisualizationHelper.VerticalFlip, defaultValue = true)
        val tempTileIndex: TileIndex = new TileIndex()
        val memTileOut = new ByteArrayOutputStream()
        var numTiles: Int = 0
        try {
          for (canvas <- canvases) {
            if (canvas != null) {
              numTiles += 1
              TileIndex.decode(canvas.tileID, tempTileIndex)
              if (vflip)
                tempTileIndex.y = ((1 << tempTileIndex.z) - 1) - tempTileIndex.y
              memTileOut.reset()
              plotter.writeImage(canvas, memTileOut, vflip)
              memTileOut.close()
              // Write in ZIP file. Use no compression to enable efficient access form the ZIP file directly.
              ZipUtil.putStoredFile(zipOut,
                s"tile-${tempTileIndex.z}-${tempTileIndex.x}-${tempTileIndex.y}$imgExt", memTileOut.toByteArray)
            }
          }
        } finally {
          zipOut.close()
        }
        if (numTiles == 0) {
          // No tiles were written, skip that file
          outFS.delete(localZipFile, false)
          (null, 0)
        } else {
          logDebug(s"Wrote $numTiles tiles in the file '$localZipFile'")
          (localZipFile.toString, numTiles)
        }
      }).filter(_._2 > 0)
    // Merge all local ZIP files into one to make it more portable
    val outFS = new Path(outPath).getFileSystem(opts.loadIntoHadoopConf())
    var tileOffsets = new Array[(Long, Long, Int)](0)
    if (!interimFiles.isEmpty) {
      // Compute prefix sum of number of tiles in each file, and prefix sum of file sizes
      // The start offset of each ZIP file in the merged one
      val zipFileStartOffset = new Array[Long](interimFiles.length)
      zipFileStartOffset(0) = 0
      for (i <- interimFiles.indices; if i > 0) {
        interimFiles(i) = (interimFiles(i)._1, interimFiles(i)._2 + interimFiles(i - 1)._2)
        zipFileStartOffset(i) = zipFileStartOffset(i - 1) + outFS.getFileStatus(new Path(interimFiles(i - 1)._1)).getLen
      }
      // interimFiles now contain file name and the index of the first tile in that file
      tileOffsets = new Array[(Long, Long, Int)](interimFiles.last._2)
      // Calculate tile offsets in all files
      val imgExt: String = plotter.getClass.getAnnotation(classOf[Plotter.Metadata]).imageExtension()
      val tileRegex = s"tile-(\\d+)-(\\d+)-(\\d+)$imgExt".r
      Parallel2.forEach(interimFiles.length, (i1, i2) => {
        for (iZipFile <- i1 until i2) {
          val localZipFile = interimFiles(iZipFile)._1
          val entryOffsets: Array[(String, Long, Long)] = ZipUtil.listFilesInZip(outFS, new Path(localZipFile))
          // Index of the first tile in this local ZIP file among all tiles
          val iFirstTile: Int = if (iZipFile == 0) 0 else interimFiles(iZipFile - 1)._2
          // Number of entries in the ZIP file should be equal to the difference in tile index between this and previous
          assert(entryOffsets.length == interimFiles(iZipFile)._2 - iFirstTile,
            s"Unexpected number of entries in ZIP file '$localZipFile'. " +
              s"Expected ${interimFiles(iZipFile)._2 - iFirstTile} but found ${entryOffsets.length}")
          for (iEntry <- entryOffsets.indices) {
            entryOffsets(iEntry)._1 match {
              case tileRegex(z, x, y) =>
                tileOffsets(iEntry + iFirstTile) = (TileIndex.encode(z.toInt, x.toInt, y.toInt),
                  zipFileStartOffset(iZipFile) + entryOffsets(iEntry)._2, entryOffsets(iEntry)._3.toInt)
            }
          }
        }
      })
    }
    // In a new ZIP file, write a hashtable with all tile offsets in the merged ZIP file
    val baos = new ByteArrayOutputStream()

    val masterZIPPath = new Path(tempPath, "master.zip")
    val masterZIPOut = new ZipOutputStream(outFS.create(masterZIPPath))
    // Write an index.html file in case someone extracts the ZIP archive
    val indexHTML: String = getIndexHTMLFile(_opts)
    ZipUtil.putStoredFile(masterZIPOut, "index.html", indexHTML.getBytes())
    // Write visualization properties to generate images on the fly
    val oformat: Option[String] = _opts.get(SpatialWriter.OutputFormat)
    if (oformat.isDefined)
      _opts.set(SpatialFileRDD.InputFormat, oformat.get)
    val prop = _opts.toProperties
    baos.reset()
    prop.store(baos, "Beast visualization properties")
    baos.close()
    ZipUtil.putStoredFile(masterZIPOut, "_visualization.properties", baos.toByteArray)
    // For efficiency, keep the master file with tile offsets as the last one to easily locate it
    baos.reset()
    val dataOut = new DataOutputStream(baos)
    DiskTileHashtable.construct(dataOut, tileOffsets)
    dataOut.close()
    ZipUtil.putStoredFile(masterZIPOut, MasterTileFileName, baos.toByteArray)
    masterZIPOut.close()

    // Final step, merge all ZIP files into one
    val mergedZIPFile = new Path(tempPath, "multilevelplot.zip")
    ZipUtil.mergeZip(outFS, mergedZIPFile, (interimFiles.map(x => new Path(x._1)) :+ masterZIPPath):_*)
    // Rename to the given output and add .zip to it
    val finalZIPPath = new Path(FileUtil.replaceExtension(outPath, ".zip"))
    outFS.rename(mergedZIPFile, finalZIPPath)
    outFS.delete(new Path(tempPath), true)
  }

  /**
   * Creates an index.html file that can display the generated tiles and returns it as strong
   * @param opts the visualization options
   * @return
   */
  def getIndexHTMLFile(opts: BeastOptions): String = {
    val tileWidth: Int = opts.getInt(SingleLevelPlotHelper.ImageWidth, 256)
    val tileHeight: Int = opts.getInt(SingleLevelPlotHelper.ImageHeight, 256)
    val mercator: Boolean = opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false)
    val plotter = opts.getClass(CommonVisualizationHelper.PlotterClassName, null, classOf[Plotter])
    val strLevels: Array[String] = opts.getString("levels", "7").split("\\.\\.")
    var minLevel: Int = 0
    var maxLevel: Int = 0
    if (strLevels.length == 1) {
      minLevel = 0
      maxLevel = strLevels(0).toInt - 1
    }
    else {
      minLevel = strLevels(0).toInt
      maxLevel = strLevels(1).toInt
    }
    val templateFile = "/zoom_view.html"
    val templateFileReader = new LineReader(classOf[MultilevelPyramidPlotHelper].getResourceAsStream(templateFile))
    val htmlOut = new StringBuilder
    try {
      val line = new Text
      val imageExtension = Plotter.getImageExtension(plotter)
      while ( {
        templateFileReader.readLine(line) > 0
      }) {
        var lineStr = line.toString
        lineStr = lineStr.replace("#{MERCATOR}", mercator.toString)
        lineStr = lineStr.replace("#{TILE_WIDTH}", Integer.toString(tileWidth))
        lineStr = lineStr.replace("#{TILE_HEIGHT}", Integer.toString(tileHeight))
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel))
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel))
        lineStr = lineStr.replace("#{TILE_URL}", "tile-{z}-{x}-{y}" + imageExtension)
        htmlOut.append(lineStr)
        htmlOut.append("\n")
      }
    } finally {
      templateFileReader.close()
    }
    htmlOut.toString()
  }

  /** Java shortcut */
  @throws(classOf[IOException])
  def plotGeometries(geoms: JavaRDD[_ <: Geometry], minLevel: Int, maxLevel: Int, outPath: String,
                     opts: BeastOptions): Unit =
    plotGeometries(geoms.rdd, minLevel to maxLevel, outPath, opts)

  /**
    * Plots the given set of geometries and writes the generated image to the output directory. This function uses the
    * [[edu.ucr.cs.bdlab.davinci.GeometricPlotter]] to plot the geometry of the shapes and if data tiles are written
    * to the output, it will use CSV file format by default.
    * @param geoms the set of geometries to plot
    * @param levels the range of levels to generate
    * @param outPath the path to the output directory
    * @param opts additional options to customize the behavior of the plot
    */
  @throws(classOf[IOException])
  def plotGeometries(geoms: RDD[_ <: Geometry], levels: Range, outPath: String, opts: BeastOptions): Unit = {
    // Convert geometries to features to be able to use the standard function
    val features: SpatialRDD = geoms.map(g => Feature.create(null, g))

    // Set a default output format if not set
    if (opts.get(SpatialFileRDD.InputFormat) == null && opts.get(SpatialWriter.OutputFormat) == null)
      opts.set(SpatialWriter.OutputFormat, "wkt")

    opts.set(NumLevels, s"${levels.min}..${levels.max}")

    plotFeatures(features, levels, classOf[GeometricPlotter], null, outPath, opts)
  }

  /**
    * Use flat partitioning to plot all tiles in the given range of zoom levels. First, all the partitions are scanned
    * and a set of partial tiles are created based on the data in each partition. Then, these partial tiles are reduced
    * by tile ID so that all partial images for each tile are merged into one final tile. Finally, the final tiles
    * are written to the output using the naming convention tile-z-x-y
    * @param features the set of features to visualize as an RDD
    * @param featuresMBR the MBR of the features
    * @param plotterClass the class to use as a plotter
    * @param tileWidth the width of each tile in pixels
    * @param tileHeight the height of each tile in pixels
    * @param partitioner the pyramid partitioner that selects the tiles to plot using flat partitioning
    * @param opts any other used-defined options
    * @return the created tiles as an RDD of TileID and Canvas
   */
  private[davinci] def createImageTilesWithFlatPartitioning(features: SpatialRDD, featuresMBR: EnvelopeNDLite,
                                                            plotterClass: Class[_ <: Plotter], tileWidth: Int, tileHeight: Int,
                                                            partitioner: PyramidPartitioner, opts: BeastOptions): RDD[Canvas] = {
    logInfo(s"Using flat plotting with partitioner $partitioner")
    features.context.setJobGroup("Image tiles", "Image tiles using flat partitioning " +
      s"in levels [${partitioner.pyramid.getMinimumLevel},${partitioner.pyramid.getMaximumLevel}] " +
      s"with sizes [${partitioner.getMinThreshold},${partitioner.getMaxThreshold})")
    val partitionerBroadcast = features.sparkContext.broadcast(partitioner)
    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      mbr.merge(feature.getGeometry)
      partitionerBroadcast.value.overlapPartitions(mbr).map(tileid => (tileid, feature))
    })
    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val partialTiles: RDD[(Long, Canvas)] = featuresAssignedToTiles.mapPartitions(partitionedFeatures =>
      new TileCreatorFlatPartiotioning(partitionedFeatures, featuresMBR, plotter, tileWidth, tileHeight))
    // Merge partial tiles
    val finalTiles: RDD[(Long, Canvas)] = partialTiles.reduceByKey((c1, c2) => plotter.merge(c1, c2))
    finalTiles.map(_._2)
  }

  /**
   * Use pyramid partitioning to plot all the images in the given range of zoom levels. First, this method scans all
   * the features and assigns each feature to all image tiles that it overlaps with in the given range of zoom levels.
   * Then, the records are grouped by tile ID and the records in each group (i.e., tile ID) are plotted. Finally,
   * the created image tiles are written to the output using the naming convention tile-z-x-y.
   * This method should be used when the size of each image tile is small enough so that all records in a tile
   * can be grouped together in memory before plotting.
   * @param features the set of features to plot as RDD
   * @param featuresMBR the MBR of the features
   * @param plotterClass the class of the plotter to use
   * @param tileWidth the width of each tile in pixels
   * @param tileHeight the height of each tile in pixels
   * @param partitioner the pyramid partitioner that chooses which tiles to plot
   * @param opts user-defined options
   * @return an RDD of TileID and the canvas
   */
  private[davinci] def createImageTilesWithPyramidPartitioning(features: SpatialRDD, featuresMBR: EnvelopeNDLite,
                                                               plotterClass: Class[_ <: Plotter], tileWidth: Int,
                                                               tileHeight: Int, partitioner: PyramidPartitioner,
                                                               opts: BeastOptions): RDD[Canvas] = {
    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val partitionerBroadcast = features.sparkContext.broadcast(partitioner)
    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      mbr.merge(feature.getGeometry)
      val allMatches: Array[Long] = partitionerBroadcast.value.overlapPartitions(mbr)
      if (allMatches.length > 1000)
        logWarning(s"!!Matched ${allMatches.length} tiles in pyramid partitioning")
      if (allMatches.length < 100)
        allMatches.map(pid => (pid, feature))
      else {
        // When number of matches is too high, we clip the feature to each
        val tilembr = new Envelope()
        allMatches.map(tileid => {
          try {
            TileIndex.getMBR(featuresMBR, tileid, tilembr)
            val clippedGeometry = feature.getGeometry.intersection(feature.getGeometry.getFactory.toGeometry(tilembr))
            (tileid, Feature.create(feature, clippedGeometry))
          } catch {
            case _: TopologyException => (tileid, Feature.create(null, EmptyGeometry.instance))
          }
        })
      }
    })

    // Repartition and sort within partitions
    val finalCanvases: RDD[Canvas] = featuresAssignedToTiles.repartitionAndSortWithinPartitions(new HashPartitioner(features.getNumPartitions))
      .mapPartitions(sortedFeatures => new TileCreatorPyramidPartitioning(sortedFeatures, partitioner, plotter, tileWidth, tileHeight))
    finalCanvases
  }


  /**
   * This method is a hybrid between flat and pyramid partitioning. It starts by assigning records to tiles.
   * Then to create tiles, it first create partial tiles, shuffles the partial tiles, and merges them to create
   * final tiles. Unlike flat partitioning, it uses aggregateByKey method to make partial and final tiles.
   * @param features the set of features to plot as RDD
   * @param featuresMBR the MBR of the features
   * @param plotterClass the class of the plotter to use
   * @param tileWidth the width of each tile in pixels
   * @param tileHeight the height of each tile in pixels
   * @param partitioner the pyramid partitioner that chooses which tiles to plot
   * @param opts user-defined options
   * @return an RDD of TileID and the canvas
   */
  private[davinci] def createImageTilesHybrid(features: SpatialRDD, featuresMBR: EnvelopeNDLite,
                                              plotterClass: Class[_ <: Plotter], tileWidth: Int,
                                              tileHeight: Int, partitioner: PyramidPartitioner,
                                              opts: BeastOptions): RDD[Canvas] = {
    val plotter: Plotter = Plotter.getConfiguredPlotter(plotterClass, opts)
    val partitionerBroadcast = features.sparkContext.broadcast(partitioner)
    features.context.setJobDescription(s"Used hybrid partitioner $partitioner")
    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      mbr.merge(feature.getGeometry)
      val par = partitionerBroadcast.value
      val matchedTiles = par.overlapPartitions(mbr)
      if (matchedTiles.length < 100) {
        // Small object matches a few tiles, assign it directly to all matching tiles
        matchedTiles.map(pid => (pid, feature))
      } else {
        // When number of matches is too high, we clip the feature to each tile and only replicate to matching ones
        val tilembr = new Envelope()
        matchedTiles.map(tileid => {
          try {
            TileIndex.getMBR(featuresMBR, tileid, tilembr)
            val clippedGeometry = feature.getGeometry.intersection(feature.getGeometry.getFactory.toGeometry(tilembr))
            (tileid, Feature.create(feature, clippedGeometry))
          } catch {
            // If an error happens while computing the intersection, just return an empty geometry
            case _: TopologyException => (tileid, Feature.create(null, EmptyGeometry.instance))
          }
        }).filter(!_._2.getGeometry.isEmpty)
      }
    })

    // Since the canvas is initialized differently for each key, we start with null and initialize for the first record
    val emptyCanvas = CanvasModified(null)
    // The aggregate by key function does not receive the key, so we replicate the key to each record since we need it
    featuresAssignedToTiles.map({ case (tileId, canvas) => (tileId, (tileId, canvas)) })
      .aggregateByKey(emptyCanvas) ((canvas, tileFeature) => {
        // Initialize canvas if not initialized
        val c = if (canvas.canvas == null) {
          val tilembr = new Envelope()
          TileIndex.getMBR(featuresMBR, tileFeature._1, tilembr)
          CanvasModified(plotter.createCanvas(tileWidth, tileHeight, tilembr, tileFeature._1))
        } else {
          canvas
        }
        // Plot the record
        c.modified = plotter.plot(c.canvas, tileFeature._2) || c.modified
        if (c.modified) c else emptyCanvas
      }, (canvas1, canvas2) => {
        if (!canvas2.modified) {
          canvas1
        } else if (!canvas1.modified) {
          canvas2
        } else {
          plotter.merge(canvas1.canvas, canvas2.canvas)
          canvas1.modified = canvas1.modified || canvas2.modified
          canvas1
        }
      })
      .map(cf => cf._2.canvas)
  }

  /**
    * A helper function parses a string into range. The string is in the following forms:
    * - 8: Produces the range [0, 8) exclusive of 8, i.e., [0,7]
    * - 3..6: Produces the range [3, 6], inclusive of both 3 and 4
    * - 4...7: Produces the range [4, 7), exclusive of the 7, i.e., [4, 6]
    * @param str the string to parse
    * @return the created range
    */
  def parseRange(str: String): Range = {
    val oneNumber = raw"(\d+)".r
    val inclusiveRange = raw"(\d+)..(\d+)".r
    val exclusiveRange = raw"(\d+)...(\d+)".r
    str match {
      case oneNumber(number) => 0 until number.toInt
      case inclusiveRange(start, end) => start.toInt to end.toInt
      case exclusiveRange(start, end) => start.toInt until end.toInt - 1
      case _ => throw new RuntimeException(s"Unrecognized range format $str. start..end is a supported format")
    }
  }

  def rangeToString(rng: Range): String = {
    if (rng.start == 0) {
      (rng.last + 1).toString
    } else {
      s"${rng.start}..${rng.last}"
    }
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options for the operation
    * @param sc the spark context to use
    * @return
    */
  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val levels: Range = parseRange(opts.getString(NumLevels, "7"))
    val plotterClass: Class[_ <: Plotter] = Plotter.getPlotterClass(opts.getString(CommonVisualizationHelper.PlotterName))
    plotFeatures(features, levels, plotterClass, inputs(0), outputs(0), opts)
  }
}
