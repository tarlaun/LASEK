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
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialDataTypes}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, GeometryReader, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.{GridPartitioner, IndexHelper}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.SpatialFilePartition
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.synopses.{AbstractHistogram, GeometryToPoints, HistogramOP, Prefix2DHistogram}
import edu.ucr.cs.bdlab.beast.util.{FileUtil, OperationMetadata, OperationParam, Parallel2, ZipUtil}
import edu.ucr.cs.bdlab.davinci.MultilevelPlot.MasterTileFileName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, LongWritable, Text}
import org.apache.hadoop.util.LineReader
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskFailureListener
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform}
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.AffineTransform
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, OutputStream}
import java.util.zip.ZipOutputStream

/**
 * A command-line tool for visualizing datasets into MVT files (Vector Tiles).
 * This class has three main functionalities:
 *  - Given a large dataset (RDD) and a specific tile, it scans the entire dataset to generate that single tile.
 *  - Given a large dataset and a range of levels, it generates all tiles in all those levels.
 *  - Given an indexed on-disk dataset and a specific tile, it uses the index to quickly visualize that tile.
 */
@OperationMetadata(
  shortName =  "vmplot",
  description = "Plots the input file as a multilevel pyramid image with mvt tiles",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat])
)
object MVTDataVisualizer extends CLIOperation with Logging {
  private val WebMercatorMBR: EnvelopeNDLite = new EnvelopeNDLite(2,
    -20037508.34, -20037508.34, 20037508.34, 20037508.34)

  @OperationParam(description = "The maximum size for the histogram used in adaptive multilevel plot",
    defaultValue = "32m",
    showInUsage = false)
  val MaximumHistogramSize = "VectorTilePlotter.MaxHistogramSize"

  /** Data tile threshold. Any tile that is larger than this size is considered an image tile */
  @OperationParam(description =
    """Image tile threshold for adaptive multilevel plot.
  Any tile that is strictly larger than (not equal) this threshold is considered an image tile""", defaultValue = "1m")
  val ImageTileThreshold = "threshold"

  /** The number of levels for multilevel plot */
  @OperationParam(description =
    """The total number of levels for multilevel plot.
    Can be specified as a range min..max (inclusive of both) or as a single number which indicates the number of levels starting at zero."""",
    defaultValue = "7") val NumLevels = "levels"

  @OperationParam(description = "The resolution of the vector tile in pixels", defaultValue = "256")
  val Resolution = "resolution"

  @OperationParam(description = "Write all output files in a compact ZIP archive", defaultValue = "true")
  val CompactOutput = "compact"

  /**
   * Plots the given set of features as a vector tile according to Mapbox specifications using a Spark job.
   * @param features the set of features to plot
   * @param resolution the resolution of the image in pixels
   * @param tileID the ID of the tile to plot
   * @param buffer additional pixels around the tile to plot from all directions (default is zero)
   * @param opts additional options to customize the plotting
   * @return a vector tile that contains all the given features
   */
  def plotSingleTileParallel(features: SpatialDataTypes.SpatialRDD, resolution: Int,
                             tileID: Long, buffer: Int = 0, opts: BeastOptions = new BeastOptions()): VectorTile.Tile = {
    // 1. Calculate the MBR of the region of interest based on the tile index in the WebMercator CRS
    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    // Set up a transformation that transforms geometries to the image space
    // 1. Convert to web mercator
    val sourceSRID = features.first().getGeometry.getSRID
    val sourceToWebMercator: MathTransform =
      Reprojector.findTransformationInfo(sourceSRID, 3857).mathTransform
    // 2. Convert from web mercator to image space
    val webMercatorToImageTile: AffineTransform = new AffineTransform()
    webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)
    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    val dataToImage = ConcatenatedTransform.create(ConcatenatedTransform.create(sourceToWebMercator, verticalFlip),
      new AffineTransform2D(webMercatorToImageTile))
    val imageToData = dataToImage.inverse()

    // Get the extents of the input region that we want to process
    val sourceExtents = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
    imageToData.transform(sourceExtents, 0, sourceExtents, 0, 2)
    val sourceExtentsEnvelope = GeometryReader.DefaultGeometryFactory.toGeometry(
      new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    )
    val featuresOfInterest = features.rangeQuery(sourceExtentsEnvelope)
      // Clip records to the extents of the image
      .map(f => Feature.create(f, f.getGeometry.intersection(sourceExtentsEnvelope)))
      // Keep only features with non-empty geometries
      .filter(!_.getGeometry.isEmpty)
      // Convert to image space
      .map(f => { Feature.create(f, JTS.transform(f.getGeometry, dataToImage)) })

    // Aggregate all the features into a single tile
    val emptyTile = new IntermediateVectorTile(resolution, buffer)
    val finalTile: VectorTile.Tile = featuresOfInterest.aggregate(emptyTile)(
      (tile, feature) => tile.addFeature(feature),
      (tile1, tile2) => tile1.merge(tile2)
    ).vectorTile

    finalTile
  }

  /**
   * Computes the affine transformation that transforms points from the data space (WebMercator) to the image
   * space of the given tile
   * @param tileID the ID of the tile
   * @param resolution the resolution of the tile in pixels
   * @return the affine transformation that transforms points from WebMercator space to the image space of this tile
   */
  def dataToImageTransformer(tileID: Long, resolution: Int): AffineTransform = {
    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    val dataToImage: AffineTransform = new AffineTransform()
    dataToImage.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    dataToImage.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)
    dataToImage
  }

  /**
   * Plots all tiles in a range of Zoom levels according to the provided specifications and configuration/
   *
   * @param features   the set of features to visualize
   * @param minLevel   the minimum level to visualize (inclusive)
   * @param maxLevel   the maximum level to visualize (inclusive)
   * @param resolution the resolution of each tile
   * @param buffer     the buffer around each tile to consider when visualizing
   * @param opts       additional options for generating the tiles
   * @return an RDD that contains all the generated tiles along with their IDs.
   */
  def plotAllTiles(features: SpatialDataTypes.JavaSpatialRDD, minLevel: Int, maxLevel: Int, resolution: Int,
                   buffer: Int, opts: BeastOptions): JavaPairRDD[java.lang.Long, IntermediateVectorTile] =
    JavaPairRDD.fromRDD(plotAllTiles(features.rdd, minLevel to maxLevel, resolution, buffer, opts)
      .map(f => (java.lang.Long.valueOf(f._1), f._2)))

  /**
   * Plots all tiles in a range of Zoom levels according to the provided specifications and configuration/
   * @param features the set of features to visualize
   * @param levels the range of levels to generate
   * @param resolution the resolution of each tile
   * @param buffer the buffer around each tile to consider when visualizing
   * @param opts additional options for generating the tiles
   * @return an RDD that contains all the generated tiles along with their IDs.
   */
  def plotAllTiles(features: SpatialDataTypes.SpatialRDD, levels: Range, resolution: Int,
                   buffer: Int = 0, opts: BeastOptions = new BeastOptions()): RDD[(Long, IntermediateVectorTile)] = {
    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    // Spatially partition the data to speed up the pyramid partitioning and visualization
    var featuresToPlot: SpatialDataTypes.SpatialRDD = features
    val sc = features.sparkContext
    // project the world rectangle to web mercator (EPSG:3857)
    featuresToPlot = VisualizationHelper.toWebMercator(
      featuresToPlot.map(f => if (f.getGeometry.isValid) f else Feature.create(f, f.getGeometry.buffer(0)))
      ).map(f => Feature.create(f, JTS.transform(f.getGeometry, verticalFlip)).asInstanceOf[IFeature])

    val maxHistogramSize = opts.getSizeAsBytes(MaximumHistogramSize, 32 * 1024 * 1024)
    val binSize = 8
    val gridDimension = MultilevelPyramidPlotHelper.computeHistogramDimension(maxHistogramSize, levels.max, binSize)
    logInfo(s"Creating a histogram with dimensions $gridDimension x $gridDimension")

    val threshold: Long = opts.getSizeAsBytes(ImageTileThreshold, "1m")
    val allPoints: SpatialRDD = featuresToPlot
      .flatMap(f => new GeometryToPoints(f.getGeometry))
      .map(g => Feature.create(null, g))
    val h: AbstractHistogram = if (threshold == 0) null else new Prefix2DHistogram(HistogramOP.computePointHistogramSparse(allPoints,
      _.getStorageSize, WebMercatorMBR, gridDimension, gridDimension))

    val maxLevelImageTile: Int = if (threshold == 0) levels.max
    else MultilevelPyramidPlotHelper.findDeepestTileWithSize(h, threshold)
    logInfo(s"Maximum level with image tile $maxLevelImageTile")

    if (!features.isSpatiallyPartitioned || featuresToPlot.getNumPartitions < sc.defaultParallelism) {
      // Spatially repartition the data to increase parallelism and efficiency
      // The *10 is to account for data skewness with the uniform grid partitioning
      val desiredNumPartitions = (featuresToPlot.getNumPartitions max sc.defaultParallelism) * 10
      var gridDepth: Int = maxLevelImageTile
      while (1L << ((gridDepth - 1) * 2) > desiredNumPartitions)
        gridDepth -= 1
      val gridPartitioner = new GridPartitioner(WebMercatorMBR, Array[Int](1 << gridDepth, 1 << gridDepth))
      // Disable disjoint partitions to avoid replication of large geometries
      gridPartitioner.setDisjointPartitions(false)
      logInfo(s"Repartitioning features using the grid $gridPartitioner")
      featuresToPlot = IndexHelper.partitionFeatures2(featuresToPlot, gridPartitioner)
    }

    // create the pyramid from minimum zoom level up to the maximum zoom level
    val pyramidPartitioner = if (threshold == 0)
      new PyramidPartitioner(new SubPyramid(WebMercatorMBR, levels.min, levels.max))
    else
      new PyramidPartitioner(new SubPyramid(WebMercatorMBR, levels.min, maxLevelImageTile min levels.max), h, threshold + 1, Long.MaxValue)
    pyramidPartitioner.setBuffer(buffer.toFloat / resolution)
    logInfo(s"Using pyramid partitioner ${pyramidPartitioner}")

    // partition the pyramid for each feature to find overlapping tiles
    val partitionedFeatures: RDD[(Long, IFeature)] = featuresToPlot.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2)
      mbr.merge(feature.getGeometry)
      pyramidPartitioner.overlapPartitions(mbr).map(tileid => (tileid, feature))
    })

    // Create tiles and merge into final tiles
    val finalTiles: RDD[(Long, IntermediateVectorTile)] = partitionedFeatures
      .mapPartitions(new VectorTileCreatorFlatPartitioning(_, resolution, buffer))
      .reduceByKey(_.merge(_))

    finalTiles
  }

  /**
   * Plots a single tile using local code, i.e., no Spark RDD. This method should only be used to plot tiles
   * that contains very little data and when the input is already partitioned and R-tree-indexed. Otherwise, this
   * method will take too long and it is advised to use [[plotSingleTileParallel()]]
   * @param fs the file system that contains the input
   * @param vizPath the path of the input data. If the data is indexed, this path points to the directory.
   * @param tileID the tile ID to generate
   * @param out the output stream to write the output to
   * @param clientTimestamp the timestamp of any client-cached version. If none is cached, this is set to zero.
   *                        the timestamp of the generated tile is set in this parameter as output
   * @param resolution the resolution of the tile to generate
   * @param buffer the buffer around the tile to consider when visualizing.
   * @param opts additional options for visualization
   */
  def plotSingleTileLocal(fs: FileSystem, vizPath: Path, tileID: Long, out: OutputStream, clientTimestamp: LongWritable,
                          resolution: Int, buffer: Int = 0, opts: BeastOptions = new BeastOptions()): Unit = {
    // 1. Retrieve partitions from Input Path
    val featureReaderClass = SpatialFileRDD.getFeatureReaderClass(vizPath.toString, opts)
    var partitions = SpatialFileRDD.createPartitions(vizPath.toString, opts, new Configuration(fs.getConf))
    // Read the first partition to retrieve data SRID
    val sourceSRID: Int = {
      val reader = SpatialFileRDD.readPartition(partitions.head, featureReaderClass, applyDuplicateAvoidance = false, opts)
      if (reader.hasNext) {
        val srid = reader.next().getGeometry.getSRID
        // Close
        reader match {
          case closeable: java.io.Closeable => closeable.close()
          case _ =>
        }
        srid
      } else {
        0
      }
    }

    // 2. Calculate the MBR of the region of interest based on the tile index in the WebMercator CRS
    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    // Set up a transformation that transforms geometries to the image space
    // 3. Convert to web mercator
    val sourceToWebMercator: MathTransform =
      Reprojector.findTransformationInfo(sourceSRID, 3857).mathTransform
    // 4. Convert from web mercator to image space
    val webMercatorToImageTile: AffineTransform = new AffineTransform()
    webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, resolution / tileMBRMercator.getHeight)
    webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMinY)
    val verticalFlip = new AffineTransform2D(AffineTransform.getScaleInstance(1, -1))
    val dataToImage = ConcatenatedTransform.create(ConcatenatedTransform.create(sourceToWebMercator, verticalFlip),
      new AffineTransform2D(webMercatorToImageTile))
    val imageToData = dataToImage.inverse()

    // 5. Get the extents of the input region that we want to process
    val sourceExtents = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
    imageToData.transform(sourceExtents, 0, sourceExtents, 0, 2)
    val y1 = sourceExtents(1)
    val y2 = sourceExtents(3)
    sourceExtents(1) = y1 min y2
    sourceExtents(3) = y1 max y2
    // Set as filter MBR for efficient reading of indexed files.
    opts.set(SpatialFileRDD.FilterMBR, sourceExtents.mkString(","))
    val sourceEnv = new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    val sourceExtentsEnvelope = GeometryReader.DefaultGeometryFactory.toGeometry(
      new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    )

    // 6. Filter partitions that overlap the source extents of the tile
    partitions = partitions.filter {
      case x: SpatialFilePartition => x.mbr.intersectsEnvelope(sourceEnv)
      case _ => true
    }

    // 7. Scan the records in matching partitions and process them
    val resultTile = new IntermediateVectorTile(resolution, buffer, dataToImage)
    for (i <- partitions.indices) {
      // get the features overlapping with the partition
      val reader: Iterator[IFeature] = SpatialFileRDD.readPartition(partitions(i), featureReaderClass,
        applyDuplicateAvoidance = false, opts)
      try {
        for (feature <- reader) {
          if (feature.getGeometry.intersects(sourceExtentsEnvelope)) {
            resultTile.addFeature(feature)
          }
        }
      } finally reader match {
        case closeable: java.io.Closeable => closeable.close()
        case _ =>
      }
    }

    // Visualize the features
    resultTile.vectorTile.writeTo(out)
    out.close()
  }

  /**
   * Retrieves a tile if it is already created or create it on the fly.
   * The function does the following:
   *  1. If the input is a directory and contains the given tile `tile-z-x-y.mvt`, the tile data is returned.
   *  1. If the input is a directory and does not contains the file, the file `_visualization.properties` is used
   *    to locate the data index and the tile is generated on the fly.
   *  1. If the input is a `plot.zip` file and contains the give tile, the tile data is returned.
   *  1. If the input is a `plot.zip` file and does not contain the given tile, the file `_visualization.properties`
   *    is used to locate the data index and the tile is generated on the fly.
   *  1. If the input is a directory of a data index, it is used directly to generate the given tile.
   * @param fs the file system that contains the visualization and the data
   * @param plotPath the path to the plotted data. This is either a directory or a plot.zip file.
   * @param tileID the ID of the tile to retrieve
   * @param out the output stream to write the visualized tile to
   * @param clientTimestamp (in/out) Contains the timestamp of a client-cached version. If the data is newer,
   *                        the timestamp of the generated tile is returned in that parameter
   * @param opts additional BeastOptions to override existing ones
   * @return whether a non-empty tile was returned or not.
   */
  def retrieveOrCreateTile(fs: FileSystem, plotPath: Path, tileID: Long, out: OutputStream,
                           clientTimestamp: LongWritable, opts: BeastOptions): Boolean = {
    var dataIndex: Path = null
    var tileFile: FSDataInputStream = null
    var tileFileLength: Long = 0
    val fileStatus: FileStatus = fs.getFileStatus(plotPath)
    var vizOpts: BeastOptions = null
    if (fileStatus.isFile) {
      // The client has an up-to-date version
      if (fileStatus.getModificationTime <= clientTimestamp.get)
        return false
      // A zip file that contains all plot file
      val infoFiles = ZipUtil.lastNFiles(fs, plotPath, 3)
      assert(infoFiles.exists(_._1 == MasterTileFileName),
        s"Error! Could not find file '$MasterTileFileName' in file '$plotPath'")
      val tilesHashtable: (String, Long, Long) = infoFiles.find(_._1 == MasterTileFileName).get
      val plotDataIn = fs.open(plotPath)
      plotDataIn.seek(tilesHashtable._2)
      val tileLocation: (Long, Int) = DiskTileHashtable.getValue(plotDataIn, tilesHashtable._2, tileID)
      if (tileLocation != null) {
        // Found the tile. Point to its location
        tileFile = plotDataIn
        tileFile.seek(tileLocation._1)
        tileFileLength = tileLocation._2
      } else {
        // Tile not found. Need to generate it on-the-fly
        assert(infoFiles.exists(_._1 == "_visualization.properties"),
          s"Error! Could not find file '_visualization.properties' in file '$plotPath'")
        val visualizationProperties: (String, Long, Long) = infoFiles.find(_._1 == "_visualization.properties").get
        val propertiesData = new Array[Byte](Math.toIntExact(visualizationProperties._3))
        plotDataIn.seek(visualizationProperties._2)
        plotDataIn.readFully(propertiesData)
        plotDataIn.close()
        vizOpts = new BeastOptions()
        vizOpts.loadFromTextFile(new ByteArrayInputStream(propertiesData))
        dataIndex = new Path(plotPath, vizOpts.getString("data"))
      }
    } else if (fileStatus.isDirectory) {
      // A directory that either contains plot data or indexed data
      val isIndex: Boolean = SpatialFileRDD.getMasterFilePath(fs, plotPath) != null
      if (isIndex) {
        // The given path points to indexed data
        dataIndex = plotPath
      } else {
        // Assume it is a directory of visualization files
        // First, search if the desired tile is pre-generated
        val tileIndex = new TileIndex()
        TileIndex.decode(tileID, new TileIndex())
        val tilePath = new Path(plotPath, s"tile-${tileIndex.z}-${tileIndex.x}-${tileIndex.y}.mvt")
        if (fs.exists(tilePath)) {
          tileFileLength = fs.getFileStatus(tilePath).getLen
          tileFile = fs.open(tilePath)
        } else {
          // Tile not pre-generated, need to generate it on the fly
          val propertiesFile = new Path(plotPath, "_visualization.properties")
          if (!fs.exists(propertiesFile))
            throw new RuntimeException(s"Cannot find '_visualization.properties' in '$plotPath'")
          vizOpts = new BeastOptions(false)
          vizOpts.loadFromTextFile(fs, propertiesFile)
          dataIndex = new Path(plotPath, vizOpts.getString("data"))
        }
      }
    } else {
      throw new RuntimeException(s"Cannot support file type $fileStatus")
    }
    // At this point, either we will load a pre-generated tile or generate one on the fly
    if (tileFile != null) {
      // There is a pre-generated tile that we will load
      IOUtils.copyBytes(tileFile, out, tileFileLength, true)
      true
    } else if (dataIndex != null) {
      // Tile will be generated on the fly
      if (vizOpts != null)
        vizOpts.mergeWith(opts)
      else
        vizOpts = opts
      val resolution: Int = vizOpts.getInt(MVTDataVisualizer.Resolution, 256)
      val buffer: Int = vizOpts.getInt("buffer", 5)
      plotSingleTileLocal(fs, dataIndex, tileID, out, clientTimestamp, resolution, buffer, vizOpts)
      true
    } else {
      throw new RuntimeException(s"Could not generate tile $tileID from path $plotPath")
    }
  }

  /**
   * Save an RDD of tiles to the given output
   *
   * @param tiles   the set of tiles to save
   * @param outPath the path to save the tiles to
   */
  def saveTiles(tiles: JavaPairRDD[java.lang.Long, IntermediateVectorTile], outPath: String, opts: BeastOptions): Unit =
    saveTiles(tiles.rdd.map(f => (f._1.longValue(), f._2)), outPath, opts)

  /**
   * Save an RDD of tiles to the given output
   * @param tiles the set of tiles to save
   * @param outPath the path to save the tiles to
   */
  def saveTiles(tiles: RDD[(Long, IntermediateVectorTile)], outPath: String, opts: BeastOptions): Unit = {
    val conf: BeastOptions = new BeastOptions(opts.loadIntoHadoopConf(tiles.sparkContext.hadoopConfiguration))
    tiles.foreach({
      case (id, tile) =>
        val tileIndex = new TileIndex()
        TileIndex.decode(id, tileIndex) // decode the tileId (Long -> z-x-y)
        val path = new Path(outPath, s"tile-${tileIndex.z}-${tileIndex.x}-${tileIndex.y}.mvt")
        val fs = path.getFileSystem(conf.loadIntoHadoopConf(new Configuration(false)))
        val output = fs.create(path)
        tile.vectorTile.writeTo(output) // write the IntermediateTile to output
        output.close()
    })
    // Save an index.html viewer for the files
    val htmlPath: Path = new Path(outPath, "index.html")
    val indexHTML: String = getIndexHTMLFile(conf)
    val outFS = htmlPath.getFileSystem(conf.loadIntoHadoopConf(null))
    val htmlOut = outFS.create(htmlPath)
    htmlOut.write(indexHTML.getBytes("utf-8"))
    htmlOut.close()
    // Save _visualization.properties for on-the-fly data generation
    val vizPropertiesPath: Path = new Path(outPath, "_visualization.properties")
    val out = outFS.create(vizPropertiesPath)
    val prop = opts.toProperties
    prop.store(out, "Beast visualization properties")
    out.close()
  }

  /**
   * A wrapper around [[saveTilesCompact()]]. Saves all the given tiles to a ZIP file.
   *
   * @param tiles   the set of tiles ot visualize
   * @param outPath the path of the output ZIP file
   * @param _opts   additional options that were used for visualization.
   */
  def saveTilesCompact(tiles: JavaPairRDD[java.lang.Long, IntermediateVectorTile], outPath: String, _opts: BeastOptions): Unit =
    saveTilesCompact(tiles.rdd.map(f => (f._1.longValue(), f._2)), outPath, _opts)

  /**
   * Save all the given tiles to disk in a single compressed ZIP File. The ZIP file contains the following:
   *  - One file per tiles with the naming convention `tile-z-x-y.mvt`
   *  - An `index.html` file that visualizes those tiles on a map
   *  - A file `_visualization.properties` that stores the additional options for visualization.
   *  - A file `_tiles.hashtable` that stores the location of each tile in the ZIP file for quick access.
   *
   * @param tiles the set of tiles ot visualize
   * @param outPath the path of the output ZIP file
   * @param _opts additional options that were used for visualization.
   */
  def saveTilesCompact(tiles: RDD[(Long, IntermediateVectorTile)], outPath: String, _opts: BeastOptions): Unit = {
    val opts = new BeastOptions(tiles.context.hadoopConfiguration).mergeWith(_opts)
    // Write partial files to a temporary path first and then combine them into a single file with the desired name
    val tempPath = outPath + "_temp"
    // Writes tiles in each partition to a single ZIP file. Return the name of the ZIP file and the number of tiles
    val interimFiles: Array[(String, Int)] = tiles.sparkContext
      .runJob(tiles, (context: TaskContext, tiles: Iterator[(Long, IntermediateVectorTile)]) => {
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
        val zipOut: ZipOutputStream = new ZipOutputStream(outFS.create(localZipFile))
        val tempTileIndex: TileIndex = new TileIndex()
        val memTileOut = new ByteArrayOutputStream()
        var numTiles: Int = 0 // Keep track of number of tiles
        try {
          for (tid <- tiles) {
            numTiles += 1
            TileIndex.decode(tid._1, tempTileIndex)
            memTileOut.reset()
            tid._2.vectorTile.writeTo(memTileOut)
            memTileOut.close()
            // Write in ZIP file. Use no compression to enable efficient access form the ZIP file directly.
            ZipUtil.putStoredFile(zipOut,
              s"tile-${tempTileIndex.z}-${tempTileIndex.x}-${tempTileIndex.y}.mvt", memTileOut.toByteArray)
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
      val tileRegex = s"tile-(\\d+)-(\\d+)-(\\d+).mvt".r
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
    ZipUtil.mergeZip(outFS, mergedZIPFile, (interimFiles.map(x => new Path(x._1)) :+ masterZIPPath): _*)
    // Rename to the given output and add .zip to it
    val finalZIPPath = new Path(FileUtil.replaceExtension(outPath, ".zip"))
    outFS.rename(mergedZIPFile, finalZIPPath)
    outFS.delete(new Path(tempPath), true)
  }

  /**
   * A helper function parses a string into range. The string is in the following forms:
   * - 8: Produces the range [0, 8) exclusive of 8, i.e., [0,7]
   * - 3..6: Produces the range [3, 6], inclusive of both 3 and 4
   * - 4...7: Produces the range [4, 7), exclusive of the 7, i.e., [4, 6]
   *
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

  /**
   * Creates an `index.html`` file that can display the generated tiles and returns it as strong
   *
   * @param opts the visualization options
   * @return
   */
  def getIndexHTMLFile(opts: BeastOptions): String = {
    val resolution: Int = opts.getInt(Resolution, 256)
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
    val templateFile = "/mvt_plot.html"
    val templateFileReader = new LineReader(classOf[MultilevelPyramidPlotHelper].getResourceAsStream(templateFile))
    val htmlOut = new StringBuilder
    try {
      val line = new Text
      while ( {
        templateFileReader.readLine(line) > 0
      }) {
        var lineStr = line.toString
        lineStr = lineStr.replace("#{RESOLUTION}", Integer.toString(resolution))
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel))
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel))
        htmlOut.append(lineStr)
        htmlOut.append("\n")
      }
    } finally {
      templateFileReader.close()
    }
    htmlOut.toString()
  }

  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val levels: Range = parseRange(opts.getString(NumLevels, "7"))
    val resolution = opts.getInt(Resolution, 256)
    val tiles: RDD[(Long, IntermediateVectorTile)] = plotAllTiles(features, levels, resolution, 5, opts)
    val outPath = new Path(outputs(0))
    val outFS = outPath.getFileSystem(sc.hadoopConfiguration)
    if (outFS.exists(outPath) && opts.getBoolean(SpatialWriter.OverwriteOutput, false))
      outFS.delete(outPath, true)
    opts.set("data", FileUtil.relativize(new Path(inputs(0)), outPath))
    if (opts.getBoolean(CompactOutput, true))
      saveTilesCompact(tiles, outputs(0), opts)
    else
      saveTiles(tiles, outputs(0), opts)
  }
}
