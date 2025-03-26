package edu.ucr.cs.bdlab.beast.satex

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, BlockCartesianRDD, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.NumPartitions
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.{SpatialFileRDD, SpatialOutputFormat}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import edu.ucr.cs.bdlab.raptor.ZonalStatistics.RasterLayer
import edu.ucr.cs.bdlab.raptor.{IRasterReader, MemoryTile, RaptorJoin, RaptorJoinResult, RaptorJoinResultTile, RasterFileRDD}
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}
import org.apache.spark.util.LongAccumulator
import org.jetbrains.bio.npy.NpyFile
import org.locationtech.jts.geom.Envelope

import java.awt.Color
import java.awt.geom.{AffineTransform, Point2D}
import java.awt.image.BufferedImage
import java.io._
import java.util.zip.Deflater
import javax.imageio.ImageIO
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * Joins a raster image with a set of polygons and extracts a separate image for each polygon.
 * Each image is resized to a given fixed size, e.g., 256 x 256. This makes it helpful to use the extracted
 * images in machine learning algorithms which expect input images to be of the same size.
 */
@OperationMetadata(shortName = "satex",
  description = "Extracts images for a set of geometries from another set of geospatial images",
  inputArity = "2",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object ImageExtractor extends CLIOperation with Logging {

  @OperationParam(description = "The resolution of the output images in pixels", defaultValue = "256")
  val ImageResolution: String = "resolution"

  @OperationParam(description = "Keep the aspect ratio of the input geometry in output images", defaultValue = "true")
  val KeepAspectRatio: String = "keepratio"

  @OperationParam(
    description = "The attribute to use as file name. If not set, a unique arbitrary ID will be given to each geometry",
    required = false)
  val FileNameAttribute: String = "filenameattr"

  @OperationParam(description = "Enable this option to write images in ZIP archives to reduce number of files",
    defaultValue = "false")
  val ZipImages: String = "zipimages"

  @OperationParam(description = "The buffer size in pixels to take around non-polygonal geometries",
    defaultValue = "10")
  val BufferSize: String = "buffersize"

  @OperationParam(description = "Number of rounds to generate the images in", defaultValue = "1")
  val Rounds: String = "rounds"

  @OperationParam(description = "The grain size of the Raptor result: tile, pixel, or tileItr", defaultValue = "tile")
  val RaptorResult: String = "raptorresult"

  @OperationParam(description = "The format of the output images: png or npy", defaultValue = "png")
  val ImageFormat: String = "imageformat"

  override def printUsage(out: PrintStream): Unit = {
    out.println("Given a set of images (rasters) and a set of features (vectors), this method extracts an image")
    out.println("for each feature that represents the overlapping pixels in the raster data.")
    out.println("The two inputs are raster and vector.")
    out.println("The output directory will contain one image for each feature that overlaps at least one pixel.")
  }

  /**
   * Run the main function using the given user command-line options and spark context
   *
   * @param opts    user options for configuring the operation
   * @param inputs  inputs provided by the user
   * @param outputs outputs provided by the user
   * @param sc      the Spark context used to run the operation
   * @return an optional result of this operation
   */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val iLayer = opts.getString(RasterLayer, "0")
    opts.set(IRasterReader.RasterLayerID, iLayer)
    val rasters: RDD[ITile[Array[Int]]] = new RasterFileRDD[Array[Int]](sc, inputs(0), opts.retainIndex(0))

    val allFeatures = sc.spatialFile(inputs(1), opts.retainIndex(1))
      .filter(_.getGeometry != null)
    val featuresWithIDs: RDD[(Long, IFeature)] = allFeatures
      .zipWithUniqueId().map(x => (x._2, x._1))
    val rounds = opts.getInt(Rounds, 1)
    val partitioner: SpatialPartitioner = if (rounds > 1) {
      IndexHelper.createPartitioner(allFeatures, classOf[RSGrovePartitioner],
        NumPartitions(IndexHelper.Fixed, rounds), _ => 1, opts)
    } else {
      null
    }
    for (round <- 0 until rounds) {
      var features = if (partitioner != null)
        featuresWithIDs.filter(f => partitioner.overlapPartition(new EnvelopeNDLite().merge(f._2.getGeometry)) == round)
      else
        featuresWithIDs
      // Use the metadata of the first raster to determine the pixel size in world coordinates of the raster
      val rasterMetadata = rasters.first().rasterMetadata
      val pixelSizeInRaster: Double = rasterMetadata.getPixelScaleX
      val range = Array[Double](0, 0, pixelSizeInRaster, pixelSizeInRaster)
      val transform = Reprojector.findTransformationInfo(rasterMetadata.srid, features.first._2.getGeometry.getSRID)
      transform.mathTransform.transform(range, 0, range, 0, 2)
      val pixelSizeInVector: Double = (range(2) - range(0)).abs
      val bufferSize: Double = pixelSizeInVector * opts.getInt(BufferSize, 10)
      val fileNameAttribute: String = opts.getString(FileNameAttribute)
      val filenames: RDD[(Long, String)] = if (fileNameAttribute == null)
        null
      else
        features.map(x => (x._1, x._2.getAs(fileNameAttribute).toString))
      features = features.map(f => (f._1, f._2.getGeometry.getGeometryType match {
        case "Point" | "MultiPoint" | "LineString" | "MultiLineString" =>
          Feature.create(null, f._2.getGeometry.buffer(bufferSize))
        case _ => Feature.create(null, f._2.getGeometry)
      }))

      val numRanges: LongAccumulator = features.sparkContext.longAccumulator("num ranges")
      val numCrossTiff: LongAccumulator = features.sparkContext.longAccumulator("numCrossTiff")
      val numTooManyTiles: LongAccumulator = features.sparkContext.longAccumulator("numTooManyTiles")
      val numTooLessPixels: LongAccumulator = features.sparkContext.longAccumulator("numTooLessPixels")
      val compMethod: String = opts.getString(RaptorResult, "tile")
      val images: RDD[(Long, ITile[Array[Int]])] = compMethod match {
        case "tile" => extractImagesTile(features, rasters, opts, numRanges)
        case "pixel" => extractImagesPixel(features, rasters, opts, numRanges)
        case "tileItr" => extractImagesTileIterate(features, rasters, opts, numRanges,
          numCrossTiff, numTooManyTiles, numTooLessPixels)
      }
      val imageFormat = opts.getString(ImageFormat, "png")
      val imageData: RDD[(Long, Array[Byte])] = encodeImages(images, imageFormat)
      val outputDir: String = outputs(0)
      val imagesWithNames: RDD[(String, Array[Byte])] = if (filenames == null)
        imageData.map(x => (x._1.toString, x._2))
      else
        filenames.join(imageData).map(x => x._2)
      val imgExtension = imageFormat match {
        case "png" => ".png"
        case "npy" => ".npy"
      }
      val hadoopConfCopy: BeastOptions = new BeastOptions(opts.loadIntoHadoopConf(sc.hadoopConfiguration))
      if (opts.getBoolean(ZipImages, false)) {
        // Combine images in archives to reduce number of files
        imagesWithNames.foreachPartition(images => {
          if (images.hasNext) {
            // TODO get partition number to avoid creating random file names
            val outPath = new Path(outputDir)
            val fileSystem = outPath.getFileSystem(hadoopConfCopy.loadIntoHadoopConf(null))
            var outputFile: OutputStream = null
            var attempts: Int = 0
            do {
              try {
                outputFile = fileSystem.create(new Path(outPath, (Math.random() * 1000000000).toInt + ".zip"))
              } catch {
                case _: IOException => attempts += 1
              }
            } while (outputFile == null && attempts < 50)
            if (outputFile == null)
              throw new RuntimeException(s"Failed to create an output directory after $attempts attempts")
            val zipOutput: ZipArchiveOutputStream = new ZipArchiveOutputStream(outputFile)
            // Disable compression since PNG images are already compressed
            zipOutput.setLevel(Deflater.NO_COMPRESSION)
            images.foreach(f => {
              // Write the image to the output
              val entry = new ZipArchiveEntry(f._1 + imgExtension)
              zipOutput.putArchiveEntry(entry)
              zipOutput.write(f._2)
              zipOutput.closeArchiveEntry()
            })
            zipOutput.close()
          }
        })
      } else {
        // Write each image as a single file
        imagesWithNames.foreach(x => {
          // Write the image to the output
          val imagePath = new Path(outputDir, x._1 + imgExtension)
          val filesystem = imagePath.getFileSystem(new Configuration())
          val out = filesystem.create(imagePath)
          out.write(x._2)
          out.close()
        })
      }
      logInfo(s"Processed a total of ${numRanges.value} ranges")
      logInfo(s"Processed a total of ${numCrossTiff.value} CrossTiff geometries," +
        s"${numTooManyTiles.value} TooManyTiles geometries and ${numTooLessPixels.value} TooLessPixels geometries")
    }
  }

  def encodeImages[T](images: RDD[(Long, ITile[T])], format: String): RDD[(Long, Array[Byte])] = {
    format match {
      case "png" =>
        images.mapValues(tile => {
          val image = new BufferedImage(tile.tileWidth, tile.tileHeight, BufferedImage.TYPE_INT_ARGB)
          for ((x, y, m) <- tile.pixels) {
            val color = m match {
              case p: Array[Int] =>
                new Color(p(0), p(1), p(2)).getRGB
            }
            image.setRGB(x, y, color)
          }
          val baos = new ByteArrayOutputStream()
          ImageIO.write(image, "png", baos)
          baos.close()
          baos.toByteArray
        })
      case "npy" =>
        images.mapValues(tile => tileToNpyBytes(tile))
    }
  }

  /**
   * Extracts images for the given set of vector and raster data.
   *
   * @param features a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImagesPixel[T](features: RDD[(Long, IFeature)], rasters: RasterRDD[T], opts: BeastOptions,
                            numRanges: LongAccumulator = null)(implicit t: ClassTag[T]): RDD[(Long, ITile[T])] = {
    // 1- Perform a raptor join between the raster and vector data
    val joinResults: RDD[RaptorJoinResult[T]] =
      RaptorJoin.raptorJoinIDFull(rasters, features, new BeastOptions(), numRanges = numRanges)

    // 2- Get polygon minimum bounding rectangles (MBRs) in the raster coordinate reference system (CRS)
    val vectorSRID = features.first._2.getGeometry.getSRID

    val geomMBRs: RDD[(Long, Envelope)] = features.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))

    // 3- Map input values to pixel values in the image space
    // 3.1- Convert each pixel to a square in the world space
    val geometryPixelsInWorld: RDD[(Long, (Float, Float, Float, Float, T))] = joinResults.map(result => {
      // For each pixel, keep its geographic location and color
      val point2D1 = new Point2D.Double()
      result.rasterMetadata.gridToModel(result.x, result.y, point2D1)
      val point2D2 = new Point2D.Double()
      result.rasterMetadata.gridToModel(result.x + 1.0, result.y + 1.0, point2D2)
      //val color = new Color(result.m(0), result.m(1), result.m(2)).getRGB
      val pixelValue = result.m
      if (result.rasterMetadata.srid != vectorSRID) {
        val transformInfo = Reprojector.findTransformationInfo(result.rasterMetadata.srid, vectorSRID)
        val points: Array[Float] = Array[Float](point2D1.x.toFloat, point2D1.y.toFloat,
          point2D2.x.toFloat, point2D2.y.toFloat)
        transformInfo.mathTransform.transform(points, 0, points, 0, 2)
        (result.featureID, (points(0), points(1), points(2), points(3), pixelValue))
      } else
        (result.featureID, (point2D1.x.toFloat, point2D1.y.toFloat, point2D2.x.toFloat, point2D2.y.toFloat, pixelValue))
    })

    // 3.2- Join with geometry MBRs to convert the square into the image space in pixels
    val outputResolution: Int = opts.getInt(ImageResolution, 256)
    val keepAspectRatio: Boolean = opts.getBoolean(KeepAspectRatio, true)
    val pixelProcessor: (Iterator[(Long, Envelope)], Iterator[(Long, (Float, Float, Float, Float, T))]) => Iterator[(Long, (Int, Int, Int, Int, T))] = {
      case (someGeometryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, (Float, Float, Float, Float, T))]) =>
        val geometryMBRsLocal: Map[Long, Envelope] = someGeometryMBRs.toMap
        pixels.map({ case (featureID: Long, (x1: Float, y1: Float, x2: Float, y2: Float, pixelValue: T)) =>
          val mbro = geometryMBRsLocal.get(featureID)
          if (mbro.isEmpty)
            null
          else {
            val mbr = mbro.get
            var xRatio = outputResolution / mbr.getWidth
            var yRatio = outputResolution / mbr.getHeight
            if (keepAspectRatio) {
              xRatio = xRatio min yRatio
              yRatio = xRatio
            }
            val x1P: Int = ((x1 - mbr.getMinX) * xRatio).toInt max 0
            val x2P: Int = ((x2 - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
            val y1P: Int = (outputResolution - 1 - ((y1 - mbr.getMinY) * yRatio)).toInt max 0
            val y2P: Int = (outputResolution - 1 - ((y2 - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)

            (featureID, (x1P, y1P, x2P, y2P, pixelValue))
          }
        }).filterNot(x => x == null || x._2._1 == x._2._3 || x._2._2 == x._2._4)
    }
    // Run hash join if the combined number of partitions is too large
    val hashJoin = geomMBRs.getNumPartitions * geometryPixelsInWorld.getNumPartitions > 2 * features.sparkContext.defaultParallelism
    logDebug(s"Using ${if (hashJoin) "hash join" else "BNLJ"}")
    val countryPixelsInImage: RDD[(Long, (Int, Int, Int, Int, T))] =
      if (!hashJoin) {
        // Apply block nested loop join
        val combinedPartitions: RDD[(Iterator[(Long, Envelope)], Iterator[(Long, (Float, Float, Float, Float, T))])] =
          new BlockCartesianRDD(features.sparkContext, geomMBRs, geometryPixelsInWorld)
        combinedPartitions.flatMap({
          case (someCountryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, (Float, Float, Float, Float, T))]) =>
            pixelProcessor(someCountryMBRs, pixels)
        })
      } else {
        // Apply hash join
        val hashPartitioner = new HashPartitioner(geomMBRs.getNumPartitions max geometryPixelsInWorld.getNumPartitions max geomMBRs.sparkContext.defaultParallelism)
        val partitionedGeomMBRs = geomMBRs.partitionBy(hashPartitioner)
        val partitionedPixels = geometryPixelsInWorld.partitionBy(hashPartitioner)

        partitionedGeomMBRs.zipPartitions(partitionedPixels, true)(pixelProcessor)
      }

    // 5- Arrange pixels into an array that resembles the output image and create the image data
    val emptyTile: MemoryTile[T] = new MemoryTile[T](0,
      new RasterMetadata(0, 0, outputResolution, outputResolution,
        outputResolution, outputResolution, 0, new AffineTransform()))

    val countryImages: RDD[(Long, ITile[T])] = countryPixelsInImage.aggregateByKey(emptyTile,
      new HashPartitioner(countryPixelsInImage.sparkContext.defaultParallelism))((tile, pixel) => {
      for (x <- pixel._1 to pixel._3; y <- pixel._2 to pixel._4) {
        tile.setPixelValue(x, y, pixel._5)
      }
      tile
    }, (tile1, tile2) => {
      for ((x, y, t) <- tile2.pixels)
        tile1.setPixelValue(x, y, t)
      tile1
    }).mapPartitions(_.map({ case (id: Long, tile: ITile[T]) =>
      // Combine pixels into an image using Java imaging API
      (id, tile.asInstanceOf[ITile[T]])
    }), preservesPartitioning = true)

    countryImages
  }

  /**
   * Extracts images for the given set of vector and raster data.
   *
   * @param features a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImagesTile[T](features: RDD[(Long, IFeature)], rasters: RasterRDD[T], opts: BeastOptions,
                        numRanges: LongAccumulator = null)(implicit t: ClassTag[T]): RDD[(Long, ITile[T])] = {
    // 1- Perform a raptor join between the raster and vector data
    val joinResults: RDD[RaptorJoinResultTile[T]] =
      RaptorJoin.raptorJoinIDFullTile(rasters, features, new BeastOptions(), numRanges = numRanges)

    extractImagesWJoinResults(joinResults, features, opts)
  }

  def extractImagesWJoinResults[T](joinResults: RDD[RaptorJoinResultTile[T]], features: RDD[(Long, IFeature)],
                                opts: BeastOptions)(implicit t: ClassTag[T]): RDD[(Long, ITile[T])] = {
    if (features.isEmpty()) return features.sparkContext.emptyRDD
    // 2- Get polygon minimum bounding rectangles (MBRs) in the raster coordinate reference system (CRS)
    val vectorSRID = features.first._2.getGeometry.getSRID

    val geomMBRs: RDD[(Long, Envelope)] = features.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))

    val geometryTile: RDD[(Long, ITile[T])] = joinResults.map(result => (result.featureID, result.tile))

    // 3- Map input values to pixel values in the image space
    // 3.1- Convert each pixel to a square in the world space
    // 3.2- Join with geometry MBRs to convert the square into the image space in pixels
    val outputResolution: Int = opts.getInt(ImageResolution, 256)
    val keepAspectRatio: Boolean = opts.getBoolean(KeepAspectRatio, true)
    val tileProcessor: (Iterator[(Long, Envelope)], Iterator[(Long, ITile[T])]) =>
      Iterator[(Long, Array[(Int, Int, Int, Int, T)])] = {
      case (someGeometryMBRs: Iterator[(Long, Envelope)], tiles: Iterator[(Long, ITile[T])]) =>
        val geometryMBRsLocal: Map[Long, Envelope] = someGeometryMBRs.toMap
        tiles.map({ case (featureID: Long, tile: ITile[T]) =>
          val mbro = geometryMBRsLocal.get(featureID)
          if (mbro == null || mbro.isEmpty)
            null
          else {
            val mbr: Envelope = mbro.get
            var xRatio = outputResolution / mbr.getWidth
            var yRatio = outputResolution / mbr.getHeight
            if (keepAspectRatio) {
              xRatio = xRatio min yRatio
              yRatio = xRatio
            }
            val res: ArrayBuffer[(Int, Int, Int, Int, T)] = new ArrayBuffer[(Int, Int, Int, Int, T)]()

            for (x <- tile.x1 to tile.x2; y <- tile.y1 to tile.y2) {
              // For each pixel, keep its geographic location and color
              if (!tile.isEmpty(x, y)) {
                val point2D1 = new Point2D.Double()
                tile.rasterMetadata.gridToModel(x, y, point2D1)
                val point2D2 = new Point2D.Double()
                tile.rasterMetadata.gridToModel(x + 1.0, y + 1.0, point2D2)
                val pixelValue: T = tile.getPixelValue(x, y)
                //val color = new Color(pixelValue(0), pixelValue(1), pixelValue(2)).getRGB
                val points: Array[Float] = Array[Float](point2D1.x.toFloat, point2D1.y.toFloat,
                  point2D2.x.toFloat, point2D2.y.toFloat)
                if (tile.rasterMetadata.srid != vectorSRID) {
                  val transformInfo = Reprojector.findTransformationInfo(tile.rasterMetadata.srid, vectorSRID)
                  transformInfo.mathTransform.transform(points, 0, points, 0, 2)
                }

                val x1P: Int = ((points(0) - mbr.getMinX) * xRatio).toInt max 0
                val x2P: Int = ((points(2) - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
                val y1P: Int = (outputResolution - 1 - ((points(1) - mbr.getMinY) * yRatio)).toInt max 0
                val y2P: Int = (outputResolution - 1 - ((points(3) - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)
                if (x1P != x2P && y1P != y2P) {
                  res.+=((x1P, y1P, x2P, y2P, pixelValue))
                }
              }
            }

            (featureID, res.toArray)
          }
        }).filterNot(x => x == null)
    }
    // Run hash join if the combined number of partitions is too large
    val hashJoin = geomMBRs.getNumPartitions * geometryTile.getNumPartitions > 2 * features.sparkContext.defaultParallelism
    logDebug(s"Using ${if (hashJoin) "hash join" else "BNLJ"}")
    val countryPixelsInImage: RDD[(Long, Array[(Int, Int, Int, Int, T)])] =
      if (!hashJoin) {
        // Apply block nested loop join
        val combinedPartitions: RDD[(Iterator[(Long, Envelope)], Iterator[(Long, ITile[T])])] =
          new BlockCartesianRDD(features.sparkContext, geomMBRs, geometryTile)
        combinedPartitions.flatMap({
          case (someCountryMBRs: Iterator[(Long, Envelope)], pixels: Iterator[(Long, ITile[T])]) =>
            tileProcessor(someCountryMBRs, pixels)
        })
      } else {
        // Apply hash join
        val hashPartitioner = new HashPartitioner(geomMBRs.getNumPartitions max geometryTile.getNumPartitions max geomMBRs.sparkContext.defaultParallelism)
        val partitionedGeomMBRs = geomMBRs.partitionBy(hashPartitioner)
        val partitionedPixels = geometryTile.partitionBy(hashPartitioner)

        partitionedGeomMBRs.zipPartitions(partitionedPixels, true)(tileProcessor)
      }

    // 5- Arrange pixels into an array that resembles the output image and create the image data
    val emptyTile: MemoryTile[T] = new MemoryTile[T](0,
      new RasterMetadata(0, 0, outputResolution, outputResolution,
        outputResolution, outputResolution, 0, new AffineTransform()))

    val countryImages: RDD[(Long, ITile[T])] = countryPixelsInImage.aggregateByKey(emptyTile,
      new HashPartitioner(countryPixelsInImage.sparkContext.defaultParallelism))((tile, pixelArray) => {
      for (i <- pixelArray) {
        for (x <- i._1 to i._3; y <- i._2 to i._4) {
          tile.setPixelValue(x, y, i._5)
        }
      }
      tile
    }, (tile1, tile2) => {
      for ((x, y, t) <- tile2.pixels)
        tile1.setPixelValue(x, y, t)
      tile1
    }).mapPartitions(_.map({ case (id: Long, tile: ITile[T]) =>
      (id, tile.asInstanceOf[ITile[T]])
    }), preservesPartitioning = true)

    countryImages
  }

  /**
   * Extracts images for the given set of vector and raster data using repartitionAndSort and ImageIterator.
   *
   * @param features a set of (id, geometry) pairs to extract images for
   * @param rasters    an RDD of raster images. It is expected that the pixel value is either three bands (RGB) or
   *                   four bands (RGBA)
   * @param opts       additional options to configure the image extraction process
   * @return an RDD of (id, image) pairs. Since [[BufferedImage]] is not serializable, the bytes represent a PNG image
   *         that is already serialized. This allows the return value to be joined back with features if needed.
   */
  def extractImagesTileIterate[T](features: RDD[(Long, IFeature)], rasters: RDD[ITile[T]], opts: BeastOptions,
                               numRanges: LongAccumulator = null,
                               numCrossTiff: LongAccumulator,
                               numTooManyTiles: LongAccumulator,
                               numTooLessPixels: LongAccumulator)(implicit t: ClassTag[T]): RDD[(Long, ITile[T])] = {
    val joinResults: RDD[RaptorJoinResultTile[T]] =
      RaptorJoin.raptorJoinIDFullTile(rasters, features, new BeastOptions(), numRanges = numRanges)

    val images: RDD[(Long, ITile[T])] = joinResults.map(x => (x.featureID, x.tile))
      .repartitionAndSortWithinPartitions(new HashPartitioner(joinResults.getNumPartitions))
      .mapPartitions(iter => new ImageIterator(iter, numCrossTiff, numTooManyTiles, numTooLessPixels))
      .persist()

    extractImagesWJoinResults(joinResults, features.join(images.filter(x => x._2 == null)).map(x => (x._1, x._2._1)), opts)
      .union(images.filter(x => x._2 != null))
  }

  def tileToNpyBytes[T](tile: ITile[T]): Array[Byte] = {
    // Determine the size of the tile and allocate an array to hold its data
    val shape = if (tile.numComponents == 1) {
      Array[Int](tile.tileWidth, tile.tileHeight)
    } else {
      Array[Int](tile.tileWidth, tile.tileHeight, tile.numComponents)
    }
    val npyFile: File = java.io.File.createTempFile("tile-data", "npy")
    val size = shape.product
    tile.componentType match {
      case IntegerType if tile.numComponents == 1=>
        val data = new Array[Int](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          data(index) = value.asInstanceOf[Int]
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
      case IntegerType=>
        val data = new Array[Int](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          val values = value.asInstanceOf[Array[Int]]
          System.arraycopy(values, 0, data, index, values.length)
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
      case FloatType if tile.numComponents == 1=>
        val data = new Array[Float](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          data(index) = value.asInstanceOf[Float]
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
      case FloatType=>
        val data = new Array[Float](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          val values = value.asInstanceOf[Array[Float]]
          System.arraycopy(values, 0, data, index, values.length)
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
      case DoubleType if tile.numComponents == 1=>
        val data = new Array[Double](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          data(index) = value.asInstanceOf[Double]
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
      case DoubleType=>
        val data = new Array[Double](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          val values = value.asInstanceOf[Array[Double]]
          System.arraycopy(values, 0, data, index, values.length)
        }
        NpyFile.write(npyFile.toPath, data, shape, java.nio.ByteOrder.nativeOrder())
    }
    // Read the file back and return as a byte array
    val fileLength = npyFile.length().toInt
    val npyData = new Array[Byte](fileLength)
    val in = new BufferedInputStream(new FileInputStream(npyFile))
    var readSize = 0
    while (readSize < fileLength) {
      val numBytes = in.read(npyData, readSize, fileLength - readSize)
      if (numBytes > 0)
        readSize += numBytes
    }
    in.close()
    npyFile.delete()
    npyData
  }

}
