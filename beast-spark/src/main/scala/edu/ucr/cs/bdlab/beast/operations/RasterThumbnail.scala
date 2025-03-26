package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.beast.util.{FileListRDD, OperationMetadata}
import edu.ucr.cs.bdlab.raptor.{GeoTiffReader, GeoTiffWriter, RasterOperationsFocal}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform, IdentityTransform}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory}

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.xml._
import scala.xml.dtd.{DocType, SystemID}

@OperationMetadata(shortName = "raster-thumbnail",
  description = "Creates a KMZ thumbnail of a directory that contains multiple raster files",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD]))
object RasterThumbnail extends CLIOperation with Logging{

  override def addDependentClasses(opts: BeastOptions, classes: java.util.Stack[Class[_]]): Unit = {
    super.addDependentClasses(opts, classes)
    classes.add(classOf[GeoTiffReader[Int]])
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
    val inputDir = new Path(inputs(0))
    val outPath = new Path(outputs(0))
    val t0 = System.nanoTime()
    val inputFiles: RDD[String] = FileListRDD.listFiles(sc, inputs(0), "*.tif", false)
    logInfo(s"Took ${(System.nanoTime() - t0)*1E-9} to create ${inputFiles.getNumPartitions} partitions")
    val hadoopConfCopy = new BeastOptions(sc.hadoopConfiguration)
    // 1- Create a geometry overview of files
    val mbrSummary = Summary.createSummaryAccumulator(sc)
    val fileGeometries: RDD[IFeature] = inputFiles.map(inputFile => {
      val t0: Long = System.nanoTime()
      val inFS = inputDir.getFileSystem(hadoopConfCopy.loadIntoHadoopConf(null))
      try {
        val metadata = GeoTiffReader.getRasterMetadata(inFS, new Path(inputFile))
        logInfo(s"Took ${(System.nanoTime()-t0)*1E-9} seconds to get metadata from file '$inputFile'")
        val corners = Array[Double](
          metadata.x1, metadata.y1,
          metadata.x2, metadata.y1,
          metadata.x2, metadata.y2,
          metadata.x1, metadata.y2
        )
        val transformer = if (metadata.srid == 4326) {
          new AffineTransform2D(metadata.g2m)
        } else {
          val t = Reprojector.findTransformationInfo(metadata.srid, 4326)
          ConcatenatedTransform.create(new AffineTransform2D(metadata.g2m), t.mathTransform)
        }
        transformer.transform(corners, 0, corners, 0, 4)
        val geometry = new GeometryFactory().createPolygon(Array(
          new Coordinate(corners(0), corners(1)),
          new Coordinate(corners(2), corners(3)),
          new Coordinate(corners(4), corners(5)),
          new Coordinate(corners(6), corners(7)),
          new Coordinate(corners(0), corners(1)),
        ))
        geometry.setSRID(4326)
        val fileStatus = inFS.getFileStatus(new Path(inputFile))
        val f = Feature.create(geometry, Array("FileName", "FileSize", "Width", "Height", "SRID"),
          null,
          Array(fileStatus.getPath.getName, fileStatus.getLen, metadata.rasterWidth, metadata.rasterHeight, metadata.srid))
        mbrSummary.add(f)
        f
      } finally {
        logInfo(s"Took ${(System.nanoTime() - t0)*1E-9} seconds to get metadata of file '$inputFile'")
      }
    })
    val filelistPath = new Path(outPath, "filelist.zip")
    fileGeometries.coalesce(1, true)
      .writeSpatialFile(filelistPath.toString, "zipshapefile")
    // Move the Shapefile to the output directory and delete the subdirectory
    val dirName = new Path(inputs(0)).getName
    val outputFS = filelistPath.getFileSystem(sc.hadoopConfiguration)
    outputFS.listStatus(filelistPath).filter(_.getPath.getName.toLowerCase().endsWith(".zip"))
      .foreach(p => outputFS.rename(p.getPath, new Path(outPath, s"_${dirName}_filelist.zip")))
    outputFS.delete(filelistPath, true)

    // 2- Create a raster thumbnail
    import edu.ucr.cs.bdlab.beast._
    // TODO handle files with any pixel type and handle other file types, e.g., HDF
    // Pixel size in EPSG:4326 to get roughly 1KM resolution
    val pixelSize: Double = 0.0089285714
    val rasters = sc.geoTiff[Array[Int]](inputs(0))
    val numBands = rasters.first().numComponents
    val summary = mbrSummary.value
    val rescaled = RasterOperationsFocal.reshapeNN(rasters, (_: RasterMetadata) => RasterMetadata.create(
      summary.getMinCoord(0), summary.getMaxCoord(1), summary.getMaxCoord(0), summary.getMinCoord(1), 4326,
      (summary.getSideLength(0) / pixelSize).toInt, (summary.getSideLength(1) / pixelSize).toInt, 128, 128
    ), sc.defaultParallelism)
    val thumbnailPath = new Path(new Path(outputs(0)), "thumbnail.tif")
    val bitsPerSample = Array.fill[Int](numBands)(16)
    rescaled.saveAsGeoTiff(thumbnailPath.toString, Seq(GeoTiffWriter.BitsPerSample -> bitsPerSample.mkString(","),
      GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.NoEmptyTiles -> true, GeoTiffWriter.FillValue -> Int.MaxValue))
    // Move the GeoTIFF to the output directory and delete the subdirectory
    outputFS.listStatus(thumbnailPath).filter(_.getPath.getName.toLowerCase().endsWith(".tif"))
      .foreach(p => outputFS.rename(p.getPath, new Path(outPath, s"_${dirName}_thumbnail.tif")))
    outputFS.delete(thumbnailPath, true)
  }

}
