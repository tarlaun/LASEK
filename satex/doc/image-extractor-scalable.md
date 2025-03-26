# Scalable Image Extraction

This page describes a scalable algorithm for the image extraction problem described [earlier](image-extractor-resize.md). This page introduces several improvements that ensures that this algorithm will work for extremely large datasets and for special cases that are not handled in the other simpler algorithm.

## Mismatching CRS
It is common to have vector and raster datasets that do not have the same coordinate reference system (CRS). While the core RaptorJoin operator automatically adjusts the CRS, the image generation part does not. To address this issue, the code below converts the feature MBRs to the raster space to get the correct result.

```scala
val vectorSRID = countries.first._2.getGeometry.getSRID
val rasterSRID = elevation.getSRID

val countryMBRs: RDD[(Long, Envelope)] = if (vectorSRID == rasterSRID)
  countries.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))
else {
  val sparkConf: SparkConf = sc.getConf
  countries.mapPartitions(x => {
    val transform = Reprojector.findTransformationInfo(vectorSRID, rasterSRID, sparkConf)
    x.map(v => {
      val reprojectedGeom = Reprojector.reprojectGeometry(v._2.getGeometry, transform)
      (v._1, reprojectedGeom.getEnvelopeInternal)
    })
  })
}
```

## Large Vector Size
The previous example builds an in-memory hashtable that holds feature MBRs and country names. While that hashtable is more efficient for small data, it will soon fail due to its large size. You might get an error similar to the following.
```
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 333 tasks (1025.6 MiB) is bigger than spark.driver.maxResultSize (1024.0 MiB)
```
To solve this issue, we keep country MBRs and names in RDDs and join them when needed instead of looking them up from the hashtable.

```scala
// 4.2- Join with country MBRs to convert the square into the image space
val countryPixelsInImage: RDD[(Long, (Int, Int, Int, Int, Int))] = countryPixelsInWorld.join(countryMBRs)
  .map({case (featureID: Long, ((x1: Float, y1: Float, x2: Float, y2: Float, color: Int), mbr: Envelope)) =>
    var xRatio = outputResolution / mbr.getWidth
    var yRatio = outputResolution / mbr.getHeight
    if (keepAspectRatio) {
      xRatio = xRatio min yRatio
      yRatio = xRatio
    }
    val x1P: Int = ((x1 - mbr.getMinX) * xRatio).toInt max 0
    val y1P: Int = (outputResolution - 1 - ((y1 - mbr.getMinY) * yRatio)).toInt max 0
    val x2P: Int = ((x2 - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
    val y2P: Int = (outputResolution - 1 - ((y2 - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)
    (featureID, (x1P, y1P, x2P, y2P, color))
  })
```

```scala
// 7- Write the final image output
countryImages.join(countryNames) // Join image data with country name to write with the file name
  .foreach({ case(id: Long, (image: Array[Byte], name: String)) =>
  // Write the image to the output
  val imagePath = new Path("output-images", name + ".png")
  val filesystem = imagePath.getFileSystem(new Configuration())
  val out = filesystem.create(imagePath)
  out.write(image)
  out.close()
})
```

## Speedup Equi-joins
As this code use equi-joins on polygon ID in two different locations, we want these joins to run as fast as possible.
Spark require joined RDDs to be partitioned on the join key. By default, the `map` transformation does not preserve
the RDD partitioner. In our case, we want to preserve the hash partitioning on feature ID as much as possible to avoid
unnecessary repartitioning. We do that by using `mapPartitions` which, unlike `map`, can preserve partitioning.

For example, instead of:
```scala
joinResults.map(x => { /* Map transformation */ })
```

We write:
```scala
joinResults.mapPartitions(_.map(x => { /* Map transformation */ }), preservesPartitioning = true)
```

## Complete Code
Below is the complete code in Scala.
```scala
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.raptor.{RaptorJoin, RaptorJoinResult}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Envelope

import java.awt.Color
import java.awt.geom.Point2D
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

/**
 * Joins a raster image with a set of polygons and extracts a separate image for each polygon.
 * Each image is resized to a given fixed size, e.g., 256 x 256. This makes it helpful to use the extracted
 * images in machine learning algorithms which expect input images to be of the same size.
 */
object RaptorImageExtractorScalable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scalable Image Extractor")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local")

    val outputResolution: Int = 256
    val keepAspectRatio: Boolean = true
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    try {
      val t1: Long = System.nanoTime()
      // 1- Load the input data
      val countries: RDD[(Long, IFeature)] = sc.shapefile("ne_10m_admin_0_countries.zip")
        .zipWithUniqueId()
        .map(f => (f._2, f._1))
      val elevation = sc.geoTiff("HYP_HR_SR.tif")

      // 2- Perform a raptor join between the raster and vector data
      val joinResults: RDD[RaptorJoinResult[Array[Int]]] =
        RaptorJoin.raptorJoinIDFull(elevation, countries, new BeastOptions())

      // 3- Get polygon minimum bounding rectangles (MBRs) in the raster coordinate reference system (CRS)
      val vectorSRID = countries.first._2.getGeometry.getSRID
      val rasterSRID = elevation.getSRID

      val countryMBRs: RDD[(Long, Envelope)] = if (vectorSRID == rasterSRID)
        countries.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))
      else {
        val sparkConf: SparkConf = sc.getConf
        countries.mapPartitions(x => {
          val transform = Reprojector.findTransformationInfo(vectorSRID, rasterSRID, sparkConf)
          x.map(v => {
            val reprojectedGeom = Reprojector.reprojectGeometry(v._2.getGeometry, transform)
            (v._1, reprojectedGeom.getEnvelopeInternal)
          })
        }, preservesPartitioning = true)
      }

      // 4- Map input values to pixel values in the image space
      // 4.1- Convert each pixel to a square in the world space
      val countryPixelsInWorld: RDD[(Long, (Float, Float, Float, Float, Int))] = joinResults.mapPartitions(_.map(result => {
        // For each pixel, keep its geographic location and color
        val point2D1 = new Point2D.Double()
        result.rasterMetadata.gridToModel(result.x, result.y, point2D1)
        val point2D2 = new Point2D.Double()
        val color = new Color(result.m(0), result.m(1), result.m(2)).getRGB
        (result.featureID, (point2D1.x.toFloat, point2D1.y.toFloat, point2D2.x.toFloat, point2D2.y.toFloat, color))
      }), preservesPartitioning = true)

      // 4.2- Join with country MBRs to convert the square into the image space
      val countryPixelsInImage: RDD[(Long, (Int, Int, Int, Int, Int))] = countryPixelsInWorld.join(countryMBRs)
        .map({case (featureID: Long, ((x1: Float, y1: Float, x2: Float, y2: Float, color: Int), mbr: Envelope)) =>
          var xRatio = outputResolution / mbr.getWidth
          var yRatio = outputResolution / mbr.getHeight
          if (keepAspectRatio) {
            xRatio = xRatio min yRatio
            yRatio = xRatio
          }
          val x1P: Int = ((x1 - mbr.getMinX) * xRatio).toInt max 0
          val y1P: Int = (outputResolution - 1 - ((y1 - mbr.getMinY) * yRatio)).toInt max 0
          val x2P: Int = ((x2 - mbr.getMinX) * xRatio).toInt min (outputResolution - 1)
          val y2P: Int = (outputResolution - 1 - ((y2 - mbr.getMinY) * yRatio)).toInt min (outputResolution - 1)

          (featureID, (x1P, y1P, x2P, y2P, color))
        })

      // 5- Arrange pixels into an array that resembles the output image and create the image data
      val emptyPixels: Array[Int] = new Array[Int](outputResolution * outputResolution)

      val countryImages: RDD[(Long, Array[Byte])] = countryPixelsInImage.aggregateByKey(emptyPixels)((pixels, pixel) => {
        for (x <- pixel._1 until pixel._3; y <- pixel._2 until pixel._4) {
          val offset = x * outputResolution + y
          pixels(offset) = pixel._5
        }
        pixels
      }, (pixels1, pixels2) => {
        for (i <- pixels1.indices; if pixels1(i) == 0)
          pixels1(i) = pixels2(i)
        pixels1
      }).mapPartitions(_.map({case (id: Long, pixels: Array[Int]) =>
        // Combine pixels into an image using Java imaging API
        val image = new BufferedImage(outputResolution, outputResolution, BufferedImage.TYPE_INT_ARGB)
        for (x <- 0 until outputResolution; y <- 0 until outputResolution) {
          val offset = y * outputResolution + x
          image.setRGB(x, y, pixels(offset))
        }
        // Convert it to PNG format
        val baos = new ByteArrayOutputStream()
        ImageIO.write(image, "png", baos)
        baos.close()
        (id, baos.toByteArray)
      }), preservesPartitioning = true)

      // 6- Join back with country names to get the desired file name for each image
      val countryNames: RDD[(Long, String)] =
        countries.map(c => (c._1, c._2.getAs[String]("NAME")))

      // 7- Write the final image output
      countryImages.join(countryNames) // Join image data with country name to write with the file name
        .foreach({ case(id: Long, (image: Array[Byte], name: String)) =>
          // Write the image to the output
          val imagePath = new Path("output-images", name + ".png")
          val filesystem = imagePath.getFileSystem(new Configuration())
          val out = filesystem.create(imagePath)
          out.write(image)
          out.close()
        })
      println(s"Total time is ${(System.nanoTime() - t1)*1E-9} seconds")
    } finally {
      spark.stop()
    }
  }
}
```