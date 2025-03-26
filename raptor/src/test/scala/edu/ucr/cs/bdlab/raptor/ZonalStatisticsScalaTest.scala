package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, ITile}
import edu.ucr.cs.bdlab.beast.io.SpatialReader
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureReader
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZonalStatisticsScalaTest extends AnyFunSuite with ScalaSparkTest {
  val expectedCounts: Array[Int] = Array(17, 25, 15, 2, 14, 13, 17, 27, 15, 17, 25, 17, 11, 20, 8, 8, 12, 12, 16, 14,
    10, 7, 39, 1, 3, 1, 0, 2, 9, 9, 10, 7, 5, 9, 8, 6, 7, 8, 5, 8, 14, 3, 0, 0, 2, 0, 10, 8, 6, 17, 178)

  val expectedSums: Array[Int] = Array(203, 260, 210, 40, 119, 113, 169, 229, 133, 180, 228, 164, 126, 201, 90, 128,
    164, 168, 228, 147, 124, 40, 461, 2, 24, 6, 0, 4, 30, 105, 95, 57, 58, 144, 132, 54, 82, 118, 60, 32, 96, 10, 0, 0,
    4, 0, 66, 58, 28, 246, 2326)

  test("Zonal statistics with raster file with RDD") {
    val vectorFile = locateResource("/vectors/ne_110m_admin_1_states_provinces.zip")
    val rasterFile = locateResource("/rasters/glc2000_small.tif")

    val polygons: RDD[IFeature] = SpatialReader.readInput(sparkContext,new BeastOptions(), vectorFile.getPath, "shapefile")
      .zipWithIndex()
      .map(featureIndex => new Feature((Row.unapplySeq(featureIndex._1).get :+ featureIndex._2.toInt).toArray,
        StructType(featureIndex._1.schema :+ StructField("index", IntegerType))))

    val raster: RDD[ITile[Int]] = new RasterFileRDD[Int](sparkContext, rasterFile.getPath, new BeastOptions())

    val zsResults: Array[Collector] =
      ZonalStatistics.zonalStats2(polygons, raster, classOf[Statistics], new BeastOptions())
        .map(fc => (fc._1.getAs[Int]("index"), fc._2))
        .collect
        .sortBy(_._1)
        .map(_._2)

    val actualCounts: Array[Int] = zsResults.map(_.asInstanceOf[Statistics].count(0).toInt)
    val actualSums: Array[Int] = zsResults.map(_.asInstanceOf[Statistics].sum(0).toInt)
    assertArrayEquals(expectedCounts.filter(_ != 0), actualCounts)
    assertArrayEquals(expectedSums.filter(_ != 0), actualSums)
  }

  test("Zonal statistics with raster file with RDD and projection") {
    val vectorFile = locateResource("/vectors/ne_110m_admin_1_states_provinces.zip")
    val rasterFile = locateResource("/rasters/glc2000_small.tif")

    val polygons: RDD[IFeature] = SpatialReader.readInput(sparkContext,new BeastOptions(), vectorFile.getPath, "shapefile")
      .zipWithIndex()
      .map(featureIndex => new Feature((Row.unapplySeq(featureIndex._1).get :+ featureIndex._2.toInt).toArray,
        StructType(featureIndex._1.schema :+ StructField("index", IntegerType))))

    val projectedPolygons: RDD[IFeature] = Reprojector.reprojectRDD(polygons, CRSServer.sridToCRS(3857))

    val raster: RDD[ITile[Int]] = new RasterFileRDD[Int](sparkContext, rasterFile.getPath, new BeastOptions())

    val zsResults: Array[Collector] =
      ZonalStatistics.zonalStats2(projectedPolygons, raster, classOf[Statistics], new BeastOptions())
        .map(fc => (fc._1.getAs[Int]("index"), fc._2))
        .collect
        .sortBy(_._1)
        .map(_._2)

    val actualCounts: Array[Int] = zsResults.map(_.asInstanceOf[Statistics].count(0).toInt)
    val actualSums: Array[Int] = zsResults.map(_.asInstanceOf[Statistics].sum(0).toInt)
    assertArrayEquals(expectedCounts.filter(_ != 0), actualCounts)
    assertArrayEquals(expectedSums.filter(_ != 0), actualSums)
  }

  test("Zonal statistics with local raptor join") {
    val vectorFile = new Path(locateResource("/vectors/ne_110m_admin_1_states_provinces.zip").getPath)
    val rasterFile = new Path(locateResource("/rasters/glc2000_small.tif").getPath)

    val vectorReader = new ShapefileFeatureReader
    vectorReader.initialize(vectorFile, new BeastOptions())
    import scala.collection.JavaConverters._
    val features: Array[IFeature] = vectorReader.iterator().asScala.toArray
    val rasterReader: IRasterReader[Int] = new GeoTiffReader[Int]
    rasterReader.initialize(rasterFile.getFileSystem(sparkContext.hadoopConfiguration), rasterFile.toString, "0",
      new BeastOptions())

    val zsResults: Array[Collector] = ZonalStatistics.zonalStatsLocal(features, rasterReader, classOf[Statistics])
      .filter(_ != null)

    val actualCounts: Array[Int] = zsResults.map(s => s.asInstanceOf[Statistics].count(0).toInt)
    val actualSums: Array[Int] = zsResults.map(s => s.asInstanceOf[Statistics].sum(0).toInt)
    assertArrayEquals(expectedCounts.filter(_ != 0), actualCounts)
    assertArrayEquals(expectedSums.filter(_ != 0), actualSums)
  }
}
