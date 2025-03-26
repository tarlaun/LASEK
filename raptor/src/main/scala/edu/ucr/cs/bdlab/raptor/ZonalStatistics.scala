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
package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, ITile}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.{CSVFeatureWriter, SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{CoordinateXY, Geometry}

import java.awt.geom.Point2D
import java.io.{IOException, PrintWriter}
import java.util
import scala.reflect.ClassTag

@OperationMetadata(shortName = "zs",
  description = "Computes zonal statistics between a vector file and a raster file. Input files (vector, raster)",
  inputArity = "2",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object ZonalStatistics extends CLIOperation with Logging {

  @OperationParam(description = "The statistics function to compute {aggregates}", defaultValue = "aggregates")
  val AggregateFunction = "collector"

  @OperationParam(description = "The name or the index of the raster layer to read from the raster file", defaultValue = "0")
  val RasterLayer = "layer"

  override def addDependentClasses(opts: BeastOptions, classes: util.Stack[Class[_]]): Unit = {
    super.addDependentClasses(opts, classes)
    classes.add(classOf[GeoTiffReader[Int]])
  }

  @throws(classOf[IOException])
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    sc.hadoopConfiguration.setInt("mapreduce.input.fileinputformat.split.maxsize", 8388608)
    val iLayer = opts.getString(RasterLayer, "0")
    opts.set(IRasterReader.RasterLayerID, iLayer)
    val rasterFile = inputs(1)
    val aggregateFunction = Symbol(opts.getString(AggregateFunction, "aggregates"))
    var vectors: RDD[IFeature] = sc.spatialFile(inputs(0), opts.retainIndex(0))
    if (vectors.getNumPartitions < sc.defaultParallelism) {
      logInfo(s"Repartitioning vector data to ${sc.defaultParallelism} partitions")
      vectors = vectors.repartition(sc.defaultParallelism)
    }
    val collectorClass: Class[_ <: Collector] = aggregateFunction match {
      case 'aggregates => classOf[Statistics]
      case other => throw new RuntimeException(s"Unrecognized aggregate function $other")
    }

    // For profiling, count number of processed tiles
    val numTiles = sc.longAccumulator("NumTiles")
    val results: RDD[(IFeature, Collector)] =
      zonalStats2(vectors, new RasterFileRDD[Int](sc, rasterFile, opts), collectorClass, opts, numTiles)

    // Write results to the output
    val element = results.first()
    val numBands = element._2.getNumBands
    var schemaElements: Seq[StructField] = element._1.schema.filterNot(_.dataType == GeometryDataType)
    if (numBands == 1) {
      schemaElements = schemaElements ++ Seq(StructField("sum", DoubleType), StructField("count", DoubleType),
        StructField("min", DoubleType), StructField("max", DoubleType))
    } else {
      for (b <- 1 to numBands) {
        schemaElements = schemaElements ++ Seq(StructField(s"sum_$b", DoubleType), StructField("count_$b", DoubleType),
          StructField("min_$b", DoubleType), StructField("max_$b", DoubleType))
      }
    }
    val normalizedResults: SpatialRDD = results.filter(fc => fc._2 != null).map(fc => {
      val statistics: Statistics = fc._2.asInstanceOf[Statistics]
      val statValues: Array[Any] = new Array[Any](numBands * 4)
      val statFields: Array[StructField] = new Array[StructField](numBands * 4)
      for (b <- 0 until numBands) {
        val suffix: String = if (numBands == 1) "" else s"_${b}"
        statValues(4 * b + 0) = statistics.sum(b)
        statFields(4 * b + 0) = StructField("sum"+suffix, DoubleType)
        statValues(4 * b + 1) = statistics.count(b)
        statFields(4 * b + 1) = StructField("count"+suffix, DoubleType)
        statValues(4 * b + 2) = statistics.min(b)
        statFields(4 * b + 2) = StructField("min"+suffix, DoubleType)
        statValues(4 * b + 3) = statistics.max(b)
        statFields(4 * b + 3) = StructField("max"+suffix, DoubleType)
      }
      Feature.concat(fc._1, new GenericRowWithSchema(statValues, StructType(statFields)))
    })
    val oFormat = opts.getString(SpatialWriter.OutputFormat, opts.getString(SpatialFileRDD.InputFormat))
    if (opts.get(CSVFeatureWriter.WriteHeader) == null)
      opts.set(CSVFeatureWriter.WriteHeader, true)
    val bo: BeastOptions = opts
    SpatialWriter.saveFeatures(normalizedResults, oFormat, outputs(0), bo)
    logInfo(s"Number of processed tiles in zonal statistics is ${numTiles.value}")
  }

  /**
   * Computes zonal statistics between a set of zones (polygons) and a raster file given by its path and a layer in
   * that file. The result is an RDD of pairs of a feature and a collector value
   * @param zones a set of polygons that represent the regions or zones
   * @param raster the RDD of tiles
   * @param collectorClass the class that collects the pixel values to compute the statistics
   * @param opts additional user-defined options
   * @param numTiles an optional accumulator to collect the total number of processed tiles
   * @return a set of (Feature, Statistics)
   */
  def zonalStats2[T](zones: RDD[IFeature], raster: RDD[ITile[T]], collectorClass: Class[_ <: Collector],
                  opts: BeastOptions, numTiles: LongAccumulator = null)(implicit t: ClassTag[T]): RDD[(IFeature, Collector)] = {
    val conf = zones.sparkContext.getConf
    val rasterSRID = raster.first().rasterMetadata.srid
    // Generate a unique ID for each feature
    val idFeatures: RDD[(Long, IFeature)] = zones
      .reproject(rasterSRID)
      .zipWithUniqueId().map(x => (x._2, x._1))

    // (Feature ID, (x, y, value))
    val featureValue: RDD[(Long, (Int, Int, T))] = RaptorJoin.raptorJoinIDFull(raster, idFeatures, opts, numTiles)
      .map(result => (result.featureID, (result.x, result.y, result.m)))

    // Compute statistics for each feature
    val zeroValue = collectorClass.getConstructor().newInstance()
    zeroValue.setNumBands(1)
    val featureStats: RDD[(Long, Collector)] = featureValue.aggregateByKey(zeroValue)(
      (u: Collector, v: (Int, Int, T)) => u.collect(v._1, v._2, Array[Float](v._3.asInstanceOf[Number].floatValue())),
      (u1: Collector, u2: Collector) => u1.accumulate(u2)
    )

    // Join back with the original features to put back the entire feature and remove the ID
    idFeatures.join(featureStats).map( kv => kv._2)
  }

  /**
   * Run zonal statistics locally in one thread. This is useful when the array of geometries is small and the overhead
   * of partitioning could be high.
   * @param zones the array of features that describe the zones.
   * @param raster the raster reader that points to the raster file being aggregated
   * @param collectorClass the class that computes the statistics
   * @return an array of collectors that is equal in length to the input array of features with the result for each.
   *         Features that do not overlap any pixels will have null.
   */
  def zonalStatsLocal[T](geometries: Array[Geometry], raster: IRasterReader[T], collectorClass: Class[_ <: Collector])
                       : Array[Collector] = {
    val joinResults = RaptorJoin.raptorJoinLocal(Array(raster), geometries)
    val constructor = collectorClass.getConstructor()
    val results: Array[Collector] = new Array[Collector](geometries.length)
    joinResults.foreach(result => {
      val m: Array[Float] = result.m match {
        case f: Float => Array(f)
        case f: java.lang.Float => Array(f)
        case i: Integer => Array(i.toFloat)
        case i: Int => Array(i.toFloat)
        case fs: Array[Float] => fs
        case is: Array[Int] => is.map(_.toFloat)
      }
      var collector: Collector = results(result.featureID.toInt)
      if (collector == null) {
        collector = constructor.newInstance()
        collector.setNumBands(m.length)
        results(result.featureID.toInt) = collector
      }
      collector.collect(result.x, result.y, m)
    })
    results
  }

  def zonalStatsLocal[T](zones: Array[IFeature], raster: IRasterReader[T], collectorClass: Class[_ <: Collector])
  : Array[Collector] = zonalStatsLocal(zones.map(_.getGeometry), raster, collectorClass)

  /**
   * Compute zonal statistics for a single geometry using the naive method that scans all pixels and tests
   * point-in-polygon for the center of each pixel.
   * @param geometry the geometry to compute the statistics for
   * @param raster the raster to read the pixels from
   * @param result the result will be collected in this object
   * @tparam T the type of the pixels
   */
  def computeZonalStatisticsNaive[T](geometry: Geometry, raster: IRasterReader[T], result: Collector): Unit = {
    // Find the MBR of the geometry to determine which pixels to read from the raster
    val p: Point2D.Double = new Point2D.Double
    val corner1: Point2D.Double = new Point2D.Double
    val corner2: Point2D.Double = new Point2D.Double
    val mbr = geometry.getEnvelopeInternal
    raster.metadata.modelToGrid(mbr.getMinX, mbr.getMinY, corner1)
    raster.metadata.modelToGrid(mbr.getMaxX, mbr.getMaxY, corner2)
    val i1: Int = Math.max(0, Math.min (corner1.x, corner2.x) ).toInt
    val i2: Int = Math.min(raster.metadata.rasterWidth, Math.ceil (Math.max (corner1.x, corner2.x) ) ).toInt
    val j1: Int = Math.max(0, Math.min (corner1.y, corner2.y) ).toInt
    val j2: Int = Math.min(raster.metadata.rasterHeight, Math.ceil (Math.max (corner1.y, corner2.y) ) ).toInt
    var currentTile: ITile[T] = null
    for (jPixel <- j1 until j2; iPixel <- i1 until i2) {
      raster.metadata.gridToModel(iPixel + 0.5, jPixel + 0.5, p)
      val pointCoords = new CoordinateXY(p.x, p.y)
      if (geometry.contains(geometry.getFactory.createPoint(pointCoords))) {
        val tileID: Int = raster.metadata.getTileIDAtPixel (iPixel, jPixel)
        if (currentTile == null || currentTile.tileID != tileID)
          currentTile = raster.readTile (tileID)
        if (!currentTile.isEmpty(iPixel, jPixel)) {
          val m: Array[Float] = currentTile.getPixelValue(iPixel, jPixel) match {
            case f: Float => Array(f)
            case i: Int => Array(i.toFloat)
            case n: java.lang.Number => Array(n.floatValue())
            case fs: Array[Float] => fs
            case is: Array[Int] => is.map(_.toFloat)
          }
          if (result.getNumBands == 0)
            result.setNumBands(m.length)
          result.collect(iPixel, jPixel, m)
        }
      }
    }
  }

  /**
   * Aggregate all pixels in each of the given geometries using the given collector.
   * This method runs in a naive way by iterating over the given geometries and using the
   * [[computeZonalStatisticsNaive()]] method for each geometry.
   * @param geometries the list of geometries to aggregate
   * @param raster the raster that contains the pixels
   * @param collectorClass the class of the collector
   * @tparam T the type of pixels in the raster
   * @return an array of collectors, one for each geometry. If a geometry is empty, an empty collector will
   *         be stored in the corresponding entry.
   */
  def computeZonalStatisticsNaive[T](geometries: Array[Geometry], raster: IRasterReader[T],
                                     collectorClass: Class[_ <: Collector]): Array[Collector] = {
    val results = new Array[Collector](geometries.length)
    val constructor = collectorClass.getConstructor()
    for (i <- geometries.indices) {
      results(i) = constructor.newInstance()
      computeZonalStatisticsNaive(geometries(i), raster, results(i))
    }
    results
  }

  def printUsage(out: PrintWriter): Unit = {
    out.println("zs <vector file> <raster file> layer:<layer> collector:<collector>")
  }
}
