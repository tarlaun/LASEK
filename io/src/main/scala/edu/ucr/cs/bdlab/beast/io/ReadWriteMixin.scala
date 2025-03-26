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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.{PartitionedSpatialRDD, SpatialRDD}
import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialPartitioner}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.generator.{DistributionType, SpatialGeneratorBuilder}
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry

/**
 * A mixin that adds additional functions to SparkContext and RDD to read and write spatial files
 */
trait ReadWriteMixin {
  /**
   * Additional functions at the context level for loading file
   * @param sc the underlying spark context
   */
  implicit class ReadWriteMixinFunctions(sc: SparkContext) {

    /**
     * Reads the given file according to the given spatial format. If spatial format is not given, it auto-detects
     * the input based on the extension and then file contents (for CSV files only)
     * @param filename the name of the file or directory of files
     * @return the [[SpatialRDD]] that contains the records
     */
    def spatialFile(filename: String, format: String = null, opts: BeastOptions = new BeastOptions): SpatialRDD = {
      val readOpts: BeastOptions = new BeastOptions(opts)
      if (format == null) {
        // Try to auto-detect it
        val detectedOptions = SpatialFileRDD.autodetectInputFormat(filename,
          readOpts.mergeWith(new BeastOptions(sc.hadoopConfiguration)))
        if (detectedOptions == null)
          throw new RuntimeException(s"Cannot autodetect the format of the file '$filename'")
        readOpts.mergeWith(detectedOptions._2)
      } else {
        readOpts.set(SpatialFileRDD.InputFormat, format)
      }
      SpatialReader.readInput(sc, readOpts, filename, readOpts.getString(SpatialFileRDD.InputFormat))
    }

    /**
     * Read the given file name as a spatial file and use the associated options to determine how to load it
     * @param filename the file name
     * @param opts the use options that can be used to determine how to load the file
     * @return the loaded file as a [[SpatialRDD]]
     */
    def spatialFile(filename: String, opts: BeastOptions): SpatialRDD =
      spatialFile(filename, opts.getString(SpatialFileRDD.InputFormat), opts)

    /**
     * Reads features from an Esri Shapefile(c)
     * @param filename the name of the .shp file, a compressed ZIP file that contains shapefiles,
     *                 or a directory that contains shapefiles or ZIP files.
     * @return an RDD of features
     */
    def shapefile(filename: String) : SpatialRDD =
      SpatialReader.readInput(sc, new BeastOptions(), filename, "shapefile")

    /**
     * Reads data from a Shapefile
     * @param filename the name of the GeoJSON file or a directory that contains GeoJSON file
     * @return an RDD of features
     */
    def geojsonFile(filename: String) : SpatialRDD =
      SpatialReader.readInput(sc, new BeastOptions(), filename, "geojson")

    /**
     * Reads points from a CSV file given the names of the columns that contain the x and y coordinates
     * @param filename the name of the file or directory that contains the data
     * @param xColumn the name of the column that contains the x coordinate
     * @param yColumn the name of the column that contains the y coordinate
     * @param delimiter the field delimiter, comma by default
     * @param skipHeader whether to skip the header line or not. If either xColumn or yColumn is String, this
     *                   option will be ignored a header line will be assumed. Otherwise, it is false by default
     * @return the set of records in the file
     */
    def readCSVPoint(filename: String, xColumn: Any = 0, yColumn: Any = 1, delimiter: Char = ',',
                     skipHeader: Boolean = false): SpatialRDD = {
      val opts = new BeastOptions()
      opts.set(CSVFeatureReader.FieldSeparator, delimiter.toString)
      if (xColumn.isInstanceOf[String] || yColumn.isInstanceOf[String])
        opts.set(CSVFeatureReader.SkipHeader, true)
      else
        opts.set(CSVFeatureReader.SkipHeader, skipHeader)
      SpatialReader.readInput(sc, opts, filename, s"point($xColumn,$yColumn)")
    }

    /**
     * Read a CSV file with WKT-encoded geometry
     * @param filename the name of the file or directory fo the input
     * @param wktColumn the column that includes the WKT-encoded geometry, either an Integer for the index of the
     *                  attribute or String for its name
     * @param delimiter the field delimiter, tab by default
     * @param skipHeader whether to skip the header line or not, if wktColumn is a string, this has to be true,
     *                   if wktColumn is an Integer, this is false by default but can be overloaded
     * @return the set of features in the input file
     */
    def readWKTFile(filename: String, wktColumn: Any, delimiter: Char = '\t',
                    skipHeader: Boolean = false): SpatialRDD = {
      val opts = new BeastOptions()
      opts.set(CSVFeatureReader.FieldSeparator, delimiter.toString)
      if (wktColumn.isInstanceOf[String])
        opts.set(CSVFeatureReader.SkipHeader, true)
      else
        opts.set(CSVFeatureReader.SkipHeader, skipHeader)
      SpatialReader.readInput(sc, opts, filename, s"wkt($wktColumn)")
    }

    /**
     * Return a [[SpatialRDD]] of randomly generated geometries according to the given options.
     * @param distribution the type of distribution {[[UniformDistribution]],
     *                     [[DiagonalDistribution]], [[GaussianDistribution]],
     *                     [[SierpinskiDistribution]], [[BitDistribution]],
     *                     [[ParcelDistribution]]}
     * @param cardinality the number of geometries to generate
     * @param opts additional options depending on the type of generator
     * @return an RDD with the generated geometries
     */
    def generateSpatialData(distribution: DistributionType, cardinality: Long,
                            numPartitions: Int = 0,
                            opts: BeastOptions = new BeastOptions) : SpatialRDD = {
      sc.generateSpatialData
        .distribution(distribution)
        .config(opts)
        .numPartitions(numPartitions)
        .generate(cardinality)
    }

    /**
     * Returns a random spatial generator factory
     * @return a factory that can be used to generate spatial data
     */
    def generateSpatialData: SpatialGeneratorBuilder = new SpatialGeneratorBuilder(sc)
  }


  /**
   * Additional functions for SpatialRDD
   * @param rdd the underlying RDD
   */
  implicit class ReadWriteRDDFunctions(rdd: SpatialRDD) {

    /**
     * Performs a range query
     *
     * @param range the spatial range to search for
     * @return
     */
    def rangeQuery(range: Geometry, mbrCount: LongAccumulator = null): SpatialRDD = {
      val convertedRange = Reprojector.reprojectGeometry(range, rdd.first().getGeometry.getSRID)
      val workableRDD: SpatialRDD =
      if (rdd.partitioner.isDefined && rdd.partitioner.get.isInstanceOf[SpatialPartitioner]) {
        // Input is spatially partitioned. Perform a spatial pruning first
        val mbb = new EnvelopeNDLite()
        mbb.merge(convertedRange)

        val spatialPartitioner: SpatialPartitioner = rdd.partitioner.get.asInstanceOf[SpatialPartitioner]
        new PartitionPruningRDD(rdd,
          partitionID => spatialPartitioner.getPartitionMBR(partitionID).intersectsEnvelope(mbb)
        )
      } else {
        // Input not spatially partitioned, run a full scan
        rdd
      }
      (if (mbrCount == null) workableRDD else workableRDD.map(f => {mbrCount.add(1); f}))
        .filter(f => f.getGeometry.intersects(convertedRange))
    }

    /**
     * Save features as a shapefile
     *
     * @param filename the output filename
     */
    def saveAsShapefile(filename: String, opts: BeastOptions = new BeastOptions()): Unit =
      rdd.writeSpatialFile(filename, "shapefile", opts)

    /**
     * Save features in GeoJSON format
     * @param filename the output filename
     */
    def saveAsGeoJSON(filename: String, opts: BeastOptions = new BeastOptions()): Unit =
      rdd.writeSpatialFile(filename, "geojson", opts)

    /**
     * Save features to a CSV or text-delimited file. This method should be used only for point features.
     * @param filename the name of the output file
     * @param xColumn the index of the column that contains the x-coordinate in the output file
     * @param yColumn the index of the column that contains the y-coordinate in the output file
     * @param delimiter the delimiter in the output file, comma by default
     * @param header whether to write a header line, true by default
     */
    def saveAsCSVPoints(filename: String, xColumn: Int = 0, yColumn: Int = 1,
                        delimiter: Char = ',', header: Boolean = true): Unit = {
      val opts = new BeastOptions()
      opts.set(CSVFeatureWriter.WriteHeader, header)
      opts.set(CSVFeatureWriter.FieldSeparator, delimiter.toString)
      rdd.writeSpatialFile(filename, s"point($xColumn,$yColumn)", opts)
    }

    /**
     * Save features to a CSV file where the geometry is encoded in WKT format
     * @param filename the name of the output file
     * @param wktColumn the index of the column that contains the WKT attribute
     * @param delimiter the delimiter between attributes, tab by default
     * @param header whether to write a header line or not, true by default
     */
    def saveAsWKTFile(filename: String, wktColumn: Int, delimiter: Char = '\t', header: Boolean = true): Unit = {
      val opts = new BeastOptions()
      opts.set(CSVFeatureWriter.WriteHeader, header)
      opts.set(CSVFeatureWriter.FieldSeparator, delimiter.toString)
      rdd.writeSpatialFile(filename, s"wkt($wktColumn)", opts)
    }

    /**
     * Save features in KML format
     * @param filename the name of the output file
     */
    def saveAsKML(filename: String): Unit =
      rdd.writeSpatialFile(filename, "kml")

    /**
     * Write this RDD as a spatial file with the given format and additional options
     * @param filename the output file name
     * @param oformat the output file format (short name)
     * @param opts additional user options
     */
    def writeSpatialFile(filename: String, oformat: String, opts: BeastOptions = new BeastOptions): Unit = {
      if (rdd.partitioner.isDefined && rdd.partitioner.get.isInstanceOf[SpatialPartitioner])
        IndexHelper.saveIndex2(rdd, filename, new BeastOptions("oformat" -> oformat).mergeWith(opts))
      else
        SpatialWriter.saveFeatures(rdd, oformat, filename, opts)
    }

    /**
     * Convert the SpatialRDD to a DataFrame where the geometry and the other attributes are represented as a row
     * @param sparkSession
     * @return
     */
    def toDataFrame(sparkSession: SparkSession): DataFrame =
      SpatialReader.rddToDataFrame(sparkSession, rdd)
  }

  implicit class ReadWriteDataframeFunctions(df: DataFrame) {
    /**
     * Converts a dataframe that contains a geometry column to a SpatialRDD
     * @return
     */
    def toSpatialRDD: SpatialRDD = SpatialReader.dataFrameToSpatialRDD(df)
  }

  /**
   * Shortcut functions for SpatialRDDs that are partitioned by any spatial partitioner
   * @param partitionedRDD
   */
  implicit class ReadWritePartitionedRDDFunctions(partitionedRDD: PartitionedSpatialRDD) {
    require(partitionedRDD.partitioner.isDefined && partitionedRDD.partitioner.get.isInstanceOf[SpatialPartitioner],
      "This function requires the RDD to be partitioned by a spatial partitioner")

    /**
     * Performs a range query by first pruning non-relevant partitions and then applying the filter on the
     * non-pruned partitions
     * @param range the range to filter as a geometry
     * @return the filtered records maintaining the same spatial partitioning of the input
     */
    def rangeQuery(range: Geometry, mbrCount: LongAccumulator = null): PartitionedSpatialRDD = {
      val mbb = new EnvelopeNDLite()
      mbb.merge(range)
      val spatialPartitioner: SpatialPartitioner = partitionedRDD.partitioner.get
        .asInstanceOf[SpatialPartitioner]
      val prunedRDD = new PartitionPruningRDD(partitionedRDD,
        partitionID => spatialPartitioner.getPartitionMBR(partitionID).intersectsEnvelope(mbb))
      (if (mbrCount == null) prunedRDD else prunedRDD.map(f => {mbrCount.add(1); f}))
        .filter(f => f._2.getGeometry.intersects(range))
    }

  }
}

object ReadWriteMixin extends ReadWriteMixin