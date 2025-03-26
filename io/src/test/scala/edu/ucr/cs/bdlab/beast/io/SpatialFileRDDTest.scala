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

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.generator.{RandomSpatialRDD, UniformDistribution}
import edu.ucr.cs.bdlab.beast.indexing.{GridPartitioner, IndexHelper}
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.FilePartition
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureReader
import edu.ucr.cs.bdlab.beast.synopses.Summary
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, FileOutputStream}
import java.util

@RunWith(classOf[JUnitRunner])
class SpatialFileRDDTest extends AnyFunSuite with ScalaSparkTest {

  test("Should not skip hidden files if explicitly specified") {
    val csvPath = new File(scratchDir, "_temp.csv")
    copyResource("/test_wkt.csv", csvPath)
    val features = new SpatialFileRDD(sparkContext, csvPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "wkt(1)", CSVFeatureReader.SkipHeader -> true,
        CSVFeatureReader.FieldSeparator -> '\t'))
    assert(features.count == 2)
  }

  test("Should read CSV files") {
    val csvPath = makeFileCopy("/test_wkt.csv")
    val features = new SpatialFileRDD(sparkContext, csvPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "wkt(1)", CSVFeatureReader.SkipHeader -> true,
        CSVFeatureReader.FieldSeparator -> '\t'))
    assert(features.count() == 2)
  }

  test("passed options should override other options") {
    val csvPath = makeFileCopy("/test_wkt.csv")
    sparkContext.hadoopConfiguration.set("iformat", "error")
    val features = new SpatialFileRDD(sparkContext, csvPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "wkt(1)", CSVFeatureReader.SkipHeader -> true,
        CSVFeatureReader.FieldSeparator -> '\t'))
    assert(features.count() == 2)
  }

  test("duplicate avoidance with no range query") {
    // Read a disjoint partitioned file and make sure that duplicate records are not repeated
    val indexPath = makeDirCopy("/test_index")
    val features = new SpatialFileRDD(sparkContext, indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "envelopek(2)", CSVFeatureReader.FieldSeparator -> ','))
    assert(features.count() == 3)
  }

  test("Skip duplicate avoidance") {
    // Read a partitioned file with repeated records and make sure that the repeated record gets read twice
    val indexPath = makeDirCopy("/test_index")
    val features = new SpatialFileRDD(sparkContext, indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "envelopek(2)", CSVFeatureReader.FieldSeparator -> ','))
    SpatialFileRDD.skipDuplicateAvoidance(features)
    assert(features.count() == 4)
  }

  test("recursive processing") {
    val filePath = new File(scratchDir, "dir")
    val csvPath = new File(filePath, "subdir/deeper/tree/temp.csv")
    copyResource("/test_points.csv", csvPath)
    val features = new SpatialFileRDD(sparkContext, csvPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "point(1,2)", CSVFeatureReader.FieldSeparator -> ',',
        CSVFeatureReader.SkipHeader -> true, SpatialFileRDD.Recursive -> true))
    assert(features.count() == 2)
  }

  test("read multiple input paths") {
    val input1: File = makeResourceCopy("/usa-major-highways")
    val input2: File = makeResourceCopy("/usa-major-cities")
    val partitions = SpatialFileRDD.createPartitions(input1.getPath+","+input2.getPath,
      "iformat"->"shapefile", sparkContext.hadoopConfiguration)
    assert(partitions.length == 2)
  }

  test("recursive with shapefile") {
    val filePath = new File(scratchDir, "dir")
    val shpPath = new File(filePath, "subdir/deeper/tree/usa-major-cities")
    makeResourceCopy("/usa-major-cities", shpPath)

    val features = new SpatialFileRDD(sparkContext, shpPath.getPath, SpatialFileRDD.InputFormat -> "shapefile")
    assert(features.getNumPartitions == 1)
    assert(features.partitions(0).asInstanceOf[FilePartition].path.endsWith("usa-major-cities.shp"))
  }

  test("Read CSV points") {
    val csvPath = makeFileCopy("/test_points.csv")
    val features = new SpatialFileRDD(sparkContext, csvPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "point(1,2)", CSVFeatureReader.SkipHeader -> true,
        CSVFeatureReader.FieldSeparator -> ','))
    assert(features.count() == 2)
  }

  test("Read shapefile") {
    val shpPath = makeFileCopy("/usa-major-cities/usa-major-cities.shp")
    val dbfPath = makeFileCopy("/usa-major-cities/usa-major-cities.dbf")
    val features = new SpatialFileRDD(sparkContext, shpPath.getPath, SpatialFileRDD.InputFormat -> "shapefile")
    assert(features.count() == 120)
  }

  test("Read shapefile from directory") {
    val shpPath = makeDirCopy("/usa-major-cities")
    val features = new SpatialFileRDD(sparkContext, shpPath.getPath, SpatialFileRDD.InputFormat -> "shapefile")
    assert(features.count() == 120)
  }

  test("Read many shapefiles from directory") {
    val inputPath = new File(scratchDir, "temp")
    inputPath.mkdir()
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(inputPath, "temp.shp"))
    copyResource("/usa-major-cities/usa-major-cities.dbf", new File(inputPath, "temp.dbf"))
    copyResource("/usa-major-cities/usa-major-cities.shp", new File(inputPath, "temp2.shp"))
    copyResource("/usa-major-cities/usa-major-cities.dbf", new File(inputPath, "temp2.dbf"))

    val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "shapefile")
    assert(features.count() == 120 * 2)
  }

  test("Should generate splits from master file") {
    // Create a directory with a master file and some data files
    // Make sure that getSplits method will be limited to what is listed in the master file
    val indexPath = new File(scratchDir, "index")
    val masterFilePath = new File(indexPath, "_master.heap")
    copyResource("/test.partitions", masterFilePath)
    // Create fake files
    for (partitionID <- 0 until 45) {
      val out = new FileOutputStream(new File(indexPath, f"part-$partitionID%05d"))
      out.write(1)
      out.close()
    }

    val features = new SpatialFileRDD(sparkContext, indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "point", SpatialFileRDD.SplitFiles -> false))
    assert(features.getNumPartitions == 44)
  }

  test("Should work with a trailing slash") {
    // Create a directory with a master file and some data files
    // Make sure that getSplits method will be limited to what is listed in the master file
    val indexPath = new File(scratchDir, "index")
    val masterFilePath = new File(indexPath, "_master.heap")
    copyResource("/test.partitions", masterFilePath)
    // Create fake files
    for (partitionID <- 0 until 45) {
      val out = new FileOutputStream(new File(indexPath, f"part-$partitionID%05d"))
      out.write(1)
      out.close()
    }

    val features = new SpatialFileRDD(sparkContext, indexPath.getPath+"/",
      Seq(SpatialFileRDD.InputFormat -> "point", SpatialFileRDD.SplitFiles -> false))
    assert(features.getNumPartitions == 44)
  }

  test("Custom block size") {
    val input = makeFileCopy("/test.partitions")
    val features = new SpatialFileRDD(sparkContext, input.getPath,
      Seq(SpatialFileRDD.MaxSplitSize -> 6 * 1024, SpatialFileRDD.InputFormat -> "wkt"))
    assert(features.getNumPartitions == 2)
  }

  test("Should work with compressed shapefiles") {
    // Point to an input directory that has several compressed zip files. Make sure that the records inside these
    // shapefiles will be returned.
    val inputPath = new File(scratchDir, "index")
    inputPath.mkdir()
    copyResource("/usa-major-cities.zip", new File(inputPath, "temp1.zip"))
    copyResource("/usa-major-cities.zip", new File(inputPath, "temp2.zip"))

    val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "shapefile")
    assert(features.count() == 240)
  }

  test("should not split zip files") {
    val inputPath = makeFileCopy("/usa-major-cities.zip")
    val features = new SpatialFileRDD(sparkContext, inputPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "shapefile", SpatialFileRDD.MinSplitSize -> 1024,
        SpatialFileRDD.MaxSplitSize -> 2 * 1024))
    assert(features.getNumPartitions == 1)
  }

  test("Should throw exception for wrong formats") {
    val inputPath = makeFileCopy("/usa-major-cities.zip")
    var errorRaised: Boolean = false
    try {
      new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "point(1,2)")
    } catch {
      case _: Throwable => errorRaised = true
    }
    assert(!errorRaised, "No error should be raised")

    // An invalid input should raise an exception
    errorRaised = false
    try {
      val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "errrr")
      assert(features.featureReaderClass == null)
    } catch {
      case _: Throwable => errorRaised = true
    }
    assert(errorRaised, "An error should be raised")

    // A correction for slight typos should be reported in the error message
    errorRaised = false
    try {
      val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "piont")
      assert(features.featureReaderClass == null)
    } catch {
      case e: Throwable =>
        errorRaised = true
        assert(e.getMessage.contains("point"), "The error message should suggest a correction")
    }
    assert(errorRaised, "An error should be raised")

    // Suggest a correct when extra parameters are added
    errorRaised = false
    try {
      val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "piont(1,2)")
      assert(features.featureReaderClass == null)
    } catch {
      case e: Throwable =>
        errorRaised = true
        assert(e.getMessage.contains("point"), "The error message should suggest a correction")
    }
    assert(errorRaised, "An error should be raised")
  }

  test("Detect the input") {
    // A shapefile is detected from the file extension without opening the file
    var inputPath = new File(scratchDir, "input.shp")
    inputPath.createNewFile()
    var features = new SpatialFileRDD(sparkContext, inputPath.getPath)
    assert(features.featureReaderClass == classOf[ShapefileFeatureReader])

    // Test a case of unsupported file
    inputPath = new File(scratchDir, "input.xxx")
    inputPath.createNewFile()
    intercept[Throwable](
      {
        features = new SpatialFileRDD(sparkContext, inputPath.getPath)
        assert(features.featureReaderClass == null)
      }, "Error should be raised for an input that cannot be auto-detected"
    )

    // Test auto detection when the input path points to a directory
    inputPath = locateResource("/usa-major-cities")
    features = new SpatialFileRDD(sparkContext, inputPath.getPath)
    assert(classOf[ShapefileFeatureReader].isAssignableFrom(features.featureReaderClass),
      s"Invalid feature reader class ${features.featureReaderClass}")
  }

  test("Add dependent classes with one input format") {
    val input = makeFileCopy("/test_wkt.csv")
    val features = new SpatialFileRDD(sparkContext, input.getPath, SpatialFileRDD.InputFormat -> "wkt")
    val dependentClasses: util.Stack[Class[_]] = new util.Stack[Class[_]]
    val opts: BeastOptions = SpatialFileRDD.InputFormat -> "wkt"
    SpatialFileRDD.addDependentClasses(opts, dependentClasses)
    assert(dependentClasses.size() == 1)
  }

  test("Read master file with custom quotes") {
    val indexPath = makeDirCopy("/test_index")
    val features = new SpatialFileRDD(sparkContext, indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "envelopek(2)", CSVFeatureReader.FieldSeparator -> ',',
        CSVFeatureReader.QuoteCharacters -> "\"\""))
    assert(features.count() == 3)
  }

  test("Should create a partitioned dataset from a master file") {
    val indexPath = locateResource("/test_index")
    val features = new SpatialFileRDD(sparkContext, indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "envelopek(2)", CSVFeatureReader.FieldSeparator -> ','))
    assert(features.partitioner.isDefined)
    assert(features.partitioner.get.isInstanceOf[SpatialPartitioner])
    assert(features.partitioner.get.numPartitions == 2)
  }

  test("filter by pattern") {
    val input = new File(scratchDir, "temp")
    input.mkdir()
    copyResource("/test_wkt.csv", new File(input, "file1.xyz"))
    copyResource("/test_wkt.csv", new File(input, "file2.xyz"))
    copyResource("/test_wkt.csv", new File(input, "file3.xz"))
    copyResource("/test_wkt.csv", new File(input, "file4.abc"))
    val features = new SpatialFileRDD(sparkContext, input.getPath, SpatialFileRDD.InputFormat -> "fakescalareader")
    assert(features.getNumPartitions == 2)
  }

  test("Should work with Scala FeatureReader classes") {
    val inputFile = makeFileCopy("/features.geojson")
    val features = new SpatialFileRDD(sparkContext, inputFile.getPath, SpatialFileRDD.InputFormat -> "fakescalareader")
    assert(features.featureReaderClass == classOf[FakeScalaReader])
  }

  test("Should respect split files") {
    val inputFile = makeFileCopy("/allfeatures.geojson")
    val features = new SpatialFileRDD(sparkContext, inputFile.getPath,
      Seq(SpatialFileRDD.InputFormat -> "fakescalareader", SpatialFileRDD.MaxSplitSize -> 1024))
    assert(features.getNumPartitions == 1)
  }

  test("Should work with master files with additional columns") {
    val inputPath = makeDirCopy("/test_index_with_deleted_records")
    val features = new SpatialFileRDD(sparkContext, inputPath.getPath, SpatialFileRDD.InputFormat -> "wkt")
    assert(features.getNumPartitions == 2)
  }

  test("local read") {
    val inputFile = makeFileCopy("/allfeatures.geojson")
    val opts: BeastOptions = SpatialFileRDD.InputFormat -> "geojson"
    val featureReaderClass = SpatialFileRDD.getFeatureReaderClass(inputFile.getPath, opts)
    val partitions = SpatialFileRDD.createPartitions(inputFile.getPath, opts, sparkContext.hadoopConfiguration)
    var featureCount: Int = 0
    for (partition <- partitions) {
      val features = SpatialFileRDD.readPartition(partition, featureReaderClass, true, opts)
      featureCount += features.length
    }
    assert(featureCount == 7)
  }

  test("use MBR to filter out non-matching partitions") {
    val indexPath = makeDirCopy("/test_index")
    val partitions = SpatialFileRDD.createPartitions(indexPath.getPath,
      Seq(SpatialFileRDD.InputFormat -> "point(0,1)", CSVFeatureReader.FieldSeparator -> ',',
        SpatialFileRDD.FilterMBR -> "0,0,1,1"), sparkContext.hadoopConfiguration)
    assert(partitions.length == 1)
  }

  test("Read local multiple files") {
    val input = new File(scratchDir, "inputdir")
    input.mkdir()
    copyResource("/test_wkt.csv", new File(input, "file1"))
    copyResource("/test_wkt.csv", new File(input, "file2"))
    val features = SpatialFileRDD.readLocal(input.getPath, "wkt(1)",
      Seq(CSVFeatureReader.SkipHeader -> true, CSVFeatureReader.FieldSeparator -> '\t'), sparkContext.hadoopConfiguration)
    assert(features.length == 4)
  }

  test("Duplicate avoidance points corner cases") {
    val features = new RandomSpatialRDD(sparkContext, UniformDistribution, 100, opts = "seed" -> 0)
    val partitionedFeatures =
      IndexHelper.partitionFeatures(features, new GridPartitioner(Summary.computeForFeatures(features), 1))
    val indexPath = new File(scratchDir, "testindex").getPath
    IndexHelper.saveIndex(partitionedFeatures, indexPath, "iformat" -> "point")
    // Read it back
    val points = new SpatialFileRDD(sparkContext, indexPath, "iformat" -> "point")
    assert(points.count() == 100)
  }
}
