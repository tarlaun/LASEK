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

import edu.ucr.cs.bdlab.beast.common.BeastOptions

import java.util.Calendar
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GPXReaderTest extends AnyFunSuite with ScalaSparkTest {

  test("ReadSampleFile") {
    val inputPath = new Path(makeFileCopy("/001005279.gpx").getPath)
    val gpxReader: FeatureReader = new GPXReader()
    val fileSystem = inputPath.getFileSystem(sparkContext.hadoopConfiguration)
    gpxReader.initialize(new FileSplit(inputPath, 0, fileSystem.getFileStatus(inputPath).getLen, null),
      new BeastOptions())
    var count = 0
    while (gpxReader.nextKeyValue()) {
      count += 1
      val feature = gpxReader.getCurrentValue
      assert(feature.getGeometry.getGeometryType == "Point")
      if (count == 1) {
        // Check first record
        assert(feature.length == 8)
        assert(feature.getAs[Double]("ele") == 188.92)
        assert(feature.getAs[String]("filename") == inputPath.getName)
        assert(feature.getAs[Int]("tracknumber") == 0)
        assert(feature.getAs[String]("trackname") == "Track 0")
        assert(feature.getAs[Int]("segmentnumber") == 0)
        val timestamp = feature.getAs("time").asInstanceOf[Calendar]
        assert(timestamp.get(Calendar.DAY_OF_MONTH) == 8)
        assert(timestamp.get(Calendar.YEAR) == 2011)
        assert(timestamp.get(Calendar.SECOND) == 20)
      } else if (count == 23) {
        // Last record should not have time but it should still have seven attributes (time will be null)
        assert(feature.length == 8)
        assert(feature.getAs[String]("time") == null)
      }
    }
    assert(count == 23)
    gpxReader.close
  }

  test("ReadMultitrackOneSplit") {
    val inputPath = new Path(makeFileCopy("/multitrack.gpx").getPath)
    val gpxReader: FeatureReader = new GPXReader()
    val fileSystem = inputPath.getFileSystem(sparkContext.hadoopConfiguration)
    gpxReader.initialize(new FileSplit(inputPath, 0, fileSystem.getFileStatus(inputPath).getLen, null),
      new BeastOptions())
    var pointCount = 0
    var lastTrackNumber = -1
    var trackCount = 0
    var lastSegmentNumber = -1
    var segmentCount = 0
    while (gpxReader.nextKeyValue()) {
      pointCount += 1
      val feature = gpxReader.getCurrentValue
      assert(feature.getGeometry.getGeometryType == "Point")
      if (feature.getAs("tracknumber") != lastTrackNumber) {
        trackCount += 1
        lastTrackNumber = feature.getAs("tracknumber").asInstanceOf[Int]
      }
      if (feature.getAs("segmentnumber") != lastSegmentNumber) {
        segmentCount += 1
        lastSegmentNumber = feature.getAs("segmentnumber").asInstanceOf[Int]
      }
    }
    assert(pointCount == 23)
    assert(trackCount == 2)
    assert(segmentCount == 3)

    gpxReader.close
  }
}
