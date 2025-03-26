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

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SpatialParquetSourceTest extends AnyFunSuite with BeastSpatialTest {
  test("Encode and decode spatial parquet") {
    val input = locateResource("/allfeatures.geojson")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    val encodedDataFrame = SpatialParquetSource.encodeSpatialParquet(dataframe)
    val decodedDataFrame = SpatialParquetSource.decodeSpatialParquet(encodedDataFrame, "geometry")
    val expectedData = dataframe.collect()
    val actualData = decodedDataFrame.collect()
    assertResult(expectedData)(actualData)
  }

  test("Encode and decode geo parquet") {
    val input = locateResource("/allfeatures.geojson")
    val dataframe = sparkSession.read.format("geojson").load(input.getPath)
    val encodedDataFrame = SpatialParquetSource.encodeGeoParquet(dataframe)
    val decodedDataFrame = SpatialParquetSource.decodeGeoParquet(encodedDataFrame, "geometry")
    val expectedData = dataframe.collect()
    val actualData = decodedDataFrame
      .drop("geometry_minx", "geometry_miny", "geometry_maxx", "geometry_maxy").collect()
    assertResult(expectedData)(actualData)
  }
}
