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
package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RaptorJoinJavaTest extends JavaSparkTest {

  public static GeometryFactory factory = GeometryReader.DefaultGeometryFactory;

  public void testRaptorJoinZS() {
    String rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath();
    Geometry testPoly = factory.toGeometry(new Envelope(-82.76, -80.25, 31.91, 35.17));
    List<Tuple2<Long, IFeature>> features = Arrays.asList(new Tuple2<Long, IFeature>(1L, Feature.create(null, testPoly)));
    JavaPairRDD<Long, IFeature> vector = javaSparkContext().<Long, IFeature>parallelizePairs(features);
    JavaRDD<ITile<Integer>> rasters = RasterFileRDD.readRaster(javaSparkContext(), rasterFile, new BeastOptions());

    JavaRDD<RaptorJoinResult<Integer>> values = RaptorJoin.raptorJoinIDFullJ(rasters, vector, new BeastOptions());

    // Values sorted by (x, y)
    RaptorJoinResult[] finalValues = values.collect().toArray(new RaptorJoinResult[0]);
    Arrays.sort(finalValues, (a, b) -> (a.x() != b.x()? a.x() - b.x() : a.y() - b.y()));
    assertEquals(6, finalValues.length);
    assertEquals(69, finalValues[0].x());
    assertEquals(48, finalValues[0].y());
  }
}