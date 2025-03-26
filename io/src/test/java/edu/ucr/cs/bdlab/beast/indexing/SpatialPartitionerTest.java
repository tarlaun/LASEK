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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.test.JavaSpatialSparkTest;

public class SpatialPartitionerTest extends JavaSpatialSparkTest {

  public void testComputeNumberOfPartitions() {
    Summary s = new Summary();
    s.set(new double[]{-1.0, -2.0}, new double[] {2.0, 4.0});
    s.setSize(10L * 1024 * 1024 * 1024); // 10 GB
    s.setNumFeatures(10000000); // 10M records ~> 1 KB / record
    assertEquals(10, IndexHelper.computeNumberOfPartitions(
        "fixed", 10, s));
    assertEquals(80, IndexHelper.computeNumberOfPartitions(
        "size", 128L*1024*1024, s));
    assertEquals(100, IndexHelper.computeNumberOfPartitions(
        "count", 100000, s));
  }

}