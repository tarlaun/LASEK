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
package edu.ucr.cs.bdlab.davinci

import org.apache.hadoop.fs.Path
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DiskTileHashtableTest extends AnyFunSuite with ScalaSparkTest {
  test("Write and read") {
    val rand = new Random(0)
    val entries = new Array[(Long, Long, Int)](100).map( _ => (rand.nextLong().abs, rand.nextLong().abs, rand.nextInt().abs))
    val file = new Path(scratchPath, "test")
    val fileSystem = file.getFileSystem(sparkContext.hadoopConfiguration)
    val out = fileSystem.create(file)
    DiskTileHashtable.construct(out, entries)
    out.close()

    // Read it back
    for ((key, expectedValue1, expectedValue2) <- entries) {
      val actualValue = DiskTileHashtable.getValue(fileSystem, file, key)
      assertResult((expectedValue1, expectedValue2))(actualValue)
    }
    // Read a value that does not exist
    var numNonExistentKeys = 0
    for (i <- 1 to 100) {
      val randomKey = rand.nextLong().abs
      if (!entries.exists(_._1 == randomKey)) {
        numNonExistentKeys += 1
        assertResult(null)(DiskTileHashtable.getValue(fileSystem, file, randomKey))
      }
    }
    assert(numNonExistentKeys > 0)
  }
}
