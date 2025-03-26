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
package edu.ucr.cs.bdlab.beast.util

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BeastOptionsTest extends AnyFunSuite with ScalaSparkTest {
  test("retain index") {
    val opts = new BeastOptions().set("key1[1]", "val1")
      .set("key1[2]", "val2")
      .set("key3", "val3")
      .set("key4[2]", "val4")
    assert(opts("key1[1]") == "val1")
    val opts1 = opts.retainIndex(1)
    assert(opts1("key1") == "val1")
    assert(!opts1.contains("key4"))
    assert(opts1("key3") == "val3")
    val opts2 = opts.retainIndex(2)
    assert(opts2("key1") == "val2")
    assert(opts2("key3") == "val3")
    assert(opts2("key4") == "val4")
  }

  test("Load defaults") {
    val opts = new BeastOptions(false)
    assert(opts.isEmpty)

    val opts2 = new BeastOptions()
    assert(opts2.size == 1)
    assert(opts2("testkey") == "testvalue")
  }

  test("Convert from a map") {
    def opts: BeastOptions = Map("key1" -> "value1", "key2" -> "value2")
    assert(opts("key1") == "value1")
    assert(opts("key2") == "value2")
  }
}
