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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.spark.test.ScalaSparkTest

@RunWith(classOf[JUnitRunner])
class LRUCacheTest extends AnyFunSuite with ScalaSparkTest {
  test("Sort by insertion") {
    val cache = new LRUCache[Int, Int](4)
    cache += ((1, 100))
    cache += ((2, 100))
    cache += ((3, 100))
    cache += ((4, 100))
    cache += ((5, 100))
    assert(!cache.contains(1))
  }

  test("Sort by access") {
    val cache = new LRUCache[Int, Int](4)
    cache += ((1, 100))
    cache += ((2, 100))
    cache += ((3, 100))
    cache += ((4, 100))
    cache.get(1)
    cache += ((5, 100))
    assert(cache.size == 4)
    assert(!cache.contains(2))
  }

  test("With evict code") {
    var evictedRecord: Int = 0
    val cache = new LRUCache[Int, Int](4, (evictedK, evictedV) => evictedRecord = evictedK)
    cache += ((1, 100))
    cache += ((2, 100))
    cache += ((3, 100))
    cache += ((4, 100))
    cache += ((5, 100))
    assert(cache.size == 4)
    assert(!cache.contains(1))
    assert(evictedRecord == 1)
  }
}
