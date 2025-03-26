/*
 * Copyright 2023 University of California, Riverside
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

import org.apache.spark.internal.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Parallel2 extends Logging {

  /**
   * Runs the given processor on sub-ranges of the given range in parallel and return an array of all results.
   * @param start the start of the range (inclusive)
   * @param end the end of the range (exclusive)
   * @param processor the function that processes a range
   * @param parallelism the maximum number of background threads to create
   * @tparam T the type of return result
   * @return an array of all results or an empty array if the range is empty
   */
  def forEach[T](start: Int, end: Int, processor: (Int, Int) => T, parallelism: Int): Traversable[T] = {
    // Return an empty result for empty arrays
    if (end <= start)
      return Seq[T]()
    // Calculate the indices that define the partitions among the given range for threads
    val threads = new Array[Future[T]](parallelism min (end - start))
    for (i_thread <- threads.indices) {
      val i1 = i_thread * (end - start) / threads.length + start
      val i2 = (i_thread + 1) * (end - start) / threads.length + start
      threads(i_thread) = Future[T] {
        processor(i1, i2)
      }
    }

    threads.map(t => Await.result(t, Duration.Inf))
  }

  /**
   * Runs the given processor on sub-ranges in the range [0, length) in parallel and return an array of all results.
   * @param length the number of entries to process
   * @param processor the function that processes a range
   * @tparam T the type of return result
   * @return an array of all results or an empty array if the range is empty
   */
  def forEach[T](length: Int, processor: (Int, Int) => T): Traversable[T] =
    forEach[T](0, length, processor, Runtime.getRuntime.availableProcessors)

  /**
   * Runs the given processor on sub-ranges of the given range in parallel and return an array of all results.
   * @param start the start of the range (inclusive)
   * @param end the end of the range (exclusive)
   * @param processor the function that processes a range
   * @tparam T the type of return result
   * @return an array of all results or an empty array if the range is empty
   */
  def forEach[T](start: Int, end: Int, processor: (Int, Int) => T): Traversable[T] =
    forEach[T](start, end, processor, Runtime.getRuntime.availableProcessors)

}
