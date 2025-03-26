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
package edu.ucr.cs.bdlab.beast.cg

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite

/**
 * Some helper functions for SpatialJoin
 */
object SpatialJoinHelper {

  /**
   * Performs a spatial join between the input arrays of envelopes.
   * @param boxes1
   * @param boxes2
   * @param result
   */
  def planeSweepRectangles(boxes1: IndexedSeq[EnvelopeNDLite], boxes2: IndexedSeq[EnvelopeNDLite], result: (Int, Int) => Unit): Unit = {
    // TODO: Run a real plane-sweep algorithm
    for (i1 <- boxes1.indices; i2 <- boxes2.indices; if boxes1(i1).intersectsEnvelope(boxes2(i2)))
      result(i1, i2)
  }

}
