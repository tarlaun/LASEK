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

import java.io.Serializable;

/**
 * An interface for a process that collects pixel values from the raster layer and accumulates them.
 * Created by Ahmed Eldawy on 5/19/2017.
 */
public interface Collector extends Serializable {

  /**
   * Initializes the collector with the given number of bands.
   * @param n the number of bands of data to be collected
   */
  void setNumBands(int n);

  /**
   * Collects a value of the given location which has multiple bands.
   * @param column the column (x-coordinate)
   * @param row the row (y-coordinate)
   * @param value (output) the value will be written to this array
   * @return this collector to call this method serially
   */
  Collector collect(int column, int row, int[] value);

  Collector collect(int column, int row, float[] value);

  /**
   * Collects all values in a given block range. The array values is assumed to contain consecutive values for each
   * band in each pixel stored row-wise. In other words, the first N entries in the array <code>values</code> are used
   * where N = width * height * number of bands.
   * @param column the first column (x-dimension)
   * @param row the first row (y-dimension)
   * @param width the width in pixels
   * @param height the height in pixels
   * @param values (output) the valus will be written to this list
   * @return this collector to call this method serially
   */
  Collector collect(int column, int row, int width, int height, int[] values);

  /**
   * Accumulates the value of this collector with another one. Helpful for combining several partial results.
   * @param c another collector to accumulate its value
   * @return this collector to call this method serially
   */
  Collector accumulate(Collector c);

  /**
   * Return the total number of bands
   * @return the number of bands of the data contained in this collector
   */
  int getNumBands();

  /**
   * Make the value of this collector invalid to indicate an error in processing
   */
  void invalidate();

  /**
   * Whether the value of the collector is valid or not
   * @return {@code true} if this collector is not empty
   */
  boolean isValid();
}
