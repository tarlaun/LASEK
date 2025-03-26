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

/**
 * A class that stores simple statistics to be computed on raster files. More specifically, it contains the following
 * four values:
 * <ul>
 *   <li>Sum: The sum of all values</li>
 *   <li>Count: The number of elements</li>
 *   <li>Min: The minimum value</li>
 *   <li>Max: The maximum value</li>
 * </ul>
 * Created by Ahmed Eldawy on 5/16/2017.
 */
public class Statistics implements Collector {
  /**The summation of all values represented by this object*/
  public double[] sum;

  /**The number of elements represented by this object*/
  public double[] count;

  /**The minimum value represented by this object*/
  public double[] max;

  /**The maximum value represented by this object*/
  public double[] min;

  /**A flag that is set when the value becomes invalid*/
  private boolean invalid;

  @Override
  public String toString() {
    if (sum == null || sum.length == 0)
      return "Empty statistics";
    return String.format("{sum: %f, count: %f, max: %f, min: %f}", sum[0], count[0], max[0], min[0]);
  }

  @Override
  public void setNumBands(int n) {
    this.sum = new double[n];
    this.min = new double[n];
    this.max = new double[n];
    this.count = new double[n];
    for (int i = 0; i < n; i++) {
      this.min[i] = Integer.MAX_VALUE;
      this.max[i] = Integer.MIN_VALUE;
    }
  }

  @Override
  public Statistics collect(int column, int row, int[] value) {
    for (int i = 0; i < value.length; i++) {
      if (value[i] < min[i])
        min[i] = value[i];
      if (value[i] > max[i])
        max[i] = value[i];
      sum[i] += value[i];
      count[i]++;
    }
    return this;
  }

  @Override
  public Statistics collect(int column, int row, float[] value) {
    for (int i = 0; i < value.length; i++) {
      if (value[i] < min[i])
        min[i] = value[i];
      if (value[i] > max[i])
        max[i] = value[i];
      sum[i] += value[i];
      count[i]++;
    }
    return this;
  }

  @Override
  public Statistics collect(int column, int row, int width, int height, int[] values) {
    int numOfDataValues = min.length * width * height;
    for (int i = 0; i < numOfDataValues; i++) {
      int j = i % this.sum.length;
      if (values[i] < min[j])
        min[j] = values[i];
      if (values[i] > max[j])
        max[j] = values[i];
      sum[j] += values[i];
    }
    for (int j = 0; j < this.count.length; j++)
      count[j] += width * height;
    return this;
  }

  @Override
  public Statistics accumulate(Collector c) {
    Statistics s = (Statistics) c;
    for (int i = 0; i < sum.length; i++) {
      this.sum[i] += s.sum[i];
      this.count[i] += s.count[i];
      if (s.max[i] > this.max[i])
        this.max[i] = s.max[i];
      if (s.min[i] < this.min[i])
        this.min[i] = s.min[i];
    }
    this.invalid = this.invalid || s.invalid;
    return this;
  }

  @Override
  public int getNumBands() {
    return sum == null? 0 : sum.length;
  }

  @Override
  public void invalidate() {
    this.invalid = true;
  }

  @Override
  public boolean isValid() {
    return !this.invalid;
  }
}
