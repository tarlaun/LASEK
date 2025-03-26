/*
 * Copyright 2021 University of California, Riverside
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

import org.geotools.util.Utilities;

public final class TiePoint {
  private double[] values = new double[6];

  public TiePoint() {
  }

  public TiePoint(double i, double j, double k, double x, double y, double z) {
    this.set(i, j, k, x, y, z);
  }

  public void set(double i, double j, double k, double x, double y, double z) {
    this.values[0] = i;
    this.values[1] = j;
    this.values[2] = k;
    this.values[3] = x;
    this.values[4] = y;
    this.values[5] = z;
  }

  public double getValueAt(int index) {
    if (index >= 0 && index <= 5) {
      return this.values[index];
    } else {
      throw new IllegalArgumentException("Provided index should be between 0 and 5");
    }
  }

  public double[] getData() {
    return (double[])this.values.clone();
  }

  public boolean isSet() {
    double[] var1 = this.values;
    int var2 = var1.length;

    for(int var3 = 0; var3 < var2; ++var3) {
      double val = var1[var3];
      if (!this.isComponentSet(val)) {
        return false;
      }
    }

    return true;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof TiePoint)) {
      return false;
    } else {
      TiePoint that = (TiePoint)obj;
      return Utilities.deepEquals(this.values, that.values);
    }
  }

  public int hashCode() {
    return Utilities.deepHashCode(this.values);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Tie point").append("\n");
    builder.append("\tRaster Space point (").append(this.values[0]).append(", ").append(this.values[1]).append(", ").append(this.values[2]).append(")").append("\n");
    builder.append("\tModel Space point (").append(this.values[3]).append(", ").append(this.values[4]).append(", ").append(this.values[5]).append(")");
    return builder.toString();
  }

  private boolean isComponentSet(double value) {
    return !Double.isInfinite(value) && !Double.isNaN(value) && Math.abs(value) > 1.0E-6D;
  }
}
