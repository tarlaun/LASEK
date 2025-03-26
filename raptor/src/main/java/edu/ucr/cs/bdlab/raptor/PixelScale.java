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

public final class PixelScale {
  private double scaleX = 0.0D / 0.0;
  private double scaleY = 0.0D / 0.0;
  private double scaleZ = 0.0D / 0.0;

  public boolean equals(Object that) {
    if (that == this) {
      return true;
    } else if (!(that instanceof PixelScale)) {
      return false;
    } else {
      PixelScale thatO = (PixelScale)that;
      return Utilities.equals(this.scaleX, thatO.scaleX) && Utilities.equals(this.scaleY, thatO.scaleY) && Utilities.equals(this.scaleZ, thatO.scaleZ);
    }
  }

  public int hashCode() {
    int hash = Utilities.hash(this.scaleX, 1);
    hash = Utilities.hash(this.scaleY, hash);
    hash = Utilities.hash(this.scaleZ, hash);
    return hash;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Pixel Scale").append("\n");
    buf.append("\tscaleX=").append(this.scaleX).append(" is set? ").append(this.isComponentSet(this.scaleX)).append("\n");
    buf.append("\tscaleY=").append(this.scaleY).append(" is set? ").append(this.isComponentSet(this.scaleY)).append("\n");
    buf.append("\tscaleZ=").append(this.scaleZ).append(" is set? ").append(this.isComponentSet(this.scaleZ)).append("\n");
    return buf.toString();
  }

  public PixelScale(double scaleX, double scaleY, double scaleZ) {
    this.scaleX = scaleX;
    this.scaleY = scaleY;
    this.scaleZ = scaleZ;
  }

  public PixelScale() {
    this.scaleX = 0.0D;
    this.scaleY = 0.0D;
    this.scaleZ = 0.0D;
  }

  public double getScaleX() {
    return this.scaleX;
  }

  public void setScaleX(double scaleX) {
    this.scaleX = scaleX;
  }

  public double getScaleY() {
    return this.scaleY;
  }

  public void setScaleY(double scaleY) {
    this.scaleY = scaleY;
  }

  public double getScaleZ() {
    return this.scaleZ;
  }

  public void setScaleZ(double scaleZ) {
    this.scaleZ = scaleZ;
  }

  public double[] getValues() {
    return new double[]{this.scaleX, this.scaleY, this.scaleZ};
  }

  public boolean isSet() {
    return this.isComponentSet(this.scaleX) && this.isComponentSet(this.scaleY);
  }

  public boolean isSetExtended() {
    return this.isComponentSet(this.scaleX) && this.isComponentSet(this.scaleY) && this.isComponentSet(this.scaleZ);
  }

  private boolean isComponentSet(double scale) {
    return !Double.isInfinite(scale) && !Double.isNaN(scale) && Math.abs(scale) > 1.0E-6D;
  }
}
