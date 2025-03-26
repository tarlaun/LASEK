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
package edu.ucr.cs.bdlab.beast.io.tiff;

import java.io.Serializable;

/**
 * An interface for a tile from a TIFF file.
 */
public interface ITiffTile extends Serializable {

  /**Tile width in pixels*/
  int getTileWidth();

  /**Tile height in pixels*/
  int getTileHeight();

  /**Number of samples per pixel, e.g., 3 for RGB images*/
  int getNumSamples();

  /**Get pixel value as long integer. This function concatenates all samples into one value.*/
  long getPixel(int iPixel, int jPixel);

  long getRawSampleValue(int iPixel, int jPixel, int iSample);

  int getSampleValueAsInt(int iPixel, int jPixel, int iSample);

  float getSampleValueAsFloat(int iPixel, int jPixel, int iSample);

  double getSampleValueAsDouble(int iPixel, int jPixel, int iSample);

  /**
   * Returns all the sample values at the given pixel location as integer. If these samples are stored in other formats,
   * e.g., byte or short, they are converted to integer with the same value. If the samples are represented as
   * floating-point numbers, they are rounded to the nearest integer.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param value the array of values to output to
   */
  default void getPixelSamplesAsInt(int iPixel, int jPixel, int[] value) {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < getNumSamples(); iSample++)
      value[iSample] = getSampleValueAsInt(iPixel, jPixel, iSample);
  }

  default void getPixelSamplesAsFloat(int iPixel, int jPixel, float[] value)  {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < getNumSamples(); iSample++)
      value[iSample] = getSampleValueAsFloat(iPixel, jPixel, iSample);
  }

  default void getPixelSamplesAsFloat(int iPixel, int jPixel, double[] value) {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < getNumSamples(); iSample++)
      value[iSample] = getSampleValueAsDouble(iPixel, jPixel, iSample);
  }

}
