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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;

import java.util.function.BiFunction;

/**
 * A function that parses a CSV representation of envelopes represented as (minimum coordinates, maximum coordinates)
 * Each of them is a coordinate of a point as (x, y, z, ...).
 */
public class CSVEnvelopeDecoder implements BiFunction<String, EnvelopeND, EnvelopeND> {

  private CSVEnvelopeDecoder() {}

  public static final CSVEnvelopeDecoder instance = new CSVEnvelopeDecoder();

  @Override
  public EnvelopeND apply(String s, EnvelopeND env) {
    String[] parts = s.split(",");
    assert (parts.length % 2) == 0;
    int numDimensions = parts.length / 2;
    if (env == null)
      env = new EnvelopeND(GeometryReader.DefaultInstance.getGeometryFactory(), numDimensions);
    else
      env.setCoordinateDimension(numDimensions);
    for (int d$ = 0; d$ < numDimensions; d$++) {
      env.setMinCoord(d$, Double.parseDouble(parts[d$]));
      env.setMaxCoord(d$, Double.parseDouble(parts[numDimensions + d$]));
    }
    return env;
  }
}
