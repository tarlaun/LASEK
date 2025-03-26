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
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.DynamicArrays;
import org.locationtech.jts.geom.Envelope;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Encodes an envelope into a CSV representation as (minimum coordinates, maximum coordinates)
 * Each of them is a coordinate of a point as (x, y, z, ...).
 */
public class CSVEnvelopeEncoder implements BiFunction<IFeature, StringBuilder, StringBuilder> {

  /**Holds the dimensions to write in ascending order. Each position holds the index of the dimension to be written
   * at that position. For example, if {@code orderedColumns[0]=2}, it means that the first attribute to be written in
   * the CSV file is coordinate #2. A -1 indicates no dimension should be written there, i.e., an attribute.*/
  protected int[] orderedColumns;

  /**Field separator*/
  protected char separator;

  public CSVEnvelopeEncoder(char fieldSeparator, int[] coordColumns) {
    this.separator = fieldSeparator;
    this.orderedColumns = DynamicArrays.invertedIndex(coordColumns);
  }

  /**
   * An encoder that encodes only an envelope (not a feature) by writing all minimum coordinates then all maximum
   * coordinates with comma separator
   */
  public static final Function<Envelope, String> defaultEncoderJTS = e ->
    String.format("%f,%f,%f,%f", e.getMinX(), e.getMinY(), e.getMaxX(), e.getMaxY());

  @Override
  public StringBuilder apply(IFeature feature, StringBuilder str) {
    if (str == null)
      str = new StringBuilder();
    EnvelopeND e = feature.getGeometry() instanceof EnvelopeND? ((EnvelopeND) feature.getGeometry()) :
        new EnvelopeND(feature.getGeometry().getFactory()).merge(feature.getGeometry());
    int $a = 0; // The attribute that will be written next
    boolean first = true;
    for (int i : orderedColumns) {
      if (!first)
        str.append(separator);
      if (i == -1) {
        if ($a == feature.iGeom())
          $a++;
        CSVHelper.encodeValue(feature.get($a++), str);
      } else
        str.append(i < e.getCoordinateDimension()? e.getMinCoord(i) : e.getMaxCoord(i - e.getCoordinateDimension()));
      first = false;
    }
    while ($a < feature.length()) {
      if ($a != feature.iGeom()) {
        if (!first)
          str.append(separator);
        CSVHelper.encodeValue(feature.get($a), str);
        first = false;
      }
      $a++;
    }
    return str;
  }
}
