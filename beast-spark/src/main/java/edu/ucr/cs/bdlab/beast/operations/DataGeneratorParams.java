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
package edu.ucr.cs.bdlab.beast.operations;

import edu.ucr.cs.bdlab.beast.util.OperationParam;

public class DataGeneratorParams {

  @OperationParam(
      description = "Total size of generated data in bytes",
      required = true
  )
  public static final String Size = "size";

  @OperationParam(
      description = "Distribution name {uniform, dia, parcel}",
      required = true
  )
  public static final String Distribution = "dist";

  @OperationParam(
      description = "Number of dimensions",
      defaultValue = "2"
  )
  public static final String Dimensions = "dims";

  @OperationParam(
      description = "Type of geometries to generate {point, envelope}",
      required = false,
      defaultValue = "point"
  )
  public static final String Type = "type";

  @OperationParam(
      description = "Upper bound of geometry's size. If upper bound is 0, we create point",
      defaultValue = "0.001"
  )
  public static final String UpperBound = "upper_bound";

  @OperationParam(
      description = "Diagonal width only for diagonal distribution",
      defaultValue = "0.01"
  )
  public static final String DiagonalWidth = "dia_width";

  @OperationParam(
      description = "Seed for random number generation",
      defaultValue = "random"
  )
  public static final String RandomSeed = "seed";

}
