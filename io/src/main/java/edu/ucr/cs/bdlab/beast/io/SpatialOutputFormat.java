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

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.util.IConfigurable;
import edu.ucr.cs.bdlab.beast.util.OperationException;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Stack;

/**
 * Writes the output files as configured by the user
 */
public class SpatialOutputFormat implements IConfigurable {


  /**
   * Adds the FeatureWriter class assigned in the user options to the list of classes with parameters
   * @param opts user options
   * @param parameterClasses (output) the dependent classes will be added to this list
   */
  @Override
  public void addDependentClasses(BeastOptions opts, Stack<Class<?>> parameterClasses) {
    parameterClasses.push(SpatialWriter$.class);
  }

}
