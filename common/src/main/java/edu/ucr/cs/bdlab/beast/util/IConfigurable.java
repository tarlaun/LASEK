/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;

import java.util.Stack;

/**
 * Interface for objects that can be configured
 * @see OperationMetadata
 */
public interface IConfigurable {

  /**
   * Setup the operation before it runs. At this step, the job can modify the user options, if needed, and can throw
   * an exception of it cannot run.
   * @param opts the user-defined options to initialize to
   */
  default void setup(BeastOptions opts) {}

  /**
   * Add any dependent classes to inherit their parameters based on the user options.
   * This can be helpful when some parameters can only be determined based on some other user parameters.
   * For example, if a specific index or a specific input format supports any additional parameters.
   * @param opts the user-provided options for this class.
   * @param classes the list of classes to add
   */
  default void addDependentClasses(BeastOptions opts, Stack<Class<?>> classes) {}
}
