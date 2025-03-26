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
package edu.ucr.cs.bdlab.beast.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for classes that can run as command line operations. In addition to this metadata, operations must also
 * implement a static function {@code run(UserOptions)} and its full name must be added to the file
 * &lt;resources&gt;/operations.xml
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperationMetadata {
  /**
   * The short name used to access this operation from command line
   * @return a short name as a string that is preferably alphanumeric with no spaces or special characters.
   */
  String shortName();

  /**
   * A description of this operation
   * @return the description as a string
   */
  String description();

  /**
   * How many input files this operation can accept
   * @return the input arity in a string format as described in {@link OperationHelper#parseArity(String)}
   */
  String inputArity() default "1";

  /**
   * How many output paths this operation can accept
   * @return the output arity as a string format as described in {@link OperationHelper#parseArity(String)}.
   */
  String outputArity() default "1";

  /**
   * Optional list of classes to inherit their parameters
    * @return an array of classes from which parameters should be inherited.
   */
  Class<?>[] inheritParams() default {};
}
