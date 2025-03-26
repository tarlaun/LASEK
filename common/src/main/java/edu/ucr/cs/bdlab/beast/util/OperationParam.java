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
 * Annotates names of parameters that can be set by the user.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperationParam {
  /**
   * A description to be written in the usage
   * @return the description or an empty string if no description is provided.
   */
  String description() default "";

  /**
   * Whether it is required or not. A required parameter should not have a default value.
   * @return {@code true} if this parameter is required.
   */
  boolean required() default false;

  /**
   * The default value
   * @return the default value for this parameter or an empty string if no default value is defined.
   */
  String defaultValue() default "";

  /**
   * Whether to show it in the usage or not
   * @return {@code true} if this operation parameter should be shown in the usage
   */
  boolean showInUsage() default true;
}
