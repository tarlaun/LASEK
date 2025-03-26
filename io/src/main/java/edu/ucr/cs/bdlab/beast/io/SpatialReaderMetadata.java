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
package edu.ucr.cs.bdlab.beast.io;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Metadata for spatial input readers (both vector and raster)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpatialReaderMetadata {
  /**
   * An optional description of this format
   *
   * @return the description
   */
  String description() default "";

  /**
   * An optional extension that can be used to automatically detect file format
   *
   * @return the extension including the dot
   */
  String extension() default "";

  /**
   * A short name that can be assigned from command line
   *
   * @return a short name
   */
  String shortName();

  /**
   * An optional filter to limit the files to read. By default, all non-hidden files are read.
   * If multiple filters are needed, they can be separated by a new line.
   *
   * @return a Linux-based wildcard expression for filename, e.g., *.csv
   */
  String filter() default "";

  /**
   * Whether files should be split or not.
   *
   * @return {@code true} if the files cannot be split.
   */
  boolean noSplit() default false;
}
