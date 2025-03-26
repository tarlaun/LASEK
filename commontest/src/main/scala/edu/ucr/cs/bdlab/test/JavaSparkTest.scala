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
package edu.ucr.cs.bdlab.test

import junit.framework.TestCase
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.test.ScalaSparkTest

/**
 * Base class for JUnit tests that use Spark.
 */
abstract class JavaSparkTest extends TestCase with ScalaSparkTest {
  override def setUp(): Unit = beforeEach
  override def tearDown(): Unit = afterEach

  def javaSparkContext: JavaSparkContext = new JavaSparkContext(sparkContext)
}
