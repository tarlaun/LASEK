/*
 * Copyright 2023 University of California, Riverside
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

import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession
import org.apache.spark.test.ScalaSparkTest

/**
 * A mixin for Scala tests that creates a Spark context and adds methods to create an empty scratch directory
 */
trait BeastSpatialTest extends ScalaSparkTest {

  override def sparkSession: SparkSession = {
    val session = super.sparkSession
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(session)
    session
  }
}
