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
package edu.ucr.cs.bdlab.beast.util

import java.util

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import org.apache.spark.SparkContext

@OperationMetadata(
  shortName = "subtest1",
  description = "..."
)
object TestSubClassScala extends CLIOperation {
  @OperationParam(description = "description", defaultValue = "false", required = true) val ScalaParam = "sparam2"

  override def addDependentClasses(opts: BeastOptions, classes: util.Stack[Class[_]]): Unit = {
    classes.push(TestBaseClassScala.getClass)
  }

  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = ???
}
