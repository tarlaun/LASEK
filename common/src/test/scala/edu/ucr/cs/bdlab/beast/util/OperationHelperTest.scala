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

import org.apache.hadoop.io.IOUtils.NullOutputStream
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util
import scala.collection.immutable.Range

@RunWith(classOf[JUnitRunner])
class OperationHelperTest extends AnyFunSuite with ScalaSparkTest {
  test("get operation parameters") {
    val params = OperationHelper.getOperationParams(OperationHelper.operations("test"), null)
    assert(params.length == 2)
    assert(params.exists(_.name == "sparam"))

    val paramssub = OperationHelper.getOperationParams(OperationHelper.operations("subtest1"), null)
    assert(paramssub.length == 3)
    assert(paramssub.exists(_.name == "sparam"))
  }

  test("list operations") {
    val operations = OperationHelper.operations
    assert(operations.size == 5)
  }

  test("Parse arity") {
    assert(OperationHelper.parseArity("?") == Range.inclusive(0, 1))
    assert(OperationHelper.parseArity("*") == Range.inclusive(0, Int.MaxValue))
    assert(OperationHelper.parseArity("+") == Range.inclusive(1, Int.MaxValue))
    assert(OperationHelper.parseArity("4") == Range.inclusive(4, 4))
    assert(OperationHelper.parseArity("4").contains(4))
    assert(OperationHelper.parseArity("5-7") == Range.inclusive(5, 7))
  }

  test("Parse command line options") {
    val parsed = OperationHelper.parseCommandLineArguments("test", "path1",
      "option1:value1", "-option2", "-no-option3", "path2", "option4[0]:1", "-option4[1]")
    assert(parsed.operation.name == "test")
    assert(parsed.inputs.length == 1)
    assert(parsed.inputs(0) == "path1")
    assert(parsed.outputs.length == 1)
    assert(parsed.outputs(0) == "path2")
    assert(parsed.options.getBoolean("option2", defaultValue = false))
    assert(!parsed.options.getBoolean("option3", defaultValue = true))
    assert(parsed.options.getString("option1") == "value1")
    assert(parsed.options.getString("option4[0]") == "1")
    assert(parsed.options.getString("option4[1]") == "true")
  }

  test("Allow dash in the middle of a parameter name") {
    val parsed = OperationHelper.parseCommandLineArguments("test", "path1",
      "option1:value1", "-no-option-3", "-option-4", "file-name:abc")
    assert(parsed.inputs.length + parsed.outputs.length == 1)
    assert(parsed.options.getString("option1") == "value1")
    assert(!parsed.options.getBoolean("option-3", true))
    assert(parsed.options.getBoolean("option-4", false))
    assert(parsed.options.getString("file-name") == "abc")
  }

  test("Allow dot in the middle of a parameter name") {
    val parsed = OperationHelper.parseCommandLineArguments("test", "path1",
      "option.1:value1", "-no-option.3", "-option.4", "file.name:abc")
    assert(parsed.inputs.length + parsed.outputs.length == 1)
    assert(parsed.options.getString("option.1") == "value1")
    assert(!parsed.options.getBoolean("option.3", true))
    assert(parsed.options.getBoolean("option.4", false))
    assert(parsed.options.getString("file.name") == "abc")
  }

  test("Check user options") {
    // Unexpected parameters
    val commandLineOptions = OperationHelper.parseCommandLineArguments("test", "path1",
      "option1:value1", "-option2", "-no-option3", "path2", "option4[0]:1", "-option4[1]")
    assert(!OperationHelper.checkOptions(commandLineOptions, new PrintStream(new NullOutputStream)))
    // Correct!
    val parsedOptions2 = OperationHelper.parseCommandLineArguments("subtest1", "path1", "sparam2:1",
        "-sparam", "-no-param1[0]", "path2")
    assert(OperationHelper.checkOptions(parsedOptions2, new PrintStream(new NullOutputStream)))
    // Required parameter not found
    val parsedOptions3 = OperationHelper.parseCommandLineArguments("subtest1", "path1",
        "-sparam", "-no-param1[0]", "path2")
    assert(!OperationHelper.checkOptions(parsedOptions3, new PrintStream(new NullOutputStream)))
  }

  test("print usage") {
    val baos = new ByteArrayOutputStream()
    val printer: PrintStream = new PrintStream(baos)
    OperationHelper.printUsage(printer)
    printer.close()
    val str = new String(baos.toByteArray)
    assert(str.contains("subtest1"))

    val baos2 = new ByteArrayOutputStream()
    val printer2: PrintStream = new PrintStream(baos2)
    OperationHelper.printOperationUsage(OperationHelper.operations("subtest1"), null, printer2)
    printer2.close()
    val str2 = new String(baos2.toByteArray)
    assert(str2.contains("sparam2"))
  }

  test("xml read") {
    val conf: util.Map[String, util.List[String]] = OperationHelper.readConfigurationXML("test-beast.xml")
    assert(4 == conf.get("Operations").size)
    assert(1 == conf.get("Indexers").size)
    assert("Op1" == conf.get("Operations").get(0))
  }
}
