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

import java.io.{IOException, PrintStream}

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import javax.xml.parsers.{DocumentBuilderFactory, ParserConfigurationException}
import org.apache.spark.internal.Logging
import org.w3c.dom.Node
import org.xml.sax.SAXException

import scala.annotation.varargs
import scala.collection.immutable.Range

/**
 * A helper object for running operations
 */
object OperationHelper extends Logging {

  /**Information about a parameters that can be passed to an operation*/
  case class OperationParamInfo(name: String, metadata: OperationParam)

  /**Information about a command line operation*/
  case class Operation(name: String, klass: Class[_ <: CLIOperation], metadata: OperationMetadata)

  /**Parsed command line options*/
  case class ParsedCommandLineOptions(operation: Operation,
                                      inputs: Array[String],
                                      outputs: Array[String],
                                      options: BeastOptions,
                                      declaredParameters: Array[String])

  val configurations = new java.util.HashMap[String, java.util.Map[String, java.util.List[String]]]

  /**
   * Read all XML configuration files of the given name in the class path and merge them into one object.
   * This method internally caches the configuration so it does not have to be loaded multiple times.
   * The XML is organized in three levels. The first level is the root element and it is always
   * &lt;beast&gt;. The second level is a name of a collection, e.g., &lt;Indexers&gt;. Finally,
   * the third level contains the contents of the collection in their text part.
   *
   * @param filename A path to an XML file that contains the configuration.
   * @return the beast configuration as a map from each key to all values under this key.
   */
  def readConfigurationXML(filename: String): java.util.Map[String, java.util.List[String]] = { // We chose to load configuration from XML rather than YAML for two reasons
    // 1- It is natively supported in Java so it will reduce the dependencies.
    // 2- We can use maven-shade-plugin to merge the XML files while packaging the final version
    // We could not use .properties file as they do not support arrays
    // If the configuration is cached, return it
    if (configurations.containsKey(filename)) return configurations.get(filename)
    // Otherwise, load it for the first time and cache it
    try {
      var configuration: java.util.Map[String, java.util.List[String]] = new java.util.HashMap[String, java.util.List[String]]
      val configFiles = getClass.getClassLoader.getResources(filename)
      val dbFactory = DocumentBuilderFactory.newInstance
      val dBuilder = dbFactory.newDocumentBuilder
      while ( {
        configFiles.hasMoreElements
      }) {
        val configFile = configFiles.nextElement
        val configIS = configFile.openStream
        val doc = dBuilder.parse(configIS)
        val documentElement = doc.getDocumentElement
        documentElement.normalize()
        val collections = documentElement.getChildNodes
        for ($i <- 0 until collections.getLength) {
          val collection = collections.item($i)
          if (collection.getNodeType == Node.ELEMENT_NODE) {
            val collectionName = collection.getNodeName
            var collectionItems: java.util.List[String] = configuration.get(collectionName)
            if (collectionItems == null) {
              collectionItems = new java.util.ArrayList[String]
              configuration.put(collectionName, collectionItems)
            }
            val collectionContents = collection.getChildNodes
            for ($j <- 0 until collectionContents.getLength) {
              val collectionContentItem = collectionContents.item($j)
              if (collectionContentItem.getNodeType == Node.ELEMENT_NODE) {
                val itemValue = collectionContentItem.getTextContent
                collectionItems.add(itemValue)
              }
            }
          }
        }
        configIS.close()
      }
      // Check the cache again for the rare case of a concurrent call to this method with the same filename
      configurations.synchronized {
        if (configurations.containsKey(filename))
          configuration = configurations.get(filename)
        else
          configurations.put(filename, configuration)
      }
      configuration
    } catch {
      case e@(_: IOException | _: ParserConfigurationException | _: SAXException) =>
        throw new RuntimeException("Error loading configuration file", e)
    }
  }

  /**
   * Load the list of operations from the XML configuration file.
   *
   * @return a map from each user-friendly short name of an operation to an instance of {@link Operation}
   */
  lazy val operations: Map[String, Operation] = {
    val ops = new scala.collection.mutable.TreeMap[String, Operation]()
    val opClassNames = readConfigurationXML("beast.xml").get("Operations")
    if (opClassNames != null) {
      val opClassNamesIter = opClassNames.iterator()
      while (opClassNamesIter.hasNext) {
        val opClassName = opClassNamesIter.next
        var opClass: Class[_ <: CLIOperation] = null
        try {
          try // Try as a Scala operation first
            opClass = Class.forName(opClassName + "$").asSubclass(classOf[CLIOperation])
          catch {
            // Try as a Java class
            case _: ClassNotFoundException => opClass = Class.forName(opClassName).asSubclass(classOf[CLIOperation])
          }
          val opMetadata = opClass.getAnnotation(classOf[OperationMetadata])
          if (opMetadata != null)
            ops.put(opMetadata.shortName.toLowerCase, Operation(opMetadata.shortName.toLowerCase, opClass, opMetadata))
          else
            logWarning(s"Skipping class $opClassName because it is not annotated with @OperationMetadata")
        } catch {
          case e: ClassNotFoundException =>
            e.printStackTrace()
        }
      }
    }
    ops.toMap
  }

  /**
   * Returns a class and the associated object while handling Java/Scala differences
   * @param klass
   * @return
   */
  private def getClassAndObject(klass: Class[_]): (Class[_], Object) = {
    var retClass: Class[_] = klass
    var klassObj: Object = null
    // Check if this is a Scala object and get the corresponding class
    try {
      retClass = Class.forName(klass.getName + "$")
    } catch {
      case _: Exception => // It is a regular Java class
    }
    try {
      // Get the declared Scala object
      klassObj = retClass.getField("MODULE$").get(klass)
    } catch {
      case _: Exception => klassObj = retClass
    }
    (retClass, klassObj)
  }

  def getDependentClasses(operation: Operation, opts: BeastOptions): Array[Class[_]] = {
    val classesToCheck = new java.util.Stack[Class[_]]()
    classesToCheck.push(operation.klass)
    var classesToReturn = Seq[Class[_]]()
    while (!classesToCheck.isEmpty) {
      val (klass: Class[_], klassObj: Object) = getClassAndObject(classesToCheck.pop())
      classesToReturn = classesToReturn :+ klass
      // 1- Add all inherited classes defined in the annotation
      val opMetadata: OperationMetadata = klass.getAnnotation(classOf[OperationMetadata])
      if (opMetadata != null) {
        for (subclass <- opMetadata.inheritParams) {
          classesToCheck.push(subclass)
        }
      }
      // 2- Add dependent classes defined programmatically
      if (klassObj.isInstanceOf[IConfigurable]) { // A Scala class
        klassObj.asInstanceOf[IConfigurable].addDependentClasses(opts, classesToCheck)
      } else  if (classOf[IConfigurable].isAssignableFrom(klass)) { // A Java class
        val operation = klass.asSubclass(classOf[IConfigurable]).newInstance
        operation.addDependentClasses(opts, classesToCheck)
      }
    }
    classesToReturn.toArray.distinct
  }

  /**
   * Returns all parameters that are allowed for the given operation. Operation parameters are all parameters
   * annotated with [[OperationParam]] that appear in one of the following:
   * - In the class associated with the given operation
   * - In any additional classes defined in the [[OperationMetadata]] annotation on the class
   * - In any classes that are added through the method [[IConfigurable]].addDependentClasses
   * @param operation the operation in question
   * @param opts any additional user options. This is used to add dependent classes if they depend on some user choice.
   *             For example, if the user selects a specific indexer, it can be used to add that specific indexer
   *             as a dependent class
   * @return an array of parameters that are allowed
   */
  def getOperationParams(operation: Operation, opts: BeastOptions): Array[OperationParamInfo] = {
    var paramInfo = List[OperationParamInfo]()
    for (kklass <- getDependentClasses(operation, opts)) {
      val (klass: Class[_], klassObj: Object) = getClassAndObject(kklass)
      // 1- Get annotations declared in the class itself
      for (f <- klass.getDeclaredFields) {
        val annotation = f.getAnnotation(classOf[OperationParam])
        if (annotation != null) {
          f.setAccessible(true)
          val name = f.get(klassObj).asInstanceOf[String]
          paramInfo = OperationParamInfo(name, annotation) :: paramInfo
        }
      }
    }
    paramInfo.toArray
  }

  def parseArity(arity: String): Range = {
    val intRegexp = raw"\d+".r
    val rangeRegexp = raw"(\d+)-(\d+)".r
    arity match {
      case "*" => Range.inclusive(0, Int.MaxValue)
      case "+" => Range.inclusive(1, Int.MaxValue)
      case "?" => Range.inclusive(0, 1)
      case intRegexp(_*) => Range.inclusive(arity.toInt, arity.toInt)
      case rangeRegexp(from, to) => Range.inclusive(from.toInt, to.toInt)
      case _ => throw new RuntimeException(s"Unsupported range format '$arity'")
    }
  }

  /**
   * Parses command line arguments and extracts the user operation, inputs, outputs, and additional options
   * @param args the command line arguments where the first one should be the operation short name
   * @return a tuple that contains, in order, the operation, the inputs, the output, and the options
   */
  @varargs
  def parseCommandLineArguments(args: String*): ParsedCommandLineOptions = {
    val operation = operations.get(args(0))
    if (operation.isEmpty) {
      // Invalid operation name. Make some suggestions and return null
      val suggestions = getSuggestions(args(0), operations.keys)
      System.err.print(s"Unrecognized operation '${args(0)}'.")
      if (suggestions.nonEmpty)
        System.err.print(s" Did you mean [${suggestions.mkString(", " )}]")
      System.err.println()
      return null
    }
    // Treat all the remaining ones as options or input/output paths
    var inout = Seq[String]()
    var declaredParameters = Seq[String]()
    val options = new BeastOptions()
    val optionName = "((\\w[\\w\\-\\.]*)(\\[\\d+\\])?)"
    val booleanTrueRegex = raw"-$optionName".r
    val booleanFalseRegex = raw"-no-$optionName".r
    val optionValue = raw"$optionName:([^/\\].*)".r
    args.slice(1, args.size).foreach {
      case booleanFalseRegex(nameNumber, name, number) => declaredParameters = declaredParameters :+ name; options.set(nameNumber, false)
      case booleanTrueRegex(nameNumber, name, number) => declaredParameters = declaredParameters :+ name; options.set(nameNumber, true)
      case optionValue(nameNumber, name, number, argvalue) => declaredParameters = declaredParameters :+ name; options.set(nameNumber, argvalue)
      case other => inout = inout :+ other
    }
    val inputarity: Range = parseArity(operation.get.metadata.inputArity())
    val outputarity: Range = parseArity(operation.get.metadata.outputArity())
    val numInputs: Int = (inout.length - outputarity.start) min inputarity.end max inputarity.start
    val inputs = inout.slice(0, numInputs).toArray
    val outputs = inout.slice(numInputs, inout.length).toArray
    ParsedCommandLineOptions(operation.get, inputs, outputs, options, declaredParameters.toArray)
  }

  /**
   * Returns a list of suggestions for the given string from the given list of options base don edit distance
   * @param str a string given by the user
   * @param options a list of allowed options
   * @return the list of options that have less than 2 edit distance to the given string in no particular order
   */
  def getSuggestions(str: String, options: Iterable[String]): Iterable[String] =
    options.filter(option => StringUtil.levenshteinDistance(str, option) <= 2)

  /**
   * Check if the user options are valid. This means that the user did not add any unexpected options
   * or leave out any required option
   * @param options parsed command line options.
   * @return
   */
  def checkOptions(options: ParsedCommandLineOptions, out: PrintStream): Boolean = {
    val inputarity: Range = parseArity(options.operation.metadata.inputArity())
    val outputarity: Range = parseArity(options.operation.metadata.outputArity())
    if (options.inputs.length < inputarity.start)
      out.println(s"Too few inputs. Found ${options.inputs.length} expected at least ${inputarity.start}")
    if (options.inputs.length > inputarity.end)
      out.println(s"Too many inputs. Found ${options.inputs.length} expected at most ${inputarity.end}")
    if (options.outputs.length < outputarity.start)
      out.println(s"Too few outputs. Found ${options.outputs.length} expected at least ${outputarity.start}")
    if (options.outputs.length > outputarity.end)
      out.println(s"Too many outputs. Found ${options.outputs.length} expected at most ${outputarity.end}")
    val allAllowedParameters: Array[OperationParamInfo] = getOperationParams(options.operation, options.options)
    val requiredParameters: Array[String] = allAllowedParameters.filter(_.metadata.required()).map(_.name)
    val missingParameters = requiredParameters diff options.declaredParameters
    if (missingParameters.nonEmpty)
      out.println(s"Required parameters not found [${missingParameters.mkString(", ")}]")
    val extraParameters = options.declaredParameters.distinct diff allAllowedParameters.map(_.name)
    if (extraParameters.nonEmpty)
      out.println(s"Unrecognized parameters found [${extraParameters.mkString(", ")}]")
    inputarity.contains(options.inputs.length) &&
      outputarity.contains(options.outputs.length) &&
      missingParameters.isEmpty && extraParameters.isEmpty
  }

  /**
   * Print the usage by listing all the supported operations.
   */
  def printUsage(out: PrintStream): Unit = {
    out.println("****************************************************")
    out.println("Choose one of the following operations to run")
    for (operationName <- operations.keys.toArray.sorted)
      out.printf("%s - %s\n", operationName, operations(operationName).metadata.description)
    out.println("****************************************************")
  }

  /**
   * Prints the usage of a specific operation.
   *
   * @param operation the operation to print the usage to
   * @param out the print stream to write to
   */
  def printOperationUsage(operation: Operation, options: BeastOptions, out: PrintStream): Unit = {
    out.println("****************************************************")
    out.printf("Usage: %s", operation.metadata.shortName)
    val inputArity = parseArity(operation.metadata.inputArity)
    if (inputArity.start == 0 && inputArity.`end` == 1) out.print(" [input]")
    else if (inputArity.start == 1 && inputArity.end == 1) out.print(" <input>")
    else if (inputArity.start == 0 && inputArity.end > 1) out.print(" [inputs]")
    else if (inputArity.start >= 1 && inputArity.end > 1) out.print(" <inputs>")
    val outputArity = parseArity(operation.metadata.outputArity)
    if (outputArity.start == 0 && outputArity.end == 1) out.print(" [output]")
    else if (outputArity.start == 1 && outputArity.end == 1) out.print(" <output>")
    else if (outputArity.start == 0 && outputArity.end > 1) out.print(" [outputs]")
    else if (outputArity.start >= 1 && outputArity.end > 1) out.print(" <outputs>")
    out.println()
    // Retrieve the options and parameters
    out.println(" -- Additional parameters. (*) Marks required parameters")
    printUsageFields(out, getOperationParams(operation, options))
    // Call an optional printUsage method in all the dependent classes
    val dependentClasses: Array[Class[_]] = getDependentClasses(operation, options)
    for (opClass <- dependentClasses) {
      try {
        val op: CLIOperation = opClass.newInstance().asInstanceOf[CLIOperation]
        op.printUsage(out)
      } catch {
        case _: Exception => // No problem if we could not print the usage
      }
    }
    out.println("****************************************************")
  }

  /**
   * Print usage for all the given fields.
   *
   * @param out    the print stream to write to
   * @param fields the list of fields to write their usage details.
   */
  def printUsageFields(out: PrintStream, fields: Array[OperationParamInfo]): Unit = {
    for (field <- fields) {
      if (field.metadata.showInUsage) { // This field indicates an operation parameter
        if (field.metadata.required)
          out.print("(*) ")
        out.print(field.name)
        out.print(": ")
        out.print(field.metadata.description)
        if (field.metadata.defaultValue != null && field.metadata.defaultValue.length > 0)
          out.printf(" (Default: %s)", field.metadata.defaultValue)
        out.println()
      }
    }
  }

}
