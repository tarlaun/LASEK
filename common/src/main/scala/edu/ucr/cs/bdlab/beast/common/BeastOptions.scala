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
package edu.ucr.cs.bdlab.beast.common

import com.fasterxml.jackson.core.JsonFactory
import edu.ucr.cs.bdlab.beast.common.BeastOptions.{defaultHadoopConf, defaultSparkConf}

import java.io._
import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils

import scala.language.implicitConversions

/**
 * Configurable options for Beast operations. A set of key-value pairs, both are Strings.
 * This is designed to include within in both Spark configuration and Hadoop configuration. Unlike Spark configuration,
 * which cannot be modified after the start of the SparkContext, Beast options can be easily changed at runtime.
 * For example, if a specific operation requires additional configuration that was not set when the Spark context
 * was started, this gives the user the opportunity to modify them.
 * Additionally, by including [[org.apache.spark.SparkConf]] and Hadoop [[Configuration]] within it, it allows
 * classes and methods to rely only on this one.
 */
class BeastOptions(loadDefaults: Boolean = true)
  extends scala.collection.mutable.HashMap[String, String] with Serializable {

  if (loadDefaults)
    this.mergeWith(BeastOptions.defaultOptions)

  /**
   * Copy constructor
   * @param bo the other BeastOptions to copy from
   */
  def this(bo: BeastOptions) {
    this(false)
    for (entry <- bo)
      this.put(entry._1, entry._2)
  }

  /**
   * Default constructor for Java
   */
  def this() {
    this(true)
  }

  /**
   * Initialize from Hadoop configuration
   * @param conf
   */
  def this(conf: Configuration) {
    this(false)
    val confIter = conf.iterator()
    while (confIter.hasNext) {
      val entry = confIter.next()
      this.put(entry.getKey, entry.getValue)
    }
  }

  /**
   * Set a key to any value by conerting it to string
   * @param key key name
   * @param value value
   * @return
   */
  def set(key: String, value: Any): BeastOptions = {
    this.put(key, value.toString)
    this
  }

  def getString(key: String, defaultValue: String): String = super.getOrElse(key, defaultValue)

  def getString(key: String): String = this.getString(key, null)

  /**
   * Get a value of a key as integer
   * @param key
   * @param defaultValue
   * @return
   */
  def getInt(key: String, defaultValue: Int): Int = super.getOrElse(key, defaultValue.toString).toInt

  /**
   * Set a key to an integer value
   * @param key
   * @param value
   * @return
   */
  def setInt(key: String, value: Int): BeastOptions = this.set(key, value.toString)

  /**
   * Get a key value as long
   * @param key
   * @param defaultValue
   * @return
   */
  def getLong(key: String, defaultValue: Long): Long = super.getOrElse(key, defaultValue.toString).toLong

  /**
   * Set a key to a long value
   * @param key
   * @param value
   * @return
   */
  def setLong(key: String, value: Long): BeastOptions = this.set(key, value.toString)

  def getDouble(key: String, defaultValue: Double): Double = super.getOrElse(key, defaultValue.toString).toDouble
  def getFloat(key: String, defaultValue: Float): Float = super.getOrElse(key, defaultValue.toString).toFloat

  /**
   * Get value as boolean
   * @param key
   * @param defaultValue
   * @return
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = super.getOrElse(key, defaultValue.toString).toBoolean

  /**
   * Set key to a boolean value
   * @param key
   * @param value
   * @return
   */
  def setBoolean(key: String, value: Boolean): BeastOptions = this.set(key, value.toString)

  /**
   * Get value of a key as a size, e.g., "1m" for 1 mega byte
   * @param key
   * @param defaultValue
   * @return
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long =
    JavaUtils.byteStringAsBytes(super.getOrElse(key, defaultValue))

  /**
   * Get value of a key as a size, e.g., "1m" for 1 mega byte
   * @param key
   * @param defaultValue
   * @return
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long =
    if (contains(key)) JavaUtils.byteStringAsBytes(super.get(key).get) else defaultValue

  /**
   * Keep only the parameters that do not have an index or the ones with the given index. In other words, remove any
   * indexed parameter that have a different index than the one given. The index of the parameter is a suffix
   * between square brackets, e.g., param[1]
   * @param index the index to retain
   * @return a new options with the given index retained
   */
  def retainIndex(index: Int): BeastOptions = {
    val suffix = s"[$index]"
    val newOpts = new BeastOptions(false)
    this.foreach(kv => {
      if (kv._1.indexOf('[') == -1)
        newOpts.put(kv._1, kv._2)
      else if (kv._1.endsWith(suffix))
        newOpts.put(kv._1.replace(suffix, ""), kv._2)
    })
    newOpts
  }

  def toProperties: Properties = {
    val prop = new Properties
    this.foreach(kv => prop.put(kv._1, kv._2))
    prop
  }

  /**
   * Stores the configuration as a .properties file; one entry per line.
   *
   * @param fs the file system to write to
   * @param p  the file path to write to.
   */
  def storeToTextFile(fs: FileSystem, p: Path): Unit = {
    val prop = this.toProperties
    val out = fs.create(p)
    val w = new OutputStreamWriter(out)
    try prop.store(w, "Beast options")
    finally w.close()
  }

  /**
   * Loads configuration back from a text file that was written using [[storeToTextFile()]]
   *
   * @param fs   the file system to write in
   * @param path the path to write to
   * @return the UserOptions instance that was read from the file.
   */
  def loadFromTextFile(fs: FileSystem, path: Path): BeastOptions = {
    val in = fs.open(path)
    try {
      loadFromTextFile(in)
    } finally in.close()
  }

  /**
   * Load all the options from the given properties file and add it to this options
   * @param in the input stream to read the BeastOptions from
   */
  def loadFromTextFile(in: InputStream): BeastOptions = {
    val r = new InputStreamReader(in)
    val p = new Properties
    p.load(r)
    val pi = p.entrySet().iterator()
    while (pi.hasNext) {
      val entry = pi.next()
      this.put(entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String])
    }
    this
  }

  def getEnumIgnoreCase[T <: Enum[T]](name: String, defaultValue: T): T = {
    val confValue: Option[String] = this.get(name)
    if (confValue.isEmpty) return defaultValue
    try {
      val valuesMethod = defaultValue.getDeclaringClass.getMethod("values")
      val values = valuesMethod.invoke(null).asInstanceOf[Array[T]]
      for (value <- values) {
        if (value.toString.equalsIgnoreCase(confValue.get)) return value
      }
    } catch {
      case e@(_: NoSuchMethodException | _: IllegalAccessException | _: InvocationTargetException) =>
        // Just ignore all errors and fall through to return the default value
        e.printStackTrace()
    }
    defaultValue
  }

  override def toString: String = {
    val str = new StringBuffer
    for (entry <- this) {
      if (entry._2 == "true") str.append("-" + entry._1)
      else if (entry._2 == "false") str.append("-no-" + entry._1)
      else {
        str.append(entry._1)
        str.append(":")
        str.append(entry._2)
      }
      str.append(" ")
    }
    str.toString
  }

  def toJson: String = {
    val jsonData = new ByteArrayOutputStream()
    val jsonGenerator = new JsonFactory().createGenerator(jsonData)
    jsonGenerator.writeStartObject()
    for (v <- this)
      jsonGenerator.writeStringField(v._1, v._2)
    jsonGenerator.writeEndObject()
    jsonGenerator.close()
    new String(jsonData.toByteArray)
  }

  /**
   * If multiple keys have the given name with different indexes, return all of them as an array
   * @param key the key to retrieve its values
   * @return an array of String values for the given key
   */
  def getValues(key: String): Array[String] = {
    var values = Seq[String]()
    for (p <- this) {
      if (p._1 == key || (p._1.startsWith(key) && p._1.indexOf('[') == key.length))
        values = values :+ p._2
    }
    values.toArray
  }

  def loadIntoHadoopConf(conf: Configuration = null): Configuration = {
    val hadoopConf: Configuration = if (conf == null) {
      val newConf = new Configuration(false)
      val defaultIter = defaultHadoopConf.iterator()
      while (defaultIter.hasNext) {
        val entry = defaultIter.next()
        newConf.set(entry.getKey, entry.getValue)
      }
      newConf
    } else {
      conf
    }
    for (entry <- this)
      hadoopConf.set(entry._1, entry._2)
    hadoopConf
  }

  def loadIntoSparkConf(conf: SparkConf = null): SparkConf = {
    val sparkConf: SparkConf = if (conf == null) {
      val newConf = new SparkConf(false)
      for (entry <- defaultSparkConf.getAll)
        newConf.set(entry._1, entry._2)
      newConf
    } else conf
    for ((k, v) <- this)
      sparkConf.set(k, v)
    sparkConf
  }

  def mergeWith(opts: BeastOptions): BeastOptions = {
    for ((k, v) <- opts)
      this.set(k, v)
    this
  }

  def getClass[U](name: String, defaultValue: Class[_ <: U], xface: Class[U]): Class[_ <: U] = try {
    val className = get(name)
    if (className.isEmpty)
      return defaultValue
    val theClass = Class.forName(className.get)
    if (theClass != null && !xface.isAssignableFrom(theClass)) throw new RuntimeException(theClass + " not " + xface.getName)
    else if (theClass != null) theClass.asSubclass(xface)
    else null
  } catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }

  def setClass(name: String, theClass: Class[_], xface: Class[_]): Unit = {
    if (!xface.isAssignableFrom(theClass)) throw new RuntimeException(theClass + " not " + xface.getName)
    set(name, theClass.getName)
  }
}

object BeastOptions {
  implicit def fromPairs(map: Iterable[(String, Any)]): BeastOptions = {
    val bo = new BeastOptions()
    for (entry <- map)
      bo.put(entry._1, entry._2.toString)
    bo
  }

  implicit def fromPair(entry: (String, Any)): BeastOptions = new BeastOptions().set(entry._1, entry._2.toString)

  implicit def fromMap(map: java.util.Map[String, String]): BeastOptions = {
    val bo = new BeastOptions()
    val entries = map.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      bo.put(entry.getKey, entry.getValue)
    }
    bo
  }

  /**
   * Create from a list of strings similar to command-line arguments which can have one of the following formats:
   * - name:value which translates to a parameter with a name and value
   * - -name which translates to a true Boolean value
   * - -no-name which translates to a false Boolean value
   * @param args
   * @return
   */
  def fromStrings(args: Iterable[String]): BeastOptions = {
    val options = new BeastOptions()
    val optionName = "((\\w+)(\\[\\d+\\])?)"
    val booleanTrueRegex = raw"-$optionName".r
    val booleanFalseRegex = raw"-no-$optionName".r
    val optionValue = raw"${optionName}:(.*)".r
    args.foreach {
      case booleanTrueRegex(nameNumber, name, number) => options.set(nameNumber, true)
      case booleanFalseRegex(nameNumber, name, number) => options.set(nameNumber, false)
      case optionValue(nameNumber, name, number, argvalue) => options.set(nameNumber, argvalue)
    }
    options
  }

  /**
   * Similar to [[fromStrings()]] but takes an array of strings rather than an iterable.
   * @param args the list of strings
   * @return
   */
  def fromStringArray(args: Array[String]): BeastOptions = fromStrings(args.toIterable)

  implicit def fromSparkConf(sparkConf: SparkConf): BeastOptions = fromPairs(sparkConf.getAll)

  lazy val defaultOptions: BeastOptions = {
    val opts = new BeastOptions(false)
    // Load defaults from beast.properties files
    val configFiles: java.util.Enumeration[URL] = getClass().getClassLoader.getResources("beast.properties")
    while (configFiles.hasMoreElements) {
      val configFile: URL = configFiles.nextElement
      val input: InputStream = configFile.openStream
      try opts.loadFromTextFile(input)
      finally {
        if (input != null) input.close()
      }
    }
    // Try to also load from the working directory
    val localfile: File = new File("beast.properties")
    if (localfile.exists && localfile.isFile) {
      val input: FileInputStream = new FileInputStream(localfile)
      try opts.loadFromTextFile(input)
      finally {
        if (input != null) input.close()
      }
    }
    opts
  }

  lazy val defaultHadoopConf: Configuration = new Configuration()

  lazy val defaultSparkConf: SparkConf = new SparkConf()
}