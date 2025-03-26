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
package org.apache.spark.test

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.awt.Color
import java.awt.image.BufferedImage
import java.io._
import scala.collection.mutable

/**
  * A mixin for Scala tests that creates a Spark context and adds methods to create an empty scratch directory
  */
trait ScalaSparkTest extends Suite with BeforeAndAfterEach with Logging {
  System.setProperty("org.geotools.referencing.forceXY", "true")

  // Initialization and resetting of a common spark context
  def sparkContext: SparkContext = {
    sparkSession.sparkContext
  }

  def sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("test")
      .config(defaultConf)
      .getOrCreate()
  }

  def closeSC(): Unit = {
    sparkSession.stop()
  }

  /**A scratch directory that gets deleted before and after each test*/
  var _scratchDir: File = _

  def scratchPath: Path = new Path(_scratchDir.getPath)
  def scratchDir: File = _scratchDir

  def clearScratchDir: Unit = {
    if (_scratchDir != null) {
      if (!FileUtil.fullyDelete(_scratchDir))
        logWarning(s"Could not delete temporary directory '${_scratchDir}'")
    }
  }

  def resetScratchDir: Unit = {
    clearScratchDir
    // Try to reuse existing scratch directory, if possible
    if (_scratchDir == null || _scratchDir.exists()) {
      // This indicates that deleting the scratch directory failed or that the scratch directory is still null
      _scratchDir = File.createTempFile("beast-scratch", null)
      if (_scratchDir.exists())
        _scratchDir.delete()
      _scratchDir.mkdirs
      _scratchDir.deleteOnExit()
    } else {
      // Just recreate it as an empty directory
      _scratchDir.mkdirs()
    }
  }

  def resetSparkConfiguration: Unit = {
    if (SparkSession.getActiveSession.nonEmpty || SparkSession.getDefaultSession.nonEmpty) {
      val sparkConf: SparkConf = sparkContext.conf
      sparkContext.hadoopConfiguration.clear()
      for (entry <- sparkConf.getAll) {
        if (entry._1 != "spark.app.id" && entry._1 != "spark.driver.host")
          sparkConf.remove(entry._1)
      }
      for (entry <- new SparkConf().setMaster("local").setAppName("test").getAll)
        sparkConf.set(entry._1, entry._2)
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      additionalSparkConf(sparkConf)
    }
  }

  def defaultConf: SparkConf = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    additionalSparkConf(conf)
    conf
  }

  /**
   * Optional, allow tests to add additional spark configuration.
   * @param conf
   */
  def additionalSparkConf(conf: SparkConf): Unit = {}

  override protected def beforeEach(): Unit = {
    resetScratchDir
    resetSparkConfiguration
  }

  override protected def afterEach(): Unit = clearScratchDir

  def shouldRunStressTest(): Boolean = {
    System.getProperty("stressTest") != null ||
      System.getProperty("sun.java.command") != null &&
        System.getProperty("sun.java.command").contains(this.getClass.getSimpleName)
  }

  // Read text files in different ways

  /**
   * Reads up-to maxLines from the given input stream
   *
   * @param is       the input stream to read from
   * @param maxLines the upper bound of the number of lines to read
   * @return the lines as an array of Strings
   */
  def getLines(is: InputStream, maxLines: Int = Int.MaxValue): Array[String] = {
    val reader: LineReader = new LineReader(is)
    val line: Text = new Text
    var lines = List[String]()
    while (lines.length < maxLines && reader.readLine(line) > 0)
      lines = lines :+ line.toString
    lines.toArray
  }

  /**
   * Read the first n lines from the given resource and return those lines as an array of Strings.
   * If the given upper bound is bigger than the input file, the entire input file is loaded and returned.
   * Therefore, the returned array might be smaller than the given upper bound if the file is smaller.
   *
   * @param resourcePath the path to the resource to read
   * @param maxLines     the upper bound of the number of lines to read
   * @return an array of strings containing the lines read from the input file
   */
  def readTextResource(resourcePath: String, maxLines: Int): Array[String] = {
    var is: BufferedInputStream = null
    try {
      is = new BufferedInputStream(getClass.getResourceAsStream(resourcePath))
      getLines(is, maxLines)
    } finally if (is != null) is.close()
  }

  def readTextResource(resourcePath: String): Array[String] = readTextResource(resourcePath, Int.MaxValue)


  /**
   * Read a text file as a single big string.
   *
   * @param filename the name (or path) of the file
   * @return the contents of the file as one big String.
   */
  def readFile(filename: String): Array[String] = {
    var is: InputStream = null
    try {
      is = new BufferedInputStream(new FileInputStream(filename))
      getLines(is, Int.MaxValue)
    } finally if (is != null) is.close()
  }

  /**
   * Reads a CSV file that contains only numbers and returns the result as a two-dimensional array in a column format.
   * That is, if the input contains {@code n} lines and each line contains {@code d} columns, the return value is an
   * array of size {@code d} where each entry is an array of size {@code n}.
   *
   * @param resourcePath the path of the resource to read, typically under src/main/resources
   * @param maxLines     the maximum number of lines to read
   * @return a two-dimensional array where the first index is the dimension (column) and the second dimension is
   *         the line number (row)
   */
  def readCoordsResource(resourcePath: String, maxLines: Int): Array[Array[Double]] = {
    val lines = readTextResource(resourcePath, maxLines)
    val numDimensions = lines(0).split(",").length
    val coords = Array.ofDim[Double](numDimensions, lines.length)
    for (i <- lines.indices) {
      val parts = lines(i).split(",")
      assert(numDimensions == parts.length,
        s"Number of dimensions differ ${parts.length} <> ${numDimensions} in line #$i")
      for (dim <- 0 until numDimensions) {
        coords(dim)(i) = parts(dim).toDouble
      }
    }
    coords
  }

  def readCoordsResource(resourcePath: String): Array[Array[Double]] = readCoordsResource(resourcePath, Int.MaxValue)


  /**
   * Read a resource as a raw byte array
   *
   * @param resourcePath the path to the resource
   * @return the contents of that resource as a byte array
   */
  def readResourceData(resourcePath: String): Array[Byte] = {
    val in = getClass.getResourceAsStream(resourcePath)
    try {
      var totalSize = 0
      var buffers = Seq[Array[Byte]]()
      var eofReached = false
      do {
        var buffer = new Array[Byte](4096)
        val bufferSize = in.read(buffer)
        if (bufferSize > 0) {
          if (bufferSize != buffer.length)
            buffer = buffer.slice(0, bufferSize)
          buffers = buffers :+ buffer
          totalSize += bufferSize
        }
        eofReached = bufferSize == -1
      } while ( {
        !eofReached
      })
      val allData = new Array[Byte](totalSize)
      var offset = 0
      for (buffer <- buffers) {
        System.arraycopy(buffer, 0, allData, offset, buffer.length)
        offset += buffer.length
      }
      assert(offset == totalSize)
      allData
    } finally in.close()
  }

  // Make copies of files for test

  /**
   * Copy a resource to a temporary file to allow reading it as a file.
   *
   * @param resourcePath the path of the resource to read
   * @param filePath     the path of the file to write
   * @param overwrite    set this flag to automatically overwrite the output file.
   */
  def copyResource(resourcePath: String, filePath: File, overwrite: Boolean): Unit = {
    if (!overwrite && filePath.exists) fail("Cannot overwrite an existing file " + filePath)
    // Create directory
    if (!new File(filePath.getParent).exists) new File(filePath.getParent).mkdirs
    var in: InputStream = null
    var out: OutputStream = null
    try {
      val buffer = new Array[Byte](1024 * 1024)
      in = getClass.getResourceAsStream(resourcePath.replaceAll("\\\\", "/"))
      out = new FileOutputStream(filePath)
      var bufferLength: Int = 0
      do {
        bufferLength = in.read(buffer, 0, buffer.length)
        if (bufferLength > 0)
          out.write(buffer, 0, bufferLength)
      } while(bufferLength > 0)
    } finally {
      if (in != null) in.close()
      if (out != null) out.close()
    }
  }

  def copyResource(resourcePath: String, filePath: File): Unit =
    copyResource(resourcePath, filePath, false)

  def makeFileCopy(resourcePath: String): File = makeResourceCopy(resourcePath)

  def makeDirCopy(resourcePath: String): File = makeResourceCopy(resourcePath)

  /**
   * Make a copy of the given resource into the scratch directory and returns the path of the copy.
   * @param srcPath the path (in the resource) to copy
   * @return the path of the copy
   */
  @deprecated("Use locateResource instead", "0.9.4")
  def makeResourceCopy(srcPath: String): File = locateResource(srcPath)

  /**
   * Returns the relative or full path to the given resource
   * @param srcPath a path to a resource that starts with "/"
   * @return the path of the file of the given resource
   */
  def locateResource(srcPath: String): File =
    new File(this.getClass.getClassLoader.getResource(srcPath.substring(1)).getFile)

  def makeResourceCopy(srcPath: String, dirPath: File): Unit = {
    val loader = this.getClass.getClassLoader
    val toCopy = new mutable.ArrayBuffer[(String, File)]()
    toCopy.append((srcPath, dirPath))
    while (toCopy.nonEmpty) {
      val (src: String, dst: File) = toCopy.remove(toCopy.length - 1)
      // Note: we replace Windows path separator (back slash) with Linux separator (forward slash) because this is
      // what the loader.getResource function understands
      val srcFile = new File(loader.getResource(
        src.substring(1).replaceAll("\\\\", "/")).getFile)
      if (srcFile.isFile) {
        // Copy a file
        copyResource(src, dst)
      } else {
        // Copy a directory
        // List files and copy each one
        val subfiles = srcFile.listFiles()
        if (subfiles != null) {
          for (subfile <- subfiles) {
            val dstDir = new File(dst, subfile.getName)
            dstDir.mkdir()
            toCopy.append((new File(src, subfile.getName).getPath, dstDir))
          }
        }
      }
    }
  }

  /**
   * Read all non-empty files in the given directory as an array of strings, one for each line.
   * @param dir the directory to read
   * @return one string array that combines the contents of all files.
   */
  def readFilesInDirAsLines(dir: File) : Array[String] = {
    var lines = List[String]()
    dir.listFiles()
      .filterNot(p => p.getName.startsWith(".") || p.getName.startsWith("_"))
      .foreach(f => {
        if (f.length() > 0) {
          lines = List.concat(lines, readFile(f.getPath).toList)
        }
      })
    lines.toArray
  }

  // New assertions

  /**
   * Tests the contents of two input stream for equality. The bytes are consumed from both streams while testing them.
   *
   * @param expected the correct (expected) stream
   * @param actual   the stream produced from the code
   * @throws IOException if an error happens while reading one of the input stream
   */
  @throws[IOException]
  def assertEquals(expected: InputStream, actual: InputStream): Unit = { // The last offset that was compared
    var offsetCompared = 0
    val expectedBuffer = new Array[Byte](1024)
    val actualBuffer = new Array[Byte](1024)
    var expectedLength = 0
    var actualLength = 0
    while ( {
      true
    }) {
      var readLength = expected.read(expectedBuffer, expectedLength, expectedBuffer.length - expectedLength)
      if (readLength > 0) expectedLength += readLength
      readLength = actual.read(actualBuffer, actualLength, actualBuffer.length - actualLength)
      if (readLength > 0) actualLength += readLength
      if ((expectedLength == 0) ^ (actualLength == 0)) fail("File lengths not equal!")
      if (expectedLength == 0 && actualLength == 0) return
      for (i <- 0 until Math.min(expectedLength, actualLength)) {
        assert(expectedBuffer(i) == actualBuffer(i), "Contents differ at byte: " + offsetCompared)
        offsetCompared += 1
      }
      // Remove the compared bytes from the buffers
      if (expectedLength < actualLength) {
        System.arraycopy(actualBuffer, expectedLength, actualBuffer, 0, actualLength - expectedLength)
        actualLength -= expectedLength
        expectedLength = 0
      }
      else {
        System.arraycopy(expectedBuffer, actualLength, expectedBuffer, 0, expectedLength - actualLength)
        expectedLength -= actualLength
        actualLength = 0
      }
    }
  }

  def assertContentsEqual(file1: File, file2: File): Unit = {
    val in1 = new FileInputStream(file1)
    val in2 = new FileInputStream(file2)
    try {
      assertEquals(in1, in2)
    } finally {
      in1.close()
      in2.close()
    }
  }

  /**
   * Tests two arrays for equality
   *
   * @param expected the expected array
   * @param actual   the actual array produced by the code
   */
  def assertArrayEqualsGeneric(expected: Array[Any], actual: Array[Any]): Unit = {
    assert(expected.length == actual.length,
      s"Array lengths differ. Expected ${expected.length} but found ${actual.length}")
    for (i <- expected.indices)
      assertResult(expected(i), s"The arrays differ at position $i")(actual(i))
  }

  def assertArrayEquals(expected: Array[Byte], actual: Array[Byte]): Unit = {
    assert(actual.length == expected.length,
      s"Array lengths differ. Expected ${expected.length} but found ${actual.length}")
    for (i <- expected.indices)
      assertResult(expected(i), s"The arrays differ at position $i")(actual(i))
  }

  def assertArrayEquals(expected: Array[Int], actual: Array[Int]): Unit = {
    assert(expected.length == actual.length,
      s"Array lengths differ. Expected ${expected.length} but found ${actual.length}")
    for (i <- expected.indices)
      assertResult(expected(i), s"The arrays differ at position $i")(actual(i))
  }

  def assertArrayEquals(expected: Array[Long], actual: Array[Long]): Unit = {
    assert(expected.length == actual.length,
      s"Array lengths differ. Expected ${expected.length} but found ${actual.length}")
    for (i <- expected.indices)
      assertResult(expected(i), s"The arrays differ at position $i")(actual(i))
  }

  def assertArrayEquals(expected: Array[Float], actual: Array[Float], tolerance: Float): Unit = {
    assert(expected.length == actual.length,
      s"Array lengths differ. Expected ${expected.length} but found ${actual.length}")
    for (i <- expected.indices)
      assertResult(expected(i), s"The arrays differ at position $i")(actual(i))
  }

  /**
   * Tests two images for equality (pixel-by-pixel)
   *
   * @param expected the first image (typically, the value that is known to be correct)
   * @param actual   the second image (typically, the value retrieved from the code)
   */
  def assertImageEquals(expected: BufferedImage, actual: BufferedImage): Unit =
    assertImageEquals(null, expected, actual)

  def assertImageEquals(message: String, expected: BufferedImage, actual: BufferedImage): Unit = {
    // Uncomment the following block to write both images for a visual comparison
    /*
    try {
      javax.imageio.ImageIO.write(expected, "png", new File("expected.png"));
      javax.imageio.ImageIO.write(actual, "png", new File("actual.png"));
    } catch {
      case e: IOException => e.printStackTrace();
    }
    */
    assert(expected.getWidth == actual.getWidth,
      s"Width is not compatible: Expected ${expected.getWidth} and actual ${actual.getWidth}")
    assert(expected.getHeight == actual.getHeight,
      s"Height is not compatible: Expected ${expected.getHeight} and actual ${actual.getHeight}")
    for (x <- 0 until expected.getWidth; y <- 0 until expected.getHeight()) {
      val expectedPixel = expected.getRGB(x, y)
      val actualPixel = actual.getRGB(x, y)
      assert(expectedPixel == actualPixel, s"$message. Pixels differ at ($x, $y): " +
        s"Expected ${new Color(expectedPixel, true)} but found ${new Color(actualPixel, true)}")
    }
  }

  /**
   * An assertion for the existence of a file (or a directory)
   *
   * @param filePath path to a local file
   */
  protected def assertFileExists(filePath: String): Unit =
    assert(new File(filePath).exists, String.format("File '%s' does not exist", filePath))

  def using[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }
}