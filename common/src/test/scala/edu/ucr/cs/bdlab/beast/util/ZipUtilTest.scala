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

import org.apache.commons.compress.archivers.zip.{Zip64Mode, ZipArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{File, FileOutputStream}
import java.util.zip.{CRC32, ZipEntry, ZipFile, ZipOutputStream}

@RunWith(classOf[JUnitRunner])
class ZipUtilTest extends AnyFunSuite with ScalaSparkTest {
  test("Merge two files") {
    val fileSystem = FileSystem.getLocal(new Configuration())
    val file1 = new Path(scratchPath, "test1.zip")
    val zip1 = new ZipOutputStream(fileSystem.create(file1))
    zip1.putNextEntry(new ZipEntry("README.bin"))
    zip1.write(Array[Byte](1, 2, 3, 4, 5, 6))
    zip1.closeEntry()
    zip1.putNextEntry(new ZipEntry("data.bin"))
    zip1.write(Array[Byte](1, 2, 3))
    zip1.closeEntry()
    zip1.close()

    val file2 = new Path(scratchPath, "test2.zip")
    val zip2 = new ZipOutputStream(fileSystem.create(file2))
    zip2.putNextEntry(new ZipEntry("README2.bin"))
    zip2.write(Array[Byte](1, 2, 3, 4))
    zip2.closeEntry()
    zip2.putNextEntry(new ZipEntry("data2.bin"))
    zip2.write(Array[Byte](1, 2, 3, 4, 5))
    zip2.closeEntry()
    zip2.close()

    val mergedFile = new Path(scratchPath, "test3.zip")

    ZipUtil.mergeZip(fileSystem, mergedFile, file1, file2)
    assertResult(false)(fileSystem.exists(file1))
    assertResult(false)(fileSystem.exists(file2))
    assertResult(true)(fileSystem.exists(mergedFile))

    val in = new ZipFile(mergedFile.toString)
    val entries = in.entries()
    var numEntries = 0
    while (entries.hasMoreElements) {
      entries.nextElement()
      numEntries += 1
    }
    in.close()
    assertResult(4)(numEntries)
  }

  test("Get file locations and sizes") {
    val file1 = new File(scratchDir, "test1.zip")
    val zip1 = new ZipOutputStream(new FileOutputStream(file1))
    var entry = new ZipEntry("README.bin")
    ZipUtil.putStoredFile(zip1, "README.bin", Array[Byte](1, 2, 3, 4, 5, 6))
    ZipUtil.putStoredFile(zip1, "data.bin", Array[Byte](1, 2, 3))
    zip1.close()

    val fileSystem = FileSystem.getLocal(new Configuration())
    val contents = ZipUtil.listFilesInZip(fileSystem, new Path(file1.getPath))
    assertResult(2)(contents.length)
    assertResult("README.bin")(contents.head._1)
    assertResult(6)(contents.head._3)

    // Read the contents of the README file
    val in = fileSystem.open(new Path(file1.getPath))
    in.seek(contents(0)._2)
    val data = new Array[Byte](contents(0)._3.toInt)
    in.readFully(data)
    assertArrayEquals(Array[Byte](1, 2, 3, 4, 5, 6), data)
  }

  test("Last files") {
    val fileSystem = FileSystem.getLocal(new Configuration())
    val file1 = new Path(scratchPath, "test1.zip")
    val zip1 = new ZipOutputStream(fileSystem.create(file1))
    ZipUtil.putStoredFile(zip1, "README.bin", Array[Byte](1, 2, 3, 4, 5, 6))
    ZipUtil.putStoredFile(zip1, "data.bin", Array[Byte](1, 2, 3))
    zip1.close()

    val lastFile: Array[(String, Long, Long)] = ZipUtil.lastNFiles(fileSystem, file1, 1)
    assertResult("data.bin")(lastFile(0)._1)

    val lastFiles: Array[(String, Long, Long)] = ZipUtil.lastNFiles(fileSystem, file1, 2)
    assertResult("README.bin")(lastFiles(0)._1)
    assertResult(6)(lastFiles(0)._3)
    val in = fileSystem.open(file1)
    try {
      in.seek(lastFiles(0)._2)
      val data = new Array[Byte](lastFiles(0)._3.toInt)
      in.readFully(data)
      assertArrayEquals(Array[Byte](1, 2, 3, 4, 5, 6), data)
    } finally {
      in.close()
    }
  }

  test("List files in a ZIP64 archive") {
    val fileSystem = FileSystem.getLocal(new Configuration())
    val file1 = new Path(scratchPath, "test1.zip")
    val zip1 = new ZipArchiveOutputStream(fileSystem.create(file1))
    zip1.setUseZip64(Zip64Mode.Always)
    ZipUtil.putStoredFile(zip1, "README.bin", Array[Byte](1, 2, 3, 4, 5, 6))
    ZipUtil.putStoredFile(zip1, "data.bin", Array[Byte](1, 2, 3))
    zip1.close()

    val files = ZipUtil.listFilesInZip(fileSystem, file1)
    assertResult(2)(files.length)
  }

  test("Last files in ZIP64") {
    val fileSystem = FileSystem.getLocal(new Configuration())
    val file1 = new Path(scratchPath, "test1.zip")
    val zip1 = new ZipArchiveOutputStream(fileSystem.create(file1))
    zip1.setUseZip64(Zip64Mode.Always)
    ZipUtil.putStoredFile(zip1, "README.bin", Array[Byte](1, 2, 3, 4, 5, 6))
    ZipUtil.putStoredFile(zip1, "data.bin", Array[Byte](1, 2, 3))
    zip1.close()

    val lastFile: Array[(String, Long, Long)] = ZipUtil.lastNFiles(fileSystem, file1, 1)
    assertResult("data.bin")(lastFile(0)._1)

    val lastFiles: Array[(String, Long, Long)] = ZipUtil.lastNFiles(fileSystem, file1, 2)
    assertResult("README.bin")(lastFiles(0)._1)
    assertResult(6)(lastFiles(0)._3)
    val in = fileSystem.open(file1)
    try {
      in.seek(lastFiles(0)._2)
      val data = new Array[Byte](lastFiles(0)._3.toInt)
      in.readFully(data)
      assertArrayEquals(Array[Byte](1, 2, 3, 4, 5, 6), data)
    } finally {
      in.close()
    }
  }
}
