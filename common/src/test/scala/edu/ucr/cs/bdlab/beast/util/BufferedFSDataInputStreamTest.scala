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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.IOException
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BufferedFSDataInputStreamTest extends AnyFunSuite with ScalaSparkTest {
  test("all read functions randomly") {
    if (shouldRunStressTest()) {
      // Create a random file
      val fileLength: Long = 10L * 1024 * 1024
      val filePath = new Path(scratchPath, "test.bin")
      val fileSystem = filePath.getFileSystem(new Configuration())
      val out = fileSystem.create(filePath)
      val random = new Random(0)
      val data = new Array[Byte](fileLength.toInt)
      random.nextBytes(data)
      out.write(data)
      out.close()

      // Now, open the file back and confirm the data
      val bufSize = 16 * 1024
      val bufStream = new BufferedFSDataInputStream(fileSystem.open(filePath), bufSize)
      val fsStream = fileSystem.open(filePath)

      try {
        // Perform random seeks and reads
        for (i <- 1 to 10000) {
          try {
            // Randomize to decide whether to seek to a new position or continue reading
            val seekPos = if (fileLength - fsStream.getPos < 16 || random.nextBoolean()) {
              val pos = random.nextLong().abs % (fileLength - 16)
              fsStream.seek(pos)
              bufStream.seek(pos)
            } else {
              fsStream.getPos
            }
            assertResult(fsStream.getPos)(bufStream.getPos)

            random.nextInt(11) match {
              case 0 => // Test readFully
                val readLength = random.nextInt((fileLength - fsStream.getPos).toInt min (2 * bufSize))
                val expectedData = new Array[Byte](readLength)
                fsStream.readFully(expectedData)
                val actualData = new Array[Byte](readLength)
                bufStream.readFully(actualData)
                assertArrayEquals(expectedData, actualData)
              case 1 => // Test readInt
                val expectedVal = fsStream.readInt()
                val actualVal = bufStream.readInt()
                assertResult(expectedVal, s"Error #$i in readInt at position $seekPos")(actualVal)
              case 2 => // Test readShort
                val expectedVal = fsStream.readShort()
                val actualVal = bufStream.readShort()
                assertResult(expectedVal, s"Error #$i in readShort at position $seekPos")(actualVal)
              case 3 => // Test readBoolean
                val expectedVal = fsStream.readBoolean()
                val actualVal = bufStream.readBoolean()
                assertResult(expectedVal, s"Error #$i in readBoolean at position $seekPos")(actualVal)
              case 4 => // Test readChar
                val expectedVal = fsStream.readChar()
                val actualVal = bufStream.readChar()
                assertResult(expectedVal, s"Error #$i in readChar at position $seekPos")(actualVal)
              case 5 => // Test readLong
                val expectedVal = fsStream.readLong()
                val actualVal = bufStream.readLong()
                assertResult(expectedVal, s"Error #$i in readLong at position $seekPos")(actualVal)
              case 6 => // Test readFloat
                val expectedVal = fsStream.readFloat()
                val actualVal = bufStream.readFloat()
                assertResult(java.lang.Float.floatToIntBits(expectedVal), s"Error #$i in readFloat at position $seekPos")(java.lang.Float.floatToIntBits(actualVal))
              case 7 => // Test readDouble
                val expectedVal = fsStream.readDouble()
                val actualVal = bufStream.readDouble()
                assertResult(java.lang.Double.doubleToLongBits(expectedVal), s"Error #$i in readDouble at position $seekPos")(java.lang.Double.doubleToLongBits(actualVal))
              case 8 => // Test readLine
                // FSDataInputStream does not support readLine
                //val expectedVal = fsStream.readLine()
                //val actualVal = bufStream.readLine()
                //assertResult(expectedVal, s"Error #$i in readLine at position $seekPos")(actualVal)
              case 9 => // Test readUTF
                // With random data, it is not easy to test this function
                //val expectedVal = fsStream.readUTF()
                //val actualVal = bufStream.readUTF()
                //assertResult(expectedVal, s"Error #$i in readUTF at position $seekPos")(actualVal)
              case 10 => // Skip bytes
                val skipLength = random.nextInt((fileLength - fsStream.getPos).toInt)
                fsStream.skip(skipLength)
                bufStream.skip(skipLength)
            }
          } catch {
            case e: IOException => fail(s"Error in round #$i", e)
            case e: IndexOutOfBoundsException => fail(s"Error in round #$i", e)
          }
        }
      } finally {
        fsStream.close()
        bufStream.close()
      }
    }
  }

  test("start at a different position") {
    if (shouldRunStressTest()) {
      // Create a random file
      val fileLength: Long = 1L * 1024 * 1024
      val filePath = new Path(scratchPath, "test.bin")
      val fileSystem = filePath.getFileSystem(new Configuration())
      val out = fileSystem.create(filePath)
      val random = new Random(0)
      val expectedData = new Array[Byte](fileLength.toInt)
      random.nextBytes(expectedData)
      out.write(expectedData)
      out.close()

      // Now, open the file back and confirm the data
      val bufSize = 16 * 1024
      val in = fileSystem.open(filePath)
      try {
        in.seek(100)
        val bufStream = new BufferedFSDataInputStream(in, bufSize)
        assertResult(100)(bufStream.getPos)
        val actualData = new Array[Byte](100)
        bufStream.readFully(actualData)
        assertResult(expectedData.slice(100, 200))(actualData)
      } finally {
        in.close()
      }
    }
    }
}
