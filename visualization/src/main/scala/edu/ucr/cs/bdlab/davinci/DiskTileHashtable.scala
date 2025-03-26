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
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.util.MathUtil
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.internal.Logging

import java.io.{DataOutput, OutputStream}
import java.util
import scala.util.hashing.MurmurHash3

/**
 * A set of functions for writing and retrieving disk-based hashtable. The hashtable is static, i.e., constructed
 * once and cannot be modified. It only works with 64-bit long keys and values.
 */
object DiskTileHashtable extends Logging {
  val Signature: Array[Byte] = "diskhash".getBytes

  /**
   * Construct a compact hashtable for the given list of entries and write to the given output
   * @param out the data output to write the hashtable to
   * @param entries the list of entries in the form (key=tileID, val1=Offset, val2=Length)
   */
  def construct(out: DataOutput, entries: Array[(Long, Long, Int)]): Unit = {
    // Special case for an empty table. Write nothing at all.
    if (entries.isEmpty)
      return
    val capacity = MathUtil.nextPowerOfTwo(entries.length * 4 / 3) - 1
    val hashtable = new Array[(Long, Long, Int)](capacity)
    val serKey = new Array[Int](8)
    var collisions: Long = 0
    for (entry <- entries) {
      var iter = 0
      var tablePosition: Int = -1
      do {
        collisions += 1
        serializeKey(entry._1, serKey)
        tablePosition = MurmurHash3.arrayHash(serKey, iter).abs % capacity
        iter += 1
      } while (hashtable(tablePosition) != null)
      hashtable(tablePosition) = entry
    }
    collisions -= entries.length
    logInfo(s"Constructed a hashtable with ${entries.length} entries with ${collisions} collisions")

    // Next, write it to the given output
    // 1- Write a fixed signature
    out.write(Signature)
    // 2- Write the capacity of the hashtable (number of buckets)
    out.writeInt(capacity)
    // 3- Write all values of the buckets
    for (value <- hashtable) {
      if (value == null) {
        out.writeLong(-1)
        out.writeLong(-1)
        out.writeInt(-1)
      } else {
        out.writeLong(value._1)
        out.writeLong(value._2)
        out.writeInt(value._3)
      }
    }
  }

  def getValueJ(in: FSDataInputStream, offset: Long, key: Long): (java.lang.Long, java.lang.Integer) = {
    val v = getValue(in, offset, key)
    if (v == null) null else (v._1, v._2)
  }

  /**
   * Return the value that corresponds to the given key or null if the value is not found.
   * @param in the hashtable file
   * @param offset the offset of the hashtable in the file
   * @param key the key to search for
   * @return the value of the key if found, or `null` if the key is not found.
   */
  def getValue(in: FSDataInputStream, offset: Long, key: Long): (Long, Int) = {
    in.seek(offset)
    val signature = new Array[Byte](8)
    in.readFully(signature)
    require(util.Arrays.equals(signature, Signature), "Incorrect file header")
    val capacity = in.readInt()
    val headerSize = signature.length + 4
    val entrySize = 8 + 8 + 4
    val serKey = new Array[Int](8)
    serializeKey(key, serKey)
    var iter: Int = 0
    var maxIter: Int = Int.MaxValue
    var iterFast: Int = 1
    var found: Boolean = false
    while (!found && iter < maxIter) {
      val tablePosition: Int = MurmurHash3.arrayHash(serKey, iter).abs % capacity
      if (tablePosition == (MurmurHash3.arrayHash(serKey, iterFast).abs % capacity)) {
        // Cycle detected. Make sure to step when the slow pointer completes the cycle.
        maxIter = iterFast
      }
      in.seek(offset + headerSize + tablePosition.toLong * entrySize)
      val diskKey: Long = in.readLong()
      if (diskKey == -1) {
        // Not found
        return null
      }
      if (diskKey == key) {
        found = true
        val offset: Long = in.readLong()
        val length: Int = in.readInt()
        return (offset, length)
      }
      iter += 1
      iterFast += 2
    }
    null
  }

  /**
   * Return the value that corresponds to the given key or null if the value is not found.
   * @param fileSystem the file system in which the hashtable is stored
   * @param path the path of the disk hashtable
   * @param key the key to search for in the hashtable
   */
  def getValue(fileSystem: FileSystem, path: Path, key: Long): (Long, Int) = {
    val in: FSDataInputStream = fileSystem.open(path)
    try {
      getValue(in, 0, key)
    } finally {
      in.close()
    }
  }

  def getValueJ(fileSystem: FileSystem, path: Path, key: Long): (java.lang.Long, java.lang.Integer) = {
    val v = getValue(fileSystem, path, key)
    if (v == null) null else (v._1, v._2)
  }

  def serializeKey(key: Long, serKey: Array[Int]): Unit = {
    serKey(0) = (key & 0xff).toInt
    serKey(1) = ((key >> 8) & 0xff).toInt
    serKey(2) = ((key >> 16) & 0xff).toInt
    serKey(3) = ((key >> 24) & 0xff).toInt
    serKey(4) = ((key >> 32) & 0xff).toInt
    serKey(5) = ((key >> 40) & 0xff).toInt
    serKey(6) = ((key >> 48) & 0xff).toInt
    serKey(7) = ((key >> 56) & 0xff).toInt
  }
}
