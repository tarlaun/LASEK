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
package edu.ucr.cs.bdlab.beast.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input

import java.io.ObjectInput

/**
 * A wrapper around Kryo [[Input]] that makes it look like [[ObjectInput]]
 * @param input
 */
class KryoInputToObjectInput(kryo: Kryo, input: Input) extends ObjectInput {
  override def readFully(b: Array[Byte]): Unit = readFully(b, 0, b.length)

  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = {
    var start = off
    var remaining = len
    while (remaining > 0) {
      val readSize = input.read(b, start, remaining)
      if (readSize > 0) {
        start += readSize
        remaining -= readSize
      }
    }
  }

  override def skipBytes(n: Int): Int = {
    input.skip(n)
    n
  }

  override def readBoolean(): Boolean = input.readBoolean()

  override def readByte(): Byte = input.readByte()

  override def readUnsignedByte(): Int = input.readByteUnsigned()

  override def readShort(): Short = input.readShort()

  override def readUnsignedShort(): Int = input.readShortUnsigned()

  override def readChar(): Char = input.readChar()

  override def readInt(): Int = input.readInt()

  override def readLong(): Long = input.readLong()

  override def readFloat(): Float = input.readFloat()

  override def readDouble(): Double = input.readDouble()

  override def readLine(): String = ???

  override def readUTF(): String = input.readString()

  override def readObject(): AnyRef = kryo.readClassAndObject(input)

  override def read(): Int = input.read()

  override def read(b: Array[Byte]): Int = read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = read(b, off, len)

  override def skip(n: Long): Long = skip(n)

  override def available(): Int = input.available()

  override def close(): Unit = input.close()
}
