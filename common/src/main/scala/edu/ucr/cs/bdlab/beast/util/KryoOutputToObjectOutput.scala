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
import com.esotericsoftware.kryo.io.Output

import java.io.ObjectOutput

/**
 * A wrapper around Kryo [[Output]] to make it look like [[ObjectOutput]]
 * @param output
 */
class KryoOutputToObjectOutput(kryo: Kryo, output: Output) extends ObjectOutput {
  override final def write(b: Int): Unit = output.write(b)

  override final def write(b: Array[Byte]): Unit = output.write(b)

  override final def write(b: Array[Byte], off: Int, len: Int): Unit = output.write(b, off, len)

  override final def writeBoolean(v: Boolean): Unit = output.writeBoolean(v)

  override final def writeByte(v: Int): Unit = output.writeByte(v)

  override final def writeShort(v: Int): Unit = output.writeShort(v)

  override final def writeChar(v: Int): Unit = output.writeChar(v.toChar)

  override final def writeInt(v: Int): Unit = output.writeInt(v)

  override final def writeLong(v: Long): Unit = output.writeLong(v)

  override final def writeFloat(v: Float): Unit = output.writeFloat(v)

  override final def writeDouble(v: Double): Unit = output.writeDouble(v)

  override final def writeBytes(s: String): Unit = output.writeBytes(s.getBytes())

  override final def writeChars(s: String): Unit = {
    val chars = new Array[Char](s.length)
    s.getChars(0, s.length, chars, 0)
    output.writeChars(chars)
  }

  override final def writeUTF(s: String): Unit = output.writeString(s)

  override def writeObject(obj: Any): Unit = kryo.writeClassAndObject(output, obj)

  override def flush(): Unit = output.flush()

  override def close(): Unit = output.close()
}
