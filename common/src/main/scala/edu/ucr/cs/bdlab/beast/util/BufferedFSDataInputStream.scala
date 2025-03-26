package edu.ucr.cs.bdlab.beast.util

import org.apache.hadoop.fs.{FSDataInputStream, Seekable}

import java.io.{DataInput, DataInputStream, EOFException, InputStream}

/**
 * A custom class that buffers a chunk of data from an FSDataInputStream and serves random access within
 * that chunk from memory, reducing disk seeks.
 *
 * @param in The FSDataInputStream to be buffered.
 * @param bufferSize  The size of the buffer in bytes.
 */
class BufferedFSDataInputStream(in: FSDataInputStream, bufferSize: Int)
  extends InputStream with DataInput with Seekable {

  private var currentPos: Long = in.getPos
  private val buffer = new Array[Byte](bufferSize)
  private var bufferStartPos: Long = currentPos
  private var bufferEndPos: Long = currentPos

  private val readBuffer: Array[Byte] = new Array[Byte](8)
  private var lineBuffer: Array[Char] = _

  private def numBytesInBuffer: Int = (bufferEndPos - currentPos).toInt

  /**
   * Seek to the specified position in the input stream. If the position is outside the buffered chunk,
   * a disk seek will be performed, and the new chunk will be loaded into the buffer.
   *
   * @param pos The position to seek to.
   */
  override def seek(pos: Long): Unit = seek(pos, false)

  override def seekToNewSource(targetPos: Long): Boolean = seek(targetPos, true)

  private def seek(pos: Long, newSource: Boolean): Boolean = {
    var retVal = false
    if (pos < bufferStartPos || pos >= bufferEndPos || newSource) {
      currentPos = pos
      // Perform a disk seek to the required position, but ensure it doesn't go beyond the end of the file
      if (in.getPos != currentPos) {
        if (newSource) retVal = in.seekToNewSource(pos)
        else in.seek(pos)
      }
      // Read the next chunk of data into the buffer
      bufferStartPos = pos
      bufferEndPos = pos
      var bytesReadLastCall = 0
      while (bytesReadLastCall >= 0 && numBytesInBuffer < buffer.length ) {
        bytesReadLastCall = in.read(buffer, numBytesInBuffer, buffer.length - numBytesInBuffer)
        if (bytesReadLastCall > 0)
          bufferEndPos += bytesReadLastCall
      }
      assert(bufferEndPos == in.getPos)
    } else {
      currentPos = pos
    }
    retVal
  }

  override def getPos: Long = this.currentPos

  def read: Int = {
    if (this.getPos < bufferStartPos || this.getPos >= bufferEndPos)
      seek(this.getPos)
    if (this.getPos == bufferEndPos)
      return -1
    assert(this.getPos >= bufferStartPos && this.getPos < bufferEndPos)
    val x: Byte = this.buffer((this.getPos - bufferStartPos).toInt)
    this.currentPos += 1
    x & 0xFF
  }

  override def readByte(): Byte = readUnsignedByte().toByte

  /**
   * Close the underlying FSDataInputStream when done.
   */
  override def close(): Unit = in.close()

  override def readFully(b: Array[Byte]): Unit = readFully(b, 0, b.length)

  override def readFully(b: Array[Byte], off: Int, len: Int): Unit = {
    var bytesReadLastCall = 0
    var totalBytesRead = 0
    while (bytesReadLastCall >= 0 && totalBytesRead < len) {
      bytesReadLastCall = read(b, off + totalBytesRead, len - totalBytesRead)
      if (bytesReadLastCall > 0)
        totalBytesRead += bytesReadLastCall
    }
    if (totalBytesRead < len)
      throw new EOFException()
  }

  override def read(b: Array[Byte]): Int = this.read(b, 0, b.length)

  override def available(): Int = (this.bufferEndPos - this.currentPos).toInt

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (numBytesInBuffer == 0)
      this.seek(this.getPos)
    val bytesToRead: Int = len min numBytesInBuffer
    System.arraycopy(this.buffer, (this.getPos - bufferStartPos).toInt, b, off, bytesToRead)
    this.currentPos += bytesToRead
    bytesToRead
  }

  override def skipBytes(n: Int): Int = skip(n min Integer.MAX_VALUE).toInt

  override def skip(n: Long): Long = {
    var actualBytesSkipped: Long = 0
    while (actualBytesSkipped < n) {
      val bytesToSkip = n - actualBytesSkipped
      if (bytesToSkip <= numBytesInBuffer) {
        currentPos += n
        actualBytesSkipped += n
      } else {
        assert(in.getPos == bufferEndPos)
        actualBytesSkipped += numBytesInBuffer
        actualBytesSkipped += in.skip(bytesToSkip - numBytesInBuffer)
        currentPos = in.getPos
        bufferStartPos = currentPos
        bufferEndPos = currentPos
      }
    }
    assert(actualBytesSkipped <= n)
    actualBytesSkipped
  }

  override def readBoolean(): Boolean = readUnsignedByte() != 0

  override def readUnsignedByte(): Int = {
    val ch: Int = this.read
    if (ch < 0)
      throw new EOFException()
    ch
  }

  override def readShort(): Short = readUnsignedShort().toShort

  override def readUnsignedShort(): Int = {
    val ch1 = this.read
    val ch2 = this.read
    if ((ch1 | ch2) < 0)
      throw new EOFException()
    (ch1 << 8) + (ch2 << 0)
  }

  override def readChar(): Char = this.readUnsignedShort().toChar

  override def readInt(): Int = {
    val ch1: Int = this.read
    val ch2: Int = this.read
    val ch3: Int = this.read
    val ch4: Int = this.read
    if ((ch1 | ch2 | ch3 | ch4) < 0)
      throw new EOFException()
    (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)
  }

  override def readLong(): Long = {
    readFully(readBuffer, 0, 8)
    (readBuffer(0).toLong << 56) + ((readBuffer(1) & 255).toLong << 48) +
      ((readBuffer(2) & 255).toLong << 40) + ((readBuffer(3) & 255).toLong << 32) +
      ((readBuffer(4) & 255).toLong << 24) + ((readBuffer(5) & 255) << 16) +
      ((readBuffer(6) & 255) << 8) + ((readBuffer(7) & 255) << 0)
  }

  override def readFloat(): Float = java.lang.Float.intBitsToFloat(readInt())

  override def readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

  @deprecated override def readLine(): String = {
    var buf = lineBuffer

    if (buf == null) {
      lineBuffer = new Array[Char](128)
      buf = lineBuffer
    }

    var room = buf.length
    var offset = 0
    var c = 0

    var done: Boolean = false
    while (!done) {
      c = this.read
      c match {
        case -1 | '\n' =>
          done = true
        case '\r' =>
          val c2 = this.read
          if ((c2 != '\n') && (c2 != -1))
            this.seek(this.getPos - 1)
          done = true
        case _ =>
          room = room - 1
          if (room < 0) {
            buf = new Array[Char](offset + 128)
            room = buf.length - offset - 1
            System.arraycopy(lineBuffer, 0, buf, 0, offset)
            lineBuffer = buf
          }
          buf(offset) = c.toChar
          offset += 1
      }
    }
    if ((c == -1) && (offset == 0))
      return null
    String.copyValueOf(buf, 0, offset)
  }

  override def readUTF(): String = DataInputStream.readUTF(this)
}
