/*
 * Copyright 2023 University of California, Riverside
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

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.internal.Logging

import java.io.DataInputStream
import java.util.zip.{CRC32, ZipEntry, ZipOutputStream}
import scala.annotation.varargs

/**
 * Some utility functions to help with manipulating and reading ZIP files
 *  - See [[https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT]]
 *  - See [[https://users.cs.jmu.edu/buchhofp/forensics/formats/pkzip.html]]
 *  - See [[https://en.wikipedia.org/wiki/ZIP_(file_format)]]
 */
object ZipUtil extends Logging {

  /**
   * Returns information about the last n files in the archive.
   *
   * **Compatibility Note**: This method is not guaranteed to return the correct answer. For efficiency, it tries to locate
   * the directory entries from the end using the ZIP signature. In rare cases, it might retrieve false information
   * since the signature might appear out of coincidence. To be accurate, this method has to read all ZIP entries
   * until it finds the last ones because directory entries are variable size in ZIP.
   * @param fs the file system that contains the ZIP archive
   * @param path the path to the ZIP file
   * @param n the number of entries to retrieve from the end
   * @return file names, offsets, and lengths for the last n entries if the ZIP file contains at least n entries.
   *         Otherwise, it returns all entries in the file.
   */
  def lastNFiles(fs: FileSystem, path: Path, n: Int): Array[(String, Long, Long)] = {
    val signature = Array[Byte](0x50, 0x4b, 0x01, 0x02)
    val buffer = new Array[Byte](2048)
    var entries = new Array[(String, Long, Long)](n)
    val fileLen = fs.getFileStatus(path).getLen
    val zipIn = fs.open(path)
    try {
      var numEntriesFound = 0
      var lastPositionSearched = fileLen
      while (numEntriesFound < n && lastPositionSearched > 0) {
        System.arraycopy(buffer, 0, buffer, buffer.length / 2, buffer.length / 2)
        val bufferLength: Int = (lastPositionSearched min buffer.length / 2).toInt
        val searchPos = lastPositionSearched - bufferLength
        zipIn.seek(searchPos)
        zipIn.readFully(buffer, 0, bufferLength)
        var signaturePos = bufferLength
        while (signaturePos >= 0 && numEntriesFound < n) {
          var i = 0
          while (i < signature.length && signature(i) == buffer(signaturePos + i))
            i += 1
          if (i == signature.length) {
            // Found signature
            numEntriesFound += 1
            var uncompressedSize: Long = getIntLittleEndian(buffer, signaturePos + 0x18) & 0xFFFFFFFFL
            val fileNameLen: Short = getShortLittleEndian(buffer, signaturePos + 0x1c)
            var localHeaderPos: Long = getIntLittleEndian(buffer, signaturePos + 0x2a) & 0xFFFFFFFFL
            val fileName = new String(buffer, signaturePos + 0x2e, fileNameLen)
            // Check for ZIP64
            if (uncompressedSize == 0xFFFFFFFFL || localHeaderPos == 0xFFFFFFFFL) {
              // Read the extra field
              val extraFieldLen: Short = getShortLittleEndian(buffer, signaturePos + 0x1e)
              assert(extraFieldLen >= 8, s"Expected longer extra field for a ZIP64 entry ($extraFieldLen)")
              // Read the correct compressed and uncompressed sizes from the extra field
              val extraFieldPos: Int = signaturePos + 46 + fileNameLen
              assert(getShortLittleEndian(buffer, extraFieldPos) == 1,
                s"Invalid signature for the extra field in ZIP64 file ${getShortLittleEndian(buffer, extraFieldPos)}")
              // val dataLength = getShortLittleEndian(this.extraField, 2)
              uncompressedSize = getLongLittleEndian(buffer, extraFieldPos + 4)
              localHeaderPos = getLongLittleEndian(buffer, extraFieldPos + 20)
            }
            entries(entries.length - numEntriesFound) = (fileName, localHeaderPos, uncompressedSize)
          }
          signaturePos -= 1
        }
        lastPositionSearched = searchPos
      }
      if (numEntriesFound < n)
        entries = entries.slice(entries.length - numEntriesFound, entries.length)
      // Translate local header position to actual file position
      getEntryPositions(zipIn, entries)
      entries
    } finally {
      zipIn.close()
    }
  }

  /** Java shortcut to [[lastNFiles()]] */
  def lastNFilesJ(fs: FileSystem, path: Path, n: Int): Array[(String, java.lang.Long, java.lang.Long)] =
    lastNFiles(fs, path, n).map(x => {(x._1, java.lang.Long.valueOf(x._2), java.lang.Long.valueOf(x._3))})

  /**
   * Locates the local header for each of the given files to retrieve the position of each entry in the file.
   * This method modifies the given array by replacing the local header position (second entry in each tuple)
   * with the position of the data file in the ZIP archive.
   * @param zipIn random input stream on the ZIP file
   * @param entries the list of entries in the format (filename, local header position, length)
   */
  private def getEntryPositions(zipIn: FSDataInputStream, entries: Array[(String, Long, Long)]): Unit = {
    for (iEntry <- entries.indices) {
      val entry = entries(iEntry)
      zipIn.seek(entry._2)
      zipIn.skip(26)
      val fileNameLen = IOUtil.readShortLittleEndian(zipIn)
      val extraFieldLen = IOUtil.readShortLittleEndian(zipIn)
      zipIn.skip(fileNameLen + extraFieldLen)
      entries(iEntry) = (entry._1, zipIn.getPos, entry._3)
    }
  }

  val CompressionNone: Short = 0
  val CompressionShrunk: Short = 1
  val CompressionReduced1: Short = 2
  val CompressionReduced2: Short = 3
  val CompressionReduced3: Short = 4
  val CompressionReduced4: Short = 5
  val CompressionImploded: Short = 6
  val CompressionDeflate: Short = 8
  val CompressionEnhancedDeflate: Short = 9
  val CompressionPKDCLImploded: Short = 10
  val CompressionBZip2: Short = 12
  val CompressionLZMA: Short = 14
  val CompressionIBMTERSE: Short = 18
  val CompressionIBMLZ77Z: Short = 19
  val CompressionPPMD1: Short = 98

  val CRCMagicNumber: Int = 0xdebb20e3

  /**
   * Local file header in a ZIP archive. See Section 4.3.7
   */
  class LocalFileHeader {
    var signature: Int = _
    var version: Short = _
    var flags: Short = _
    var compression: Short = _
    var modificationTime: Long = _
    /** value computed over file data by CRC-32 algorithm with 'magic number' 0xdebb20e3 (little endian) */
    var crc32: Int = _
    var compressedSize: Long = _
    var uncompressedSize: Long = _
    var filename: String = _
    var extraField: Array[Byte] = _

    def read(in: FSDataInputStream): Unit = {
      this.signature = in.readInt()
      require(this.signature == 0x04034b50)
      this.version = in.readShort()
      this.flags = in.readShort()
      this.compression = in.readShort()
      val modTime = in.readShort()
      val modDate = in.readShort()
      this.crc32 = in.readInt()
      this.compressedSize = in.readInt() & 0xffffffffL
      this.uncompressedSize = in.readInt() & 0xfffffffL
      val fileNameLen = in.readShort()
      val extraFieldLen = in.readShort()
      val name = new Array[Byte](fileNameLen)
      in.readFully(name)
      this.filename = new String(name)
      this.extraField = new Array[Byte](extraFieldLen)
      in.readFully(this.extraField)
      // TODO Check for ZIP64
    }
  }

  /**
   * An entry in the central directory for a file. See section 4.3.12
   */
  class CentralDirectoryFileHeader {
    var version: Short = _
    var versionNeeded: Short = _
    var flags: Short = _
    var compression: Short = _
    var modTime: Short = _
    var modDate: Short = _
    var crc32: Int = _
    var compressedSize: Long = _
    var uncompressedSize: Long = _
    var filename: String = _
    var extraField: Array[Byte] = _
    var fileComment: String = _
    var diskStartNum: Short = _
    var internalAttributes: Short = _
    var externalAttributes: Int = _
    var offsetLocalHeader: Long = _

    def read(in: FSDataInputStream): Unit = {
      // Signature should always be 0x504b0102
      val signature = in.readInt()
      require(signature == 0x504b0102)
      this.version = IOUtil.readShortLittleEndian(in)
      this.versionNeeded = IOUtil.readShortLittleEndian(in)
      this.flags = IOUtil.readShortLittleEndian(in)
      this.compression = IOUtil.readShortLittleEndian(in)
      this.modTime = IOUtil.readShortLittleEndian(in)
      this.modDate = IOUtil.readShortLittleEndian(in)
      // TODO combine modTime and modDate into modification time
      this.crc32 = IOUtil.readIntLittleEndian(in)
      this.compressedSize = IOUtil.readIntLittleEndian(in) & 0xffffffffL
      this.uncompressedSize = IOUtil.readIntLittleEndian(in) & 0xffffffffL
      val filenameLen = IOUtil.readShortLittleEndian(in)
      val extraFieldLen = IOUtil.readShortLittleEndian(in)
      val fileCommentLen = IOUtil.readShortLittleEndian(in)
      this.diskStartNum = IOUtil.readShortLittleEndian(in)
      this.internalAttributes = IOUtil.readShortLittleEndian(in)
      this.externalAttributes = IOUtil.readIntLittleEndian(in)
      this.offsetLocalHeader = IOUtil.readIntLittleEndian(in) & 0xffffffffL
      def readByteArray(len: Int): Array[Byte] = {
        if (len == 0) null else {
          val dataArray = new Array[Byte](len)
          in.readFully(dataArray)
          dataArray
        }
      }
      def readStr(len: Int): String = if (len == 0) null else new String(readByteArray(len))
      this.filename = readStr(filenameLen)
      this.extraField = readByteArray(extraFieldLen)
      this.fileComment = readStr(fileCommentLen)
      // Check for ZIP 64
      if (this.compressedSize == 0xffffffffL && this.uncompressedSize == 0xffffffffL) {
        // Read the correct compressed and uncompressed sizes from the extra field
        val extraSignature = getShortLittleEndian(this.extraField, 0)
        assert(extraSignature == 1, s"Invalid signature for the extra field in ZIP64 file ${extraSignature}")
        // val dataLength = getShortLittleEndian(this.extraField, 2)
        this.uncompressedSize = getLongLittleEndian(this.extraField, 4)
        this.compressedSize = getLongLittleEndian(this.extraField, 12)
        this.offsetLocalHeader = getLongLittleEndian(this.extraField, 20)
      }
    }

    def write(out: FSDataOutputStream): Unit = {
      IOUtil.writeIntLittleEndian(out, 0x02014b50)
      IOUtil.writeShortLittleEndian(out, this.version)
      IOUtil.writeShortLittleEndian(out, this.versionNeeded)
      IOUtil.writeShortLittleEndian(out, this.flags)
      IOUtil.writeShortLittleEndian(out, this.compression)
      IOUtil.writeShortLittleEndian(out, this.modTime)
      IOUtil.writeShortLittleEndian(out, this.modDate)
      IOUtil.writeIntLittleEndian(out, this.crc32)
      if (isZIP64) {
        extraField = new Array[Byte](32)
        putShortLittleEndian(extraField, 0, 1)
        putShortLittleEndian(extraField, 2, 28)
        putLongLittleEndian(extraField, 4, this.uncompressedSize)
        putLongLittleEndian(extraField, 12, this.compressedSize)
        putLongLittleEndian(extraField, 20, this.offsetLocalHeader)
      } else {
        extraField = null
      }
      IOUtil.writeIntLittleEndian(out, (this.compressedSize min 0xffffffffL).toInt)
      IOUtil.writeIntLittleEndian(out, (this.uncompressedSize min 0xffffffffL).toInt)
      val filenameBytes = filename.getBytes()
      IOUtil.writeShortLittleEndian(out, filenameBytes.length.toShort)
      IOUtil.writeShortLittleEndian(out, if (extraField == null) 0 else extraField.length.toShort)
      val fileCommentBytes = if (this.fileComment == null) null else this.fileComment.getBytes()
      IOUtil.writeShortLittleEndian(out, if (fileCommentBytes == null) 0 else fileCommentBytes.length.toShort)
      IOUtil.writeShortLittleEndian(out, this.diskStartNum)
      IOUtil.writeShortLittleEndian(out, this.internalAttributes)
      IOUtil.writeIntLittleEndian(out, this.externalAttributes)
      IOUtil.writeIntLittleEndian(out, (this.offsetLocalHeader min 0xffffffffL).toInt)
      out.write(filenameBytes)
      if (extraField != null)
        out.write(extraField)
      if (fileCommentBytes != null)
        out.write(fileCommentBytes)
    }

    def isZIP64: Boolean = this.compressedSize >= 0xffffffffL || this.uncompressedSize >= 0xffffffffL ||
      this.offsetLocalHeader >= 0xffffffffL
  }

  class EndOfCentralDirectory {
    var versionMadeBy: Short = 45
    var versionNeededToExtract: Short = 45
    var diskNumber: Int = _
    var diskNumCentralDir: Int = _
    var numDiskEntries: Long = _
    var numEntries: Long = _
    var centralDirSize: Long = _
    var offsetOfCentralDirInDisk: Long = _
    var comment: String = _

    def read(in: FSDataInputStream): Unit = {
      // See Section 4.3.16
      val signature = IOUtil.readIntLittleEndian(in)
      require(signature == 0x0504b0506, f"Invalid EOCD signature ${signature}%x")
      this.diskNumber = IOUtil.readShortLittleEndian(in)
      this.diskNumCentralDir = IOUtil.readShortLittleEndian(in)
      this.numDiskEntries = IOUtil.readShortLittleEndian(in) & 0xffffL
      this.numEntries = IOUtil.readShortLittleEndian(in) & 0xffffL
      this.centralDirSize = IOUtil.readIntLittleEndian(in)
      this.offsetOfCentralDirInDisk = IOUtil.readIntLittleEndian(in) & 0xffffffffL
      val commentLength = IOUtil.readShortLittleEndian(in)
      this.comment = if (commentLength == 0) null
      else {
        val comment = new Array[Byte](commentLength)
        in.readFully(comment)
        new String(comment)
      }
    }

    def read(buffer: Array[Byte], pos: Int, bufferLength: Int): Unit = {
      // See Section 4.3.16
      val signature = getIntLittleEndian(buffer, pos)
      assert(signature == 0x06054B50, f"Invalid ZIP signature ${signature}%x")
      this.diskNumber = getShortLittleEndian(buffer, pos + 0x4)
      this.diskNumCentralDir = getShortLittleEndian(buffer, pos + 0x6)
      this.numDiskEntries = getShortLittleEndian(buffer, pos + 0x8)
      this.numEntries = getShortLittleEndian(buffer, pos + 0xa)
      this.centralDirSize = getIntLittleEndian(buffer, pos + 0xc)
      this.offsetOfCentralDirInDisk = getIntLittleEndian(buffer, pos + 0x10)
      val commentLength = getShortLittleEndian(buffer, pos + 0x14)
      if (pos + 0x16 + commentLength > bufferLength)
        throw new RuntimeException("Could not read full EOCD record")
      this.comment = new String(buffer, pos + 0x16, commentLength)
    }

    /**
     * Read ZIP64 end of central directory record
     * @param in the input stream to read from
     */
    def read64(in: DataInputStream): Unit = {
      // See Section 4.3.14
      val signature = IOUtil.readIntLittleEndian(in)
      assert(signature == 0x06064B50, f"Invalid ZIP64 end of central directory signature ${signature}%x")
      val eocdSize: Long = IOUtil.readLongLittleEndian(in)
      this.versionMadeBy = IOUtil.readShortLittleEndian(in)
      this.versionNeededToExtract = IOUtil.readShortLittleEndian(in)
      this.diskNumber = IOUtil.readIntLittleEndian(in)
      this.diskNumCentralDir = IOUtil.readIntLittleEndian(in)
      this.numDiskEntries = IOUtil.readLongLittleEndian(in)
      this.numEntries = IOUtil.readLongLittleEndian(in)
      this.centralDirSize = IOUtil.readLongLittleEndian(in)
      this.offsetOfCentralDirInDisk = IOUtil.readLongLittleEndian(in)
      // TODO depending on eocdSize, we may need to read extended data
    }

    def isZIP64: Boolean = this.numEntries >= 0xffffL ||
      this.offsetOfCentralDirInDisk >= 0xffffffffL ||
      this.centralDirSize >= 0xffffffffL

    /**
     * Write this end of central directory to the file
     * @param headerOut the output to write the header to
     * @param eocdPos the final position of this eocd record in the file.
     *                In general, this  should be equal to headerOut.getPos. However, if this header is going to be
     *                appended to a bigger file, and this is a ZIP64 file, the correct position should be given.
     */
    def write(headerOut: FSDataOutputStream, eocdPos: Long): Unit = {
      if (isZIP64) {
        // Write extended end of central directory for ZIP64 (Section 4.3.14)
        IOUtil.writeIntLittleEndian(headerOut, 0x06064B50)
        IOUtil.writeLongLittleEndian(headerOut, 44) // Size
        IOUtil.writeShortLittleEndian(headerOut, this.versionMadeBy)
        IOUtil.writeShortLittleEndian(headerOut, this.versionNeededToExtract)
        IOUtil.writeIntLittleEndian(headerOut, this.diskNumber)
        IOUtil.writeIntLittleEndian(headerOut, this.diskNumCentralDir)
        IOUtil.writeLongLittleEndian(headerOut, this.numDiskEntries)
        IOUtil.writeLongLittleEndian(headerOut, this.numEntries)
        IOUtil.writeLongLittleEndian(headerOut, this.centralDirSize)
        IOUtil.writeLongLittleEndian(headerOut, this.offsetOfCentralDirInDisk)
        // Write EOCD locator for ZIP64
        IOUtil.writeIntLittleEndian(headerOut, 0x07064B50) // Signature
        IOUtil.writeIntLittleEndian(headerOut, 0) // Disk number with EOCD
        IOUtil.writeLongLittleEndian(headerOut, eocdPos) // Offset of EOCD in disk
        IOUtil.writeIntLittleEndian(headerOut, 1) // Total number of disks
        // Write a fake end of central directory for small zip
        IOUtil.writeIntLittleEndian(headerOut, 0x06054B50) // Signature
        IOUtil.writeShortLittleEndian(headerOut, this.diskNumber.toShort) // Disk Number
        IOUtil.writeShortLittleEndian(headerOut, this.diskNumCentralDir.toShort) // Disk number with central directory
        IOUtil.writeShortLittleEndian(headerOut, (this.numDiskEntries min 0xffffL).toShort) // Number of entries in this disk
        IOUtil.writeShortLittleEndian(headerOut, (this.numEntries min 0xffffL).toShort) // Total number of entries
        IOUtil.writeIntLittleEndian(headerOut, (this.centralDirSize min 0xffffffffL).toInt) // Central directory size
        IOUtil.writeIntLittleEndian(headerOut, (this.offsetOfCentralDirInDisk min 0xffffffffL).toInt) // Offset of central directory in disk
        if (this.comment == null) {
          IOUtil.writeShortLittleEndian(headerOut, 0)
        } else {
          val commentBytes = this.comment.getBytes
          IOUtil.writeShortLittleEndian(headerOut, commentBytes.length.toShort)
          headerOut.write(commentBytes)
        }
      } else {
        // Regular small file
        IOUtil.writeIntLittleEndian(headerOut, 0x06054B50)
        IOUtil.writeShortLittleEndian(headerOut, this.diskNumber.toShort)
        IOUtil.writeShortLittleEndian(headerOut, this.diskNumCentralDir.toShort)
        IOUtil.writeShortLittleEndian(headerOut, this.numDiskEntries.toShort)
        IOUtil.writeShortLittleEndian(headerOut, this.numEntries.toShort)
        IOUtil.writeIntLittleEndian(headerOut, this.centralDirSize.toInt)
        IOUtil.writeIntLittleEndian(headerOut, this.offsetOfCentralDirInDisk.toInt)
        if (this.comment == null) {
          IOUtil.writeShortLittleEndian(headerOut, 0)
        } else {
          val commentBytes = this.comment.getBytes
          IOUtil.writeShortLittleEndian(headerOut, commentBytes.length.toShort)
          headerOut.write(commentBytes)
        }
      }
    }

  }

  /**
   * Merges multiple ZIP files into one and deletes the input files.
   * @param mergedFile the output file that contains the merged ZIP files
   * @param zipFiles the input files to be merged
   */
  def mergeZip(fileSystem: fs.FileSystem, mergedFile: Path, @varargs zipFiles: Path*): Unit = {
    val mergedHeaderFile = new Path(mergedFile.getParent, mergedFile.getName+"_mergedheader")
    val headerOut = fileSystem.create(mergedHeaderFile)
    // How much to shift the file offsets in the next file to write
    var shift: Long = 0
    var totalNumEntries: Long = 0
    var centralDirectorySize: Long = 0
    // Retrieve the file information from all input files to build the merged central directory
    for (zipFile <- zipFiles) {
      val fileLength = fileSystem.getFileStatus(zipFile).getLen
      val in = fileSystem.open(zipFile)
      try {
        val endOfCentralDirectory = readEndOfCentralDirectory(in, fileLength)

        // Seek to the central directory to read the contents
        in.seek(endOfCentralDirectory.offsetOfCentralDirInDisk)
        var iEntry: Int = 0
        val directoryFileHeader = new CentralDirectoryFileHeader
        while (iEntry < endOfCentralDirectory.numEntries) {
          assert(in.getPos < endOfCentralDirectory.offsetOfCentralDirInDisk + endOfCentralDirectory.centralDirSize)
          directoryFileHeader.read(in)
          directoryFileHeader.offsetLocalHeader += shift
          directoryFileHeader.write(headerOut)
          iEntry += 1
        }
        shift += fileLength
        totalNumEntries += endOfCentralDirectory.numEntries
      } finally {
        in.close()
      }
    }
    centralDirectorySize = headerOut.getPos
    // Write end of central directory record
    val endOfCentralDirectory = new EndOfCentralDirectory
    endOfCentralDirectory.diskNumber = 0
    endOfCentralDirectory.diskNumCentralDir = 0
    endOfCentralDirectory.numDiskEntries = totalNumEntries
    endOfCentralDirectory.numEntries = totalNumEntries
    endOfCentralDirectory.centralDirSize = centralDirectorySize
    endOfCentralDirectory.offsetOfCentralDirInDisk = shift
    endOfCentralDirectory.comment = "Merged with Beast"
    endOfCentralDirectory.write(headerOut, centralDirectorySize + shift)
    headerOut.close()
    // Merge all the files, ending with the new header
    FileUtil.concat(fileSystem, mergedFile, (zipFiles :+ mergedHeaderFile):_*)
  }

  /**
   * List all files contained in the given ZIP file
   * @param fileSystem the file system that contains the zip file
   * @param zipFilePath the ZIP file to return its contents
   * @return
   */
  def listFilesInZip(fileSystem: fs.FileSystem, zipFilePath: Path): Array[(String, Long, Long)] = {
    val fileLength = fileSystem.getFileStatus(zipFilePath).getLen
    val in = fileSystem.open(zipFilePath)
    try {
      val endOfCentralDirectory = readEndOfCentralDirectory(in, fileLength)
      getZipFileContents(in, endOfCentralDirectory)
    } finally {
      in.close()
    }
  }

  /**
   * Locates and reads the end-of-central-directory record in the given ZIP file.
   * @param in the input ZIP file
   * @param fileSize the total file size to locate the end-of-central-directory at the end.
   * @return
   */
  private def readEndOfCentralDirectory(in: FSDataInputStream, fileSize: Long): EndOfCentralDirectory = {
    var eocdSignature: Array[Byte] = Array(0x50, 0x4B, 0x05, 0x06)
    val buffer: Array[Byte] = new Array[Byte](2048)
    var bufferPosInFile: Long = fileSize
    var searchPosInArray: Int = -1

    def findSignature: Boolean = {
      var signatureFound = false
      while (!signatureFound && bufferPosInFile + searchPosInArray >= 0) {
        if (searchPosInArray < 0) {
          // Need to read more data
          // Shift 1024 bytes of the array to the right to make room for new data from file
          val len: Int = (1024L min bufferPosInFile).toInt
          System.arraycopy(buffer, 0, buffer, len, 1024)
          searchPosInArray += len
          // Read the next batch from file
          bufferPosInFile -= len
          in.seek(bufferPosInFile)
          in.readFully(buffer, 0, len)
        }
        if (buffer(searchPosInArray) == eocdSignature(0) && buffer(searchPosInArray + 1) == eocdSignature(1) &&
          buffer(searchPosInArray + 2) == eocdSignature(2) && buffer(searchPosInArray + 3) == eocdSignature(3)) {
          signatureFound = true
        } else {
          searchPosInArray -= 1
        }
      }
      signatureFound
    }

    if (!findSignature)
      throw new IllegalArgumentException("Could not find central directory offset in the zip file.")

    // Found the signature, now read the record
    val endOfCentralDirectory = new EndOfCentralDirectory
    endOfCentralDirectory.read(buffer, searchPosInArray, buffer.length)
    // Check for ZIP64 format
    if (endOfCentralDirectory.numEntries == -1 || endOfCentralDirectory.offsetOfCentralDirInDisk == -1 ||
        endOfCentralDirectory.centralDirSize == -1) {
      // Must read the extended ZIP64 end of central directory record
      // Continue searching for ZIP64 end of central directory locator
      eocdSignature = Array(0x50, 0x4B, 0x06, 0x07)
      if (!findSignature)
        throw new IllegalArgumentException("Could not find the ZIP64 end of central directory marker")
      // Parse the record locator (See Section 4.3.15)
      endOfCentralDirectory.diskNumCentralDir = getIntLittleEndian(buffer, searchPosInArray + 4)
      val eocdOffset: Long = getLongLittleEndian(buffer, searchPosInArray + 0x8)
      in.seek(eocdOffset)
      endOfCentralDirectory.read64(in)
    }
    endOfCentralDirectory
  }

  /**
   * Extracts the contents of the ZIP file in the form of triplets (filename, offset, length).
   * The length indicates the compressed file size.
   * @param in the input file stream
   * @param endOfCentralDirectory the end of central directory record
   * @return the list of all files in the format (filename, offset, length) where length is the decompressed size
   */
  private def getZipFileContents(in: FSDataInputStream, endOfCentralDirectory: EndOfCentralDirectory): Array[(String, Long, Long)] = {
    // Seek to the central directory to read the contents
    in.seek(endOfCentralDirectory.offsetOfCentralDirInDisk)
    assert(endOfCentralDirectory.numEntries <= Int.MaxValue,
      s"Too many entries in ZIP file ${endOfCentralDirectory.numEntries} > ${Int.MaxValue}")
    val fileEntries = new Array[(String, Long, Long)](endOfCentralDirectory.numEntries.toInt)
    var iEntry: Int = 0
    val fileHeader = new CentralDirectoryFileHeader
    while (iEntry < endOfCentralDirectory.numEntries) {
      assert(in.getPos < endOfCentralDirectory.offsetOfCentralDirInDisk + endOfCentralDirectory.centralDirSize)
      fileHeader.read(in)
      // Not, the second entry in the tuple points to the local header offset not the file offset
      fileEntries(iEntry) = (fileHeader.filename, fileHeader.offsetLocalHeader, fileHeader.uncompressedSize)
      iEntry += 1
    }
    assert(iEntry == endOfCentralDirectory.numEntries)

    // Retrieve the offset of the file from the local header
    getEntryPositions(in, fileEntries)

    fileEntries
  }

  /**
   * Add a file to the given ZIP file using [[ZipEntry.STORED]] method, i.e., no compression.
   * @param zip the ZIP file to write the entry to
   * @param filename the name of the entry in the ZIP file
   * @param data the binary data of the file
   */
  def putStoredFile(zip: ZipOutputStream, filename: String, data: Array[Byte]): Unit = {
    val entry = new ZipEntry(filename)
    entry.setMethod(ZipEntry.STORED)
    entry.setSize(data.length)
    val crc32 = new CRC32
    crc32.update(data)
    entry.setCrc(crc32.getValue)
    zip.putNextEntry(entry)
    zip.write(data)
    zip.closeEntry()
  }

  def putStoredFile(zip: org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream,
                    filename: String, data: Array[Byte]): Unit = {
    val entry = new ZipArchiveEntry(filename)
    entry.setMethod(ZipEntry.STORED)
    entry.setSize(data.length)
    val crc32 = new CRC32
    crc32.update(data)
    entry.setCrc(crc32.getValue)
    zip.putArchiveEntry(entry)
    zip.write(data)
    zip.closeArchiveEntry()
  }

  /**
   * Parses a short value in little-endian format from the bytes at offset and offset+1.
   * @param byteArray the array of bytes to parse
   * @param offset the position of the array of the first byte to parse.
   * @return
   */
  private def getShortLittleEndian(byteArray: Array[Byte], offset: Int): Short = {
    val lowerByte = byteArray(offset) & 0xFF
    val upperByte = byteArray(offset + 1) & 0xFF

    ((upperByte << 8) | lowerByte).toShort
  }

  def getIntLittleEndian(byteArray: Array[Byte], offset: Int): Int = {
    val byte1 = byteArray(offset) & 0xFF
    val byte2 = byteArray(offset + 1) & 0xFF
    val byte3 = byteArray(offset + 2) & 0xFF
    val byte4 = byteArray(offset + 3) & 0xFF

    (byte4 << 24) | (byte3 << 16) | (byte2 << 8) | byte1
  }

  def getLongLittleEndian(byteArray: Array[Byte], offset: Int): Long = {
    val byte1 = byteArray(offset) & 0xFF
    val byte2 = byteArray(offset + 1) & 0xFF
    val byte3 = byteArray(offset + 2) & 0xFF
    val byte4 = byteArray(offset + 3) & 0xFF
    val byte5 = byteArray(offset + 4) & 0xFF
    val byte6 = byteArray(offset + 5) & 0xFF
    val byte7 = byteArray(offset + 6) & 0xFF
    val byte8 = byteArray(offset + 7) & 0xFF

    (byte8 << 56) | (byte7 << 48) | (byte6 << 40) | (byte5 << 32) |
      (byte4 << 24) | (byte3 << 16) | (byte2 << 8) | byte1
  }

  def putShortLittleEndian(byteArray: Array[Byte], offset: Int, value: Short): Unit = {
    byteArray(offset) = (value & 0xFF).toByte
    byteArray(offset + 1) = ((value >> 8) & 0xFF).toByte
  }

  def putIntLittleEndian(byteArray: Array[Byte], offset: Int, value: Int): Unit = {
    byteArray(offset) = (value & 0xFF).toByte
    byteArray(offset + 1) = ((value >> 8) & 0xFF).toByte
    byteArray(offset + 2) = ((value >> 16) & 0xFF).toByte
    byteArray(offset + 3) = ((value >> 24) & 0xFF).toByte
  }

  def putLongLittleEndian(byteArray: Array[Byte], offset: Int, value: Long): Unit = {
    byteArray(offset) = (value & 0xFF).toByte
    byteArray(offset + 1) = ((value >> 8) & 0xFF).toByte
    byteArray(offset + 2) = ((value >> 16) & 0xFF).toByte
    byteArray(offset + 3) = ((value >> 24) & 0xFF).toByte
    byteArray(offset + 4) = ((value >> 32) & 0xFF).toByte
    byteArray(offset + 5) = ((value >> 40) & 0xFF).toByte
    byteArray(offset + 6) = ((value >> 48) & 0xFF).toByte
    byteArray(offset + 7) = ((value >> 56) & 0xFF).toByte
  }
}
