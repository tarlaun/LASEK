package edu.ucr.cs.bdlab.beast.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.shaded.org.apache.commons.net.ftp.{FTPClient, FTPReply}
import org.apache.spark.internal.Logging

import java.io.{BufferedInputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URI}
import java.util.regex.{Matcher, Pattern}
import java.util.zip.GZIPInputStream

object WebUtil extends Logging {

  /**
   * Downloads a file from a URL or a file and writes up-to n bytes to the given output stream. If the number of bytes
   * is zero, it will retrieve the entire file.
   * @param url the url or path to retrieve
   * @param numBytes the number of bytes to read or 0 to read all bytes
   * @param out the output stream to write all output to
   */
  def retrieveFileFromURL(url: String, numBytes: Long, out: OutputStream): Unit = {
    def createDecompressedInputStream(inputStream: InputStream): InputStream = url match {
      case _ if url.endsWith(".bz2") => new BZip2CompressorInputStream(inputStream)
      case _ if url.endsWith(".gz") => new GZIPInputStream(inputStream)
      case _ => inputStream // No decompression needed
    }
    var ftpClient: FTPClient = null
    var rawInputStream: InputStream = null
    try {
      if (url.startsWith("http")) {
        // Handle files served on HTTP
        var downloadURL = new URI(getDownloadURL(url)).toURL
        var urlConnection = downloadURL.openConnection
        urlConnection.connect()
        urlConnection match {
          case connection: HttpURLConnection =>
            var httpURLConnection = connection
            while (httpURLConnection.getResponseCode == 301 || httpURLConnection.getResponseCode == 302 || httpURLConnection.getResponseCode == 304) {
              // Follow redirect
              downloadURL = new URI(urlConnection.getHeaderField("Location")).toURL
              logInfo(s"${httpURLConnection.getResponseCode}: Redirecting ... '$downloadURL'")
              httpURLConnection = downloadURL.openConnection.asInstanceOf[HttpURLConnection]
              urlConnection = httpURLConnection
              urlConnection.connect()
            }
            if (httpURLConnection.getResponseCode != 200) throw new RuntimeException("Unexpected response code " + httpURLConnection.getResponseCode)
          case _ =>
        }
        rawInputStream = new BufferedInputStream(downloadURL.openStream)
      } else if (url.startsWith("ftp")) {
        // Handle files served on FTP
        val ftpURL = new URI(url).toURL
        ftpClient = new FTPClient
        if (ftpURL.getPort == -1) ftpClient.connect(ftpURL.getHost)
        else ftpClient.connect(ftpURL.getHost, ftpURL.getPort)
        if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode)) throw new RuntimeException(String.format("Error connecting to FTP server at URL '%s'", ftpURL))
        ftpClient.login("anonymous", null)
        ftpClient.changeWorkingDirectory(ftpURL.getPath)
        val files = ftpClient.listFiles(ftpURL.getPath)
        for (file <- files; if rawInputStream == null) {
          if (file.isFile) {
            // Download this file
            rawInputStream = new BufferedInputStream(ftpClient.retrieveFileStream(file.getName))
          }
        }
      } else {
        // Assume that the source is a file available in the file system. Copy it
        val sourcePath = new Path(url)
        val sourceFS = sourcePath.getFileSystem(new Configuration())
        rawInputStream = sourceFS.open(sourcePath)
      }
      val decompressedInputStream: InputStream = createDecompressedInputStream(rawInputStream)
      if (numBytes == 0)
        IOUtils.copyBytes(decompressedInputStream, out, 1024 * 1024, false)
      else
        IOUtils.copyBytes(decompressedInputStream, out, numBytes, false)
    } finally {
      if (ftpClient != null && ftpClient.isConnected)
        ftpClient.disconnect()
      if (rawInputStream != null)
        rawInputStream.close()
    }
  }

  /**
   * Download the given file to the download directory using a pure Java method.
   *
   * @param url          the download URL
   * @param downloadPath path to a directory to download the file into
   */
  def downloadFile(url: String, downloadPath: Path, downloadFS: FileSystem): Unit = {
    if (url.startsWith("http")) {
      // Handle files served on HTTP
      var downloadURL = new URI(getDownloadURL(url)).toURL
      var urlConnection = downloadURL.openConnection
      urlConnection.connect()
      if (urlConnection.isInstanceOf[HttpURLConnection]) {
        var httpURLConnection = urlConnection.asInstanceOf[HttpURLConnection]
        while (httpURLConnection.getResponseCode == 301 || httpURLConnection.getResponseCode == 302 || httpURLConnection.getResponseCode == 304) {
          // Follow redirect
          downloadURL = new URI(urlConnection.getHeaderField("Location")).toURL
          logInfo(s"${httpURLConnection.getResponseCode}: Redirecting ... '$downloadURL'")
          httpURLConnection = downloadURL.openConnection.asInstanceOf[HttpURLConnection]
          urlConnection = httpURLConnection
          urlConnection.connect()
        }
        if (httpURLConnection.getResponseCode != 200) throw new RuntimeException("Unexpected response code " + httpURLConnection.getResponseCode)
      }
      val contentDisposition: String = urlConnection.getHeaderField("Content-Disposition")
      var matcher: Matcher = null
      var filename: String = null
      if (contentDisposition != null && {
        matcher = FileNameRegexHTTPHeader.matcher(contentDisposition);
        matcher
      }.matches) {
        // Get the filename from the response header
        filename = matcher.group(1)
      } else {
        // Get the filename from the URL
        filename = new Path(url).getName
        if (!filename.matches(FilenameRegex)) {
          // Generate a random filename that does not exist
          do {
            filename = f"part-${(Math.random * 100000).toInt}%05d"
          } while (downloadFS.exists(new Path(downloadPath, filename)))
        }
      }
      val outputFile = new Path(downloadPath, filename)
      val inputStream = new BufferedInputStream(downloadURL.openStream)
      // Create channel to the output file
      val out = downloadFS.create(outputFile, true)
      IOUtils.copyBytes(inputStream, out, 1024 * 1024, true)
    } else if (url.startsWith("ftp")) {
      // Handle files served on FTP
      val ftpURL = new URI(url).toURL
      val ftpClient = new FTPClient
      try {
        if (ftpURL.getPort == -1) ftpClient.connect(ftpURL.getHost)
        else ftpClient.connect(ftpURL.getHost, ftpURL.getPort)
        if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode)) throw new RuntimeException(String.format("Error connecting to FTP server at URL '%s'", ftpURL))
        ftpClient.login("anonymous", null)
        ftpClient.changeWorkingDirectory(ftpURL.getPath)
        val files = ftpClient.listFiles(ftpURL.getPath)
        for (file <- files) {
          if (file.isFile) {
            // Download this file
            val inputStream = new BufferedInputStream(ftpClient.retrieveFileStream(file.getName))
            val outputFile = new Path(downloadPath, file.getName)
            // Create channel to the output file
            val out = downloadFS.create(outputFile, true)
            IOUtils.copyBytes(inputStream, out, 1024 * 1024, true)
          }
        }
      } finally if (ftpClient.isConnected) ftpClient.disconnect()
    } else {
      // Assume that the source is a file available in the file system. Copy it
      val sourcePath = new Path(url)
      val sourceFS = sourcePath.getFileSystem(downloadFS.getConf)
      downloadFS.mkdirs(downloadPath)
      // Copy the entire source directory or file into the target directory
      IOUtil.deepCopy(sourceFS, sourcePath, downloadFS, downloadPath)
    }
  }


  /** A regular expression that matches a valid file name */
  private[beast] val FilenameRegex = "[^\"\'\\?\\*\\/\\\\]+"

  /** A regular expression that matches the filename in the HTTP response header Content-Disposition. */
  private[beast] val FileNameRegexHTTPHeader = Pattern.compile(String.format(".*filename=[\\\"\\\']?(%s)[\\\"\\\']?.*", FilenameRegex))

  /** A list of regular expressions for Google Drive links with file ID */
  private[beast] val GoogleIDRegex = Array(Pattern.compile(".*id=([^\\/\\&]+).*"), Pattern.compile(".*file/d/([^\\/\\&]+).*"))

  /** A list of regular expressions for Dropbox links with file ID */
  private[beast] val DropboxIDRegex = Array(Pattern.compile(".*dropbox.com/([^\\&\\?]+).*"))

  /**
   * Get the URL form which the file can be downloaded directly using a simple HTTP GET command.
   *
   * @param url the URL given by the user
   * @return an updated URL that can be used to access the file
   */
  private def getDownloadURL(url: String): String = {
    if (url.contains("drive.google.com")) {
      // Google drive link
      for (googleDrive <- GoogleIDRegex) {
        val matcher = googleDrive.matcher(url)
        if (matcher.matches) {
          val googleID = matcher.group(1)
          val google_url = String.format("https://docs.google.com/uc?export=download&id=%s", googleID)
          return google_url
        }
      }
      throw new RuntimeException(String.format("Cannot parse Google URL '%s'", url))
    } else if (url.contains("onedrive.live.com")) {
      // OneDrive link
      var one_url = url.substring(url.indexOf("file/d/"))
      one_url = one_url.substring(7).trim
      one_url = String.format("https://onedrive.live.com/download?cid=%s", one_url)
      one_url
    } else if (url.contains("dropbox.com")) {
      // Dropbox link
      for (dropboxLink <- DropboxIDRegex) {
        val matcher = dropboxLink.matcher(url)
        if (matcher.matches) {
          val dropboxID = matcher.group(1)

          val dropboxURL = String.format("https://www.dropbox.com/%s?dl=1", dropboxID)
          return dropboxURL
        }
      }
      throw new RuntimeException(String.format("Cannot parse Dropbox URL '%s'", url))
    }
    else {
      // Direct link
      url
    }
  }

  def scalaMapToJsonString(map: Map[String, Any]): String = {
    val jsonNode = scalaMapToJsonNode(map)
    jsonNode.toString
  }

  def scalaMapToJsonNode(map: Map[String, Any]): com.fasterxml.jackson.databind.node.ObjectNode = {
    val objectMapper = new ObjectMapper()
    val jsonNode: ObjectNode = objectMapper.createObjectNode()

    def convertArray(seq: Array[_]): com.fasterxml.jackson.databind.node.ArrayNode = {
      val arrayNode = objectMapper.createArrayNode()
      seq.foreach({
        case x: Array[_] => arrayNode.add(convertArray(x))
        case m: Map[String, _] => arrayNode.add(WebUtil.scalaMapToJsonNode(m))
        case x: String => arrayNode.add(x)
        case x: Byte => arrayNode.add(x)
        case x: Short => arrayNode.add(x)
        case x: Int => arrayNode.add(x)
        case x: Long => arrayNode.add(x)
        case x: Float => arrayNode.add(x)
        case x: Double => arrayNode.add(x)
        case x: Boolean => arrayNode.add(x)
      })
      arrayNode
    }

    map.foreach {
      case (key: String, value: Map[String, _]) => jsonNode.set(key, WebUtil.scalaMapToJsonNode(value))
      case (key: String, value: Array[_]) => jsonNode.set(key, convertArray(value))
      case (key: String, value: String) => jsonNode.put(key, value)
      case (key: String, value: Byte) => jsonNode.put(key, value)
      case (key: String, value: Short) => jsonNode.put(key, value)
      case (key: String, value: Int) => jsonNode.put(key, value)
      case (key: String, value: Long) => jsonNode.put(key, value)
      case (key: String, value: Float) => jsonNode.put(key, value)
      case (key: String, value: Double) => jsonNode.put(key, value)
      case (key: String, value: Boolean) => jsonNode.put(key, value)
    }

    jsonNode
  }

  /**
   * Converts a JSON node to a corresponding Scala object.
 *
   * @param node A parsed JSON node
   * @return the value of the given node converted to a Scala object.
   */
  def jsonNodeToScala(node: JsonNode): Any = {
    import scala.collection.JavaConverters._
    if (node.isObject) {
      node.fieldNames.asScala.map { fieldName =>
        fieldName -> WebUtil.jsonNodeToScala(node.get(fieldName))
      }.toMap
    } else if (node.isArray) {
      node.elements.asScala.map(WebUtil.jsonNodeToScala).toArray
    } else if (node.isTextual) {
      node.asText()
    } else if (node.isInt) {
      node.asInt()
    } else if (node.isLong) {
      node.asLong()
    } else if (node.isDouble) {
      node.asDouble()
    } else if (node.isBoolean) {
      node.asBoolean()
    } else {
      null
    }
  }
}
