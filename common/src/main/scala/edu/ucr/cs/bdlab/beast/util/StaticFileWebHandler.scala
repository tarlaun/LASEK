package edu.ucr.cs.bdlab.beast.util

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, WebMethod}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import java.io.{IOException, OutputStream}

/**
 * A web handler for static files that works like a simple web server.
 * Enable this web handler with caution as it might expose unwanted files to the public.
 * For security reasons, this web handler is disabled by default, to enable, add the
 * parameter "-enableStaticFileHandling" when starting the server.
 */
class StaticFileWebHandler extends AbstractWebHandler {
  private var enabled: Boolean = _

  override def setup(sc: SparkContext, opts: BeastOptions): Unit = {
    super.setup(sc, opts)
    this.enabled = opts.getBoolean(StaticFileWebHandler.EnableStaticFileHandling, false)
  }

  @WebMethod(url = "/(.*)", order = Integer.MAX_VALUE)
  @throws(classOf[IOException])
  def handleStaticResource(target: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    if (!enabled)
      return false
    super.handleStaticResource(target, request, response, false)
  }

  /**
   * Downloads a file from a URL and writes up to n bytes to the HTTP response.
   * If the number of bytes is zero, it will retrieve the entire file.
   *
   * @param request  the HTTP request
   * @param response the HTTP response
   * @return true if the operation was successful, false otherwise
   */
  @WebMethod(url = "/fetchFile", method = "GET")
  def fetchFile(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    if (!enabled)
      return false
    try {
      response.setContentType("application/octet-stream")
      response.setStatus(HttpServletResponse.SC_OK)
      val url = request.getParameter("url")
      val numBytes = if (request.getParameter("num_bytes") == null) 4096 else request.getParameter("num_bytes").toInt
      val out: OutputStream = response.getOutputStream
      try {
        WebUtil.retrieveFileFromURL(url, numBytes, out)
      } finally {
        out.close()
      }
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  @WebMethod(url = "/listfiles(.*)", order = 100)
  @throws(classOf[IOException])
  def handleListFiles(target: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    if (!enabled)
      return false
    val conf = new Configuration()
    val path = new Path(if (target == "/listfiles") "." else target.substring(10))
    val fs = path.getFileSystem(conf)

    var result = Map[String, Any]()
    if (!fs.exists(path)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      return true
    }
    val status = fs.getFileStatus(path)
    if (status.isFile) {
      // It's a file. Return its metadata.
      result = Map[String, Any](
        "type" -> "file",
        "path" -> target,
        "length" -> status.getLen,
        "modificationTime" -> status.getModificationTime
      )
    } else {
      // It's a directory. List its contents.
      val directoryStatus = fs.listStatus(path)
      val contents = directoryStatus.map { status =>
        Map[String, Any](
          "path" -> status.getPath.toUri.getPath,
          "name" -> status.getPath.getName,
          "type" -> (if (status.isDirectory) "directory" else "file")
        )
      }
      result = Map[String, Any](
        "type" -> "directory",
        "path" -> status.getPath.toUri.getPath,
        "contents" -> contents
      )
    }
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    val out = response.getWriter
    out.print(WebUtil.scalaMapToJsonString(result))
    out.close()
    true
  }
}

// Since Scala doesn't support static fields and annotations in the same way as Java,
// we use a companion object to contain what would be static in Java.
object StaticFileWebHandler {
  @OperationParam( description = "Enable static file handling", defaultValue = "false")
  val EnableStaticFileHandling: String = "enableStaticFileHandling"
}
