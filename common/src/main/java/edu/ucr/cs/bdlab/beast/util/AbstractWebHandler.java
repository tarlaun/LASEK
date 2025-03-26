/*
 * Copyright 2018 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * An abstract class that handles basic web requests. A setup function can be optionally configured to
 * setup the server. In addition, all methods annotated with {@link WebMethod} will be treated as an HTTP
 * entry point.
 */
public abstract class AbstractWebHandler {
  private static final Log LOG = LogFactory.getLog(AbstractWebHandler.class);

  /** The creation timestamp of the server is used to timestamp all resources loaded from the class path */
  protected long startTimestamp;

  /** Milliseconds in one day. Used for client-cache expiration */
  protected static final long OneDay = 24L * 60 * 60 * 1000;

  /**Environment configuration for creating file systems*/
  protected Configuration conf;

  /**
   * Retrieves the IP address of the requester
   * Edited from https://gist.github.com/nioe/11477264
   * @param request the request to handle
   * @return the IP address of the remote host if it could be read from the request header
   */
  public static String getRemoteIpFrom(HttpServletRequest request) {
    // A list of headers that might contain the source IP
    final String[] ipHeaders = {
        "X-Forwarded-For",
        "Proxy-Client-IP",
        "WL-Proxy-Client-IP",
        "HTTP_CLIENT_IP",
        "HTTP_X_FORWARDED_FOR"
    };
    String ip = request.getRemoteAddr();
    int tryCount = 0;
    while (tryCount < ipHeaders.length && !isIpFound(ip)) {
      ip = request.getHeader(ipHeaders[tryCount]);
      tryCount++;
    }

    return ip;
  }

  /**
   * Checks if the given IP string is a correct IP address or an invalid one
   * @param ip the IP address to test
   * @return {@code true} if the given IP address is a valid IP address
   */
  private static boolean isIpFound(String ip) {
    final String UNKNOWN = "unknown";
    return ip != null && ip.length() > 0 && !UNKNOWN.equalsIgnoreCase(ip);
  }

  /**
   * Report the error back to the web browser
   * @param response the HTTP response to write the error to
   * @param msg the message to include
   * @param e the exception to report its stack trace
   * @throws IOException if an error happens while writing the output
   */
  public static void reportError(HttpServletResponse response, String msg, Exception e) throws IOException {
    if (e != null)
      e.printStackTrace();
    LOG.error(msg);
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    response.getWriter().println("{\"message\": '" + msg + "',");
    if (e != null) {
      response.getWriter().println("\"error\": '" + e.getMessage() + "',");
      response.getWriter().println("\"stacktrace\": [");
      for (StackTraceElement trc : e.getStackTrace()) {
        response.getWriter().println("'" + trc.toString() + "',");
      }
      response.getWriter().println("],");
      response.getWriter().print("\"full-stacktrace\": \"");
      e.printStackTrace(response.getWriter());
      response.getWriter().println("\"");
    }
    response.getWriter().println("}");
  }
  public static void reportError(HttpServletResponse response, String msg) throws IOException {
    reportError(response, msg, null);
  }

  /**
   * Sets up this operation from user-provided options.
   * @param sc the spark context
   * @param opts the user options
   */
  public void setup(SparkContext sc, BeastOptions opts) {
    this.startTimestamp = System.currentTimeMillis();
    this.conf = opts.loadIntoHadoopConf(sc.hadoopConfiguration());
  }

  /**
   * Set up the server from a SparkSession
   * @param ss the SparkSession to use for setting up this web handler
   * @param opts additional options
   */
  public void setup(SparkSession ss, BeastOptions opts) {
    this.setup(ss.sparkContext(), opts);
  }

  public boolean handleStaticResource(String target, HttpServletRequest request, HttpServletResponse response,
                                      boolean absolute) throws IOException {
    // Will hold the timestamp of the actual resource (file) served by this request
    long resourceTimestamp;
    // The timestamp of the most recent cached version at the client (if any)
    long requesterCachedTimestamp = (request.getHeader("If-Modified-Since") != null)?
        request.getDateHeader("If-Modified-Since") : 0;
    InputStream resourceStream = null;
    try {
      // Try to get the file from the classpath which includes the JAR file
      resourceStream = this.getClass().getResourceAsStream(target);
      // If successful, mark the timestamp of the resource as the start time of the server
      resourceTimestamp = startTimestamp;
      if (resourceStream == null) {
        // Try to load it as a regular file
        // Remove leading slash to avoid reading from the root
        if (!absolute && target.startsWith("/"))
          target = target.substring(1);
        Path p = new Path(target);
        FileSystem fs = p.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(p);
        System.out.println("Locating file "+p);
        if (!fileStatus.isFile()) {
          reportMissingFile("/" + target, response);
          return false;
        }
        resourceTimestamp = fileStatus.getModificationTime();
        if (resourceTimestamp > requesterCachedTimestamp)
          resourceStream = fs.open(p); // Newer version is available, send it
      }
      if (resourceTimestamp <= requesterCachedTimestamp) {
        // No new version is available, return 304 (not modified) response.
        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
        return true;
      }
      // Set the response headers
      if (target.endsWith(".js"))
        response.setContentType("application/javascript");
      else if (target.endsWith(".css"))
        response.setContentType("text/css");
      else if (target.endsWith(".yaml"))
        response.setContentType("application/x-yaml");
      else if (target.endsWith(".svg"))
        response.setContentType("image/svg+xml");
      else if (target.endsWith(".mvt"))
          response.setContentType("application/vnd.mapbox-vector-tile");
      else if (target.endsWith(".pbf"))
        response.setContentType("application/octet-stream");
      else
        response.setContentType(URLConnection.guessContentTypeFromName(target));
      // Set file modification time to allow client-side (browser) caching
      response.addDateHeader("Last-Modified", resourceTimestamp);
      response.addDateHeader("Expires", resourceTimestamp + OneDay);

      // Write the content
      byte[] buffer = new byte[1024 * 1024];
      ServletOutputStream outResponse = response.getOutputStream();
      int size;
      while ((size = resourceStream.read(buffer)) != -1) {
        outResponse.write(buffer, 0, size);
      }
      outResponse.close();
      response.setStatus(HttpServletResponse.SC_OK);
      return true;
    } catch (IOException e) {
      reportMissingFile(target, response);
      return false;
    } finally {
      if (resourceStream != null)
        resourceStream.close();
    }
  }

  protected void reportMissingFile(String target, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
  }

  protected void ensureRequiredParameters(HttpServletRequest request, HttpServletResponse response, String[] requiredParameters) throws IOException {
    Enumeration enumeration = request.getParameterNames();
    List<String> requestParameters = new ArrayList<>();
    while (enumeration.hasMoreElements()) {
      requestParameters.add((String) enumeration.nextElement());
    }
    for (String requiredParam : requiredParameters) {
      if (!requestParameters.contains(requiredParam))
        reportError(response, String.format("Missing parameters '%s'", requiredParam), null);
    }
  }

  public static boolean isGZIPAcceptable(HttpServletRequest request) {
    boolean serverAcceptGZIP = false;
    Enumeration serverAccepts = request.getHeaders("Accept-Encoding");
    while (serverAccepts.hasMoreElements()) {
      String serverAccept = (String) serverAccepts.nextElement();
      if (serverAccept.toLowerCase().contains("gzip"))
        serverAcceptGZIP = true;
    }
    return serverAcceptGZIP;
  }
}
