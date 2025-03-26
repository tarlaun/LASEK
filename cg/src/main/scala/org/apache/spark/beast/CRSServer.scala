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
package org.apache.spark.beast

import edu.ucr.cs.bdlab.beast.util.OperationParam

import javax.servlet.http
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.http.HttpEntity
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.eclipse.jetty.server.{Request, Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.net.URL
import java.util.regex.Pattern

/**
 * A server that handles non-standard coordinate reference systems (CRS) between executor
 * nodes and the driver. It has three main methods.
 * - `setPort(port: Int)` - Sets the port where the server is listening (or should listen)
 * - `startServer` - Starts the server. Should run on the driver node before the executors start.
 * - `stopServer` - Stops the server
 * - `crsToSRID(crs: CoordinateReferenceSystem): Int` - puts the given CRS into the cache
 *     and returns a unique ID for it.
 * - `sridToCRS(srid: Int): CoordinateReferenceSystem` - Returns the CRS with the given SRID if it exists.
 */
object CRSServer extends Logging {

  @OperationParam(description = "The port on which the CRSServer is listening", defaultValue = "21345")
  val CRSServerPort = "crsserver.port"

  @OperationParam(description = "The host on which the CRSServer is running", defaultValue = "127.0.0.1")
  val CRSServerHost = "crsserver.host"

  val DefaultPort: Int = 21345

  lazy val serverPort: Int = TaskContext.get().getLocalProperty(CRSServerPort).toInt

  lazy val isServerRunning: Boolean = TaskContext.get() != null && TaskContext.get().getLocalProperty(CRSServerPort) != null

  lazy val serverAddress: String = TaskContext.get().getLocalProperty(CRSServerHost)

  /**The running Jetty server instance*/
  var server: Server = _

  /**
   * A cache (at both the client and server) to speed up [[crsToSRID()]] calls.
   * It contains pairs of (CRS, SRID) whether the CRS is retrieved from the server or from [[CRS.lookupEpsgCode()]]
   */
  private[beast] val crsCache: scala.collection.mutable.Map[CoordinateReferenceSystem, Int] =
    new scala.collection.mutable.HashMap[CoordinateReferenceSystem, Int]()

  /**
   * The reverse cache maps each SRID to a CRS to speed up [[sridToCRS()]] calls.
   */
  private[beast] val sridCache: scala.collection.mutable.Map[Int, CoordinateReferenceSystem] =
    new scala.collection.mutable.HashMap[Int, CoordinateReferenceSystem]()

  /**A pattern for HTTP requests*/
  val pattern: Pattern = Pattern.compile("(?i)/?crs(/?(-?\\d+))?/?")

  /**
   * Starts the server and returns the port on which it is listening
   * @return the port on which the server is started
   */
  def startServer(defaultPort: Int = DefaultPort): Int = {
    val numThreads: Int = Runtime.getRuntime.availableProcessors() * 2
    val threadPool = new QueuedThreadPool(numThreads * 2, numThreads)
    server = new Server(threadPool)
    val connector = new ServerConnector(server)
    server.addConnector(connector)
    var started: Boolean = false
    var attempt: Int = 0
    // Try several ports starting with the default port until an open port is found
    do {
      try {
        connector.setPort(defaultPort + attempt)
        val servletHandler = new ServletHandler
        server.setHandler(servletHandler)
        //server.setThreadPool(threadPool)
        server.start()
        // Wait until the server is running
        while (!server.isRunning)
          Thread.sleep(1000)
        // If no exception was thrown, then store this port and use it
        val port: Int = defaultPort + attempt
        logInfo(s"Successfully started CRSServer on port $port")

        // Register the servlets
        servletHandler.addServletWithMapping(new ServletHolder("insert", insertServlet), "/crs")
        servletHandler.addServletWithMapping(new ServletHolder("retrieve", retrieveServlet), "/crs/*")
        started = true
        return port
      } catch {
        case e: IOException =>
          logWarning(s"Could not start CRSServer on port ${defaultPort + attempt}", e)
          attempt += 1
      }
    } while (!started && attempt < 10)
    logWarning("Could not start CRSServer on all attempted ports. Failing ...")
    -1
  }

  /**
   * Start the CRS server and store the information in the configuration of the given [[SparkContext]]
   * @param sc the spark context to store the server information in
   * @return `true` if the server starts correctly. `false` otherwise.
   */
  def startServer(sc: SparkContext): Boolean = {
    // Start the server and get the port
    val crsServerPort = CRSServer.startServer(sc.getConf.getInt(CRSServer.CRSServerPort, DefaultPort))
    // Port -1 indicates that the server did not start correctly
    if (crsServerPort == -1)
      return false
    // Close the CRSServer on application exit
    stopOnExit(sc)
    // Set the CRSServer information as a local property to be accessed by the workers
    sc.setLocalProperty(CRSServer.CRSServerPort, crsServerPort.toString)
    sc.setLocalProperty(CRSServer.CRSServerHost, java.net.InetAddress.getLocalHost.getHostAddress)
    true
  }

  def startServer(jsc: JavaSparkContext): Boolean = startServer(jsc.sc)

  /**
   * Register a listener in the given [[SparkContext]] to stop the CRSServer on application exit.
   * @param sc the [[SparkContext]] to register the closing listener to
   */
  private def stopOnExit(sc: SparkContext): Unit = {
    // Close the CRSServer upon application end
    sc.addSparkListener(new SparkListener() {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
        CRSServer.stopServer()
    })
  }

  def stopServer(wait: Boolean = false): Unit = {
    if (server == null)
      return
    server.stop()
    if (wait)
      server.join()
  }

  /**
   * Inserts a CRS given in the HTTP request into the cache. If the CRS is already in the cache, its key (SRID) is
   * returned in the response. If it does not exist, it is inserted in the cache with a new ID and the ID is returned.
   */
  val insertServlet: javax.servlet.Servlet = new javax.servlet.http.HttpServlet() {
    override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      logInfo("Handling insert")
      // First, parse the CRS from the request
      val length = request.getContentLength
      val crsArray = new Array[Byte](length)
      val inputStream = request.getInputStream
      var offset = 0
      while (offset < length)
        offset += inputStream.read(crsArray, offset, length - offset)
      inputStream.close()
      try {
        val in = new ObjectInputStream(new ByteArrayInputStream(crsArray))
        val crs = in.readObject().asInstanceOf[CoordinateReferenceSystem]
        logDebug(s"Handling insert of crs '$crs'")
        // Use the local cache to retrieve or assigned an SRID for this CRS
        // This whole method is synchronized to avoid multiple inserts of CRS into the cache
        var srid = 0
        crsCache.synchronized {
          val sridO = crsCache.get(crs)
          if (sridO.isDefined) srid = sridO.get else {
            // Not found, try to get an EPSG code using full search
            // Note: We do not try the quick search since it must have been tried at the client already
            val epsgCode = CRS.lookupEpsgCode(crs, true)
            if (epsgCode != null) srid = epsgCode else {
              // No EPSG code, assign it a custom negative SRID
              srid = if (crsCache.isEmpty) -1 else (crsCache.values.min min 0) - 1
            }
            assert(srid != 0, "A valid CRS should never get assigned SRID=0")
            logDebug(s"New SRID $srid assigned to CRS '${crs.toWKT}'")
            // Update the two caches
            crsCache.put(crs, srid)
            sridCache.put(srid, crs)
          }
        }
        // Return the SRID in the response
        request.asInstanceOf[Request].setHandled(true)
        response.setStatus(http.HttpServletResponse.SC_OK)
        response.setContentType("text/plain")
        val writer = response.getWriter
        writer.print(srid)
        writer.close()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          response.setStatus(http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
          response.setContentType("text/plain")
          val writer = response.getWriter
          e.printStackTrace(writer)
          writer.close()
        }
      }
    }

  /**
   * Handles the service of retrieving a CRS for an assigned SRID.
   * This method is expected to be called only for a negative SRID that corresponds to a custom CRS.
   * Therefore, it is expected that we will always find it in the cache since it must have been previously
   * assigned at the server.
   * If found, the CRS is serialized using Java format and returned.
   * In case it is not found, a 404 response is returned.
   */
  val retrieveServlet: javax.servlet.Servlet = new javax.servlet.http.HttpServlet() {
    override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val path = request.getPathInfo
      val srid: Int = path.substring(1).toInt
      logInfo(s"Handling retrieve of SRID: $srid")
      val crsO = sridCache.get(srid)
      if (crsO.isEmpty) {
        // Not found
        logInfo(s"SRID: $srid not found!")
        request.asInstanceOf[Request].setHandled(true)
        response.setStatus(http.HttpServletResponse.SC_NOT_FOUND)
      } else {
        // Found! Serialize to the output in a binary format
        request.asInstanceOf[Request].setHandled(true)
        response.setContentType("application/octet-stream")
        response.setStatus(http.HttpServletResponse.SC_OK)
        val responseOut = new ObjectOutputStream(response.getOutputStream)
        responseOut.writeObject(crsO.get)
        responseOut.close()
      }

    }
  }

  /**
   * Get an integer SRID that corresponds to the given CRS according to the following logic.
   *  1. If crs is null, return 0
   *  2. Search the local cache as the fastest method of known CRS.
   *  3. If not found in cache, look up the the EPSG database to find an SRID, cache, and return it.
   *  4a. If the server is running, contact the server to get the SRID
   *  4b. If the server is not running, assign a custom negative SRID and cache it
   * @param crs the CRS to find an SRID for
   * @return a unique SRID that identifies the given CRS
   */
  def crsToSRID(crs: CoordinateReferenceSystem) : Int = {
    var assignedSRID: Int = 0
    try {
      // 1. If crs is null, return 0
      if (crs == null)
        return 0
      // 2. Search the local cache as the fastest method of known CRS.
      val sridO = crsCache.get(crs)
      if (sridO.isDefined) {
        assignedSRID = sridO.get
      } else {
        // CRS not found in cache
        // 3. Try to search for an EPSG code to avoid assigning a custom ID
        val epsgCode: Integer = CRS.lookupEpsgCode(crs, false)
        // If not found even with a full scan, must assign a custom negative ID
        if (epsgCode != null) {
          assignedSRID = epsgCode
        } else if (isServerRunning) {
          // 4a. Not found, contact the server to retrieve or assign a new SRID
          val url: String = s"http://$serverAddress:$serverPort/crs"
          logInfo(s"Calling POST '$url'")
          val httpClient: HttpClient = HttpClients.createDefault()
          val httpPost: HttpPost = new HttpPost(url)
          // Serialize the CRS to send to the server
          // Note: For some rare cases, when we serialized the CRS in WKT format, the server couldn't parse it back
          val baos = new ByteArrayOutputStream()
          val out = new ObjectOutputStream(baos)
          out.writeObject(crs)
          out.close()
          httpPost.setEntity(new ByteArrayEntity(baos.toByteArray))
          val response = httpClient.execute(httpPost)
          val responseEntity: HttpEntity = response.getEntity

          if (responseEntity != null && responseEntity.getContentLength > 0) {
            val buffer = new Array[Byte](responseEntity.getContentLength.toInt)
            val inputStream = responseEntity.getContent
            var offset = 0
            while (offset < buffer.length) {
              offset += inputStream.read(buffer, offset, buffer.length - offset)
            }
            inputStream.close()
            assignedSRID = new String(buffer).toInt
          } else {
            logError(s"Did not receive a proper response for $url for CRS '$crs''")
          }
        } else {
          // 4b. Server not running, assign a custom negative SRID locally
          crsCache.synchronized {
            assignedSRID = if (crsCache.isEmpty) -1 else ((crsCache.values.min min 0) - 1)
            // Insert in both caches
            crsCache.put(crs, assignedSRID)
            sridCache.put(assignedSRID, crs)
          }
        }
        if (!sridCache.contains(assignedSRID)) {
          // Handle all cases except 4b which already updates the cache
          crsCache.synchronized {
            if (assignedSRID != 0) {
              // Insert in both caches
              crsCache.put(crs, assignedSRID)
              sridCache.put(assignedSRID, crs)
            }
          }
        }
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException("s\"Did not receive a response for CRS '$crs'", e)
        //logError(s"Did not receive a response for CRS '$crs'. Returning 0 to signal error")
    }
    assignedSRID
  }

  /**
   * Convert the given SRID to CRS according to the following logic.
   *  1. If the SRID is zero, it indicates an invalid SRID and `null` is returned.
   *  2. It searches the local cache and retrieves the SRID.
   *  3a. If SRID is positive, use it as an EPSG, retrieve the CRS, cache and return it.
   *  3b. If SRID is negative, contact the server, retrieve the CRS, cache and return it.
   * @param srid the SRID that needs to be converted to a CRS
   * @return the converted CRS.
   */
  def sridToCRS(srid: Int): CoordinateReferenceSystem = {
    try {
      // 1. Zero always indicates an invalid or unset SRID that always return null
      if (srid == 0)
        return null
      logDebug(s"Finding CRS for SRID:$srid")
      var crs: CoordinateReferenceSystem = null
      // 2. Try to the local cache since it is faster than all other methods
      val crsO = sridCache.get(srid)
      if (crsO.isDefined) {
        crs = crsO.get
      } else if (srid > 0) {
        // 3a. Not found and positive, try it as an EPSG code.
        crs = CRS.decode(s"EPSG:$srid", true)
      } else {
        // 3b. Not found and negative, contact the server
        // Request from the server
        val path: String = s"http://$serverAddress:$serverPort/crs/$srid"
        val url = new URL(path)
        logInfo(s"Calling GET '$url'")
        val inputStream = url.openStream()
        val rawCRS = new ByteArrayOutputStream()
        val buffer = new Array[Byte](4096)
        var done: Boolean = false
        while (!done) {
          val bytesRead = inputStream.read(buffer)
          if (bytesRead > 0) {
            rawCRS.write(buffer, 0, bytesRead)
          } else {
            done = true
          }
        }
        inputStream.close()
        rawCRS.close()
        val parsedBack = new ObjectInputStream(new ByteArrayInputStream(rawCRS.toByteArray))
        crs = parsedBack.readObject().asInstanceOf[CoordinateReferenceSystem]
      }
      if (crs != null) {
        // Add to local caches
        crsCache.put(crs, srid)
        sridCache.put(srid, crs)
      }
      crs
    } catch {
      case e: Exception => throw new RuntimeException(s"Error retrieving the CRS of SRID $srid", e)
    }
  }

  /**
   * Implicit conversion from Hadoop Configuration to Spark Configuration to allow the methods above to accept
   * Hadoop Configuration
   * @param hadoopConf
   * @return
   */
  implicit def sparkConfFromHadoopConf(hadoopConf: Configuration): SparkConf = {
    val sparkConf = new SparkConf()
    val hadoopConfIterator = hadoopConf.iterator()
    while (hadoopConfIterator.hasNext) {
      val hadoopConfEntry = hadoopConfIterator.next()
      sparkConf.set(hadoopConfEntry.getKey, hadoopConfEntry.getValue, true)
    }
    sparkConf
  }
}
