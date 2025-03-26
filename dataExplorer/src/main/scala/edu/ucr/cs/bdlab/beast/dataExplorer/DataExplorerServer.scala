package edu.ucr.cs.bdlab.beast.dataExplorer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, WebMethod}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import edu.ucr.cs.bdlab.beast.io.shapefile.CompressedShapefileWriter
import edu.ucr.cs.bdlab.beast.io.{CSVFeatureWriter, FeatureWriter, GeoJSONFeatureWriter, KMLFeatureWriter, KMZFeatureWriter, SpatialFileRDD}
import edu.ucr.cs.bdlab.beast.util.{AbstractWebHandler, CounterOutputStream, WebUtil, ZipUtil}
import edu.ucr.cs.bdlab.davinci.{MVTDataVisualizer, TileIndex}
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, LongWritable}
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Geometry, TopologyException}

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * A web server for managing spatial datasets and workspaces through a RESTful API. The operations that this server provides are:
 *
 * Dataset Operations:
 *  - `GET /datasets.json`: Lists all available datasets in a JSON format.
 *  - `POST /datasets.json`: Creates a new dataset with the provided parameters. Initially, the dataset is not in any workspace.
 *  - `GET /datasets/(name).json`: Retrieves the information of the given dataset.
 *  - `GET /datasets/(name)/download.(format)`: Downloads the given datasets (or part of it) in the given format.
 *  - `GET /datasets/(name)/process`: Processes the given dataset from where it stopped.
 *  - `DELETE /datasets/(name)`: Deletes the given dataset completely and removes all its files from all workspaces.
 *  - `GET /datasets/(name)/tiles.json`: Retrieves information about tiled visualization for a dataset within a workspace.
 *  - `GET /datasets/(name)/tiles/tile-z-x-y.(format)`: Retrieves a specific tile in a format for a dataset within a workspace.
 *  - `GET /datasets/dangling.json`: Lists all datasets that are not included in any workspaces.
 *
 * Workspace Operations:
 *  - `GET /workspaces.json`: Lists all available workspaces in a JSON format.
 *  - `POST /workspaces.json`: Creates a new workspace with the provided parameters.
 *  - `GET /workspaces/(id).json`: Retrieves information about the given workspace.
 *  - `PUT /workspaces/(id).json`: Updates information about the given workspace.
 *  - `DELETE /workspaces/(id)`: Deletes the given workspace, removing associated datasets from it.
 *
 * Workspace-Dataset Specific Operations:
 *  - `GET /workspaces/(id)/datasets.json`: Lists all datasets in the specified workspace.
 *  - `GET /workspaces/(id)/datasets/(name)/style.json`: Retrieves the visualization style for a dataset within a workspace.
 *  - `PUT /workspaces/(id)/datasets/(name)/style.json`: Updates the visualization style for a dataset within a workspace.
 *  - `POST /workspaces/(id)/datasets/(name)`: Adds a dataset to a workspace.
 *  - `DELETE /workspaces/(id)/datasets/(name)`: Removes a dataset from a workspace.
 *
 * All information about the datasets and workspaces are stored in an internal embedded database to be accessible across runs.
 * The contents of those datasets are stored on the default file system, i.e., either local FS or HDFS.
 *
 * To test adding a dataset, use the following examples:
 * <pre>
    curl -X POST http://localhost:8890/datasets.json \
    -H "Content-Type: application/json" \
    -d '{"name": "airports", "source_uri": "https://star.cs.ucr.edu/datasets/NE/airports/download.geojson.gz", "source_format": "geojson"}'

    curl -X POST http://localhost:8890/datasets.json \
       -H "Content-Type: application/json" \
        -d '{"name": "ports", "source_uri": "https://star.cs.ucr.edu/datasets/NE/ports/download.csv.gz", "source_format": "csv", "beast_options": {"header": true, "geometry_type": "point", "separator": ",", "dimensions": "x,y"}}'

    curl -X POST http://localhost:8890/datasets.json \
       -H "Content-Type: application/json" \
        -d '{"name": "countries", "source_uri": "https://star.cs.ucr.edu/datasets/NE/countries/download.csv.gz", "source_format": "csv", "beast_options": {"header": true, "geometry_type": "wkt", "separator": ",", "dimensions": "0"}}'
 * </pre>
 */
class DataExplorerServer extends AbstractWebHandler with Logging {

  /** Additional options passed on by the user to override existing options */
  var opts: BeastOptions = _

  /** The SparkSession that is used to process datasets */
  var sparkSession: SparkSession = _

  /** Database connection to access dataset information */
  var dbConnection: java.sql.Connection = _

  /** The path at which this server keeps all datasets */
  var datasetsPath: String = _

  /** The file system at which this server stores the datasets */
  var datasetFS: FileSystem = _

  /** The datasets that are currently being processed in the background */
  private var datasetsInProgress: collection.mutable.ArrayBuffer[(Int, scala.concurrent.Future[Any])] =
    new collection.mutable.ArrayBuffer()

  override def setup(ss: SparkSession, opts: BeastOptions): Unit = {
    super.setup(ss, opts)
    this.opts = opts
    this.sparkSession = ss
    this.datasetsPath = opts.getString("datasetPath", "dataExplorerData")
    this.datasetFS = new Path(datasetsPath).getFileSystem(ss.sparkContext.hadoopConfiguration)
    setupDB()
  }

  protected def setupDB(): Unit = {
    // Load H2 Driver
    Class.forName("org.h2.Driver")
    dbConnection = java.sql.DriverManager.getConnection(s"jdbc:h2:./$datasetsPath/db", "sa", "")
    DatasetProcessor.createDB(dbConnection)
    // Close the DB connection on application end
    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
        dbConnection.close()
    })
  }

  @WebMethod(url = "/workspaces.json", method = "GET")
  def listWorkspaces(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val statement = dbConnection.createStatement()

    val sqlQuery = "SELECT id, name, description FROM workspaces"
    val resultSet = statement.executeQuery(sqlQuery)

    val mapper = new ObjectMapper()
    val factory = mapper.getFactory
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val jsonGenerator = factory.createGenerator(response.getOutputStream)

    jsonGenerator.writeStartArray()

    while (resultSet.next()) {
      jsonGenerator.writeStartObject()

      jsonGenerator.writeNumberField("id", resultSet.getInt("id"))
      jsonGenerator.writeStringField("name", resultSet.getString("name"))
      jsonGenerator.writeStringField("description", resultSet.getString("description"))

      jsonGenerator.writeEndObject()
    }

    jsonGenerator.writeEndArray()
    jsonGenerator.close()
    true
  }

  @WebMethod(url = "/workspaces/{workspaceId}.json", method = "GET")
  def getWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse, workspaceId: Int): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      val sqlQuery = s"SELECT id, name, description FROM workspaces WHERE id = $workspaceId"
      val resultSet = statement.executeQuery(sqlQuery)

      if (resultSet.next()) {
        val mapper = new ObjectMapper()
        val factory = mapper.getFactory
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType("application/json")
        val jsonGenerator = factory.createGenerator(response.getOutputStream)

        jsonGenerator.writeStartObject()

        jsonGenerator.writeNumberField("id", resultSet.getInt("id"))
        jsonGenerator.writeStringField("name", resultSet.getString("name"))
        jsonGenerator.writeStringField("description", resultSet.getString("description"))

        jsonGenerator.writeEndObject()
        jsonGenerator.close()
        true
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND)
        false
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        false
    } finally {
      if (statement != null) statement.close()
    }
  }

  @WebMethod(url = "/workspaces/{workspaceId}.json", method = "PUT")
  def updateWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse, workspaceId: Int): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      // Parse the JSON payload from the request
      val mapper = new ObjectMapper()
      val node = mapper.readTree(request.getInputStream)
      val name = node.get("name").asText()
      val description = node.get("description").asText()

      // Update the workspace in the database
      val sqlUpdate = s"UPDATE workspaces SET name = ?, description = ? WHERE id = ?"
      val preparedStatement = dbConnection.prepareStatement(sqlUpdate)
      preparedStatement.setString(1, name)
      preparedStatement.setString(2, description)
      preparedStatement.setInt(3, workspaceId)

      val rowsAffected = preparedStatement.executeUpdate()

      if (rowsAffected > 0) {
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType("application/json")
        val result = Map("id" -> workspaceId, "name" -> name, "description" -> description)
        val out = response.getWriter
        out.print(WebUtil.scalaMapToJsonString(result))
        out.close()
      } else
        response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        true
    } finally {
      if (statement != null) statement.close()
    }
  }


  /**
   * Delete a workspace by ID. Test with the following command:
   * curl -X DELETE http://localhost:8890/workspaces/1
   * @param path
   * @param request
   * @param response
   * @param workspaceId
   * @return
   */
  @WebMethod(url = "/workspaces/{workspaceId}", method = "DELETE")
  def deleteWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse, workspaceId: Int): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      // Start a database transaction
      dbConnection.setAutoCommit(false)

      // Step 1: Remove associations between the workspace and its datasets
      val sqlDeleteAssociations = s"DELETE FROM workspace_datasets WHERE workspace_id = $workspaceId"
      statement.executeUpdate(sqlDeleteAssociations)

      // Step 2: Delete the workspace itself
      val sqlDeleteWorkspace = s"DELETE FROM workspaces WHERE id = $workspaceId"
      val rowsAffected = statement.executeUpdate(sqlDeleteWorkspace)

      // Commit the transaction
      dbConnection.commit()

      if (rowsAffected > 0) {
        response.setStatus(HttpServletResponse.SC_OK) // Successfully deleted
        true
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Workspace not found
        false
      }
    } catch {
      case e: Exception =>
        // Rollback in case of error
        dbConnection.rollback()
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        false
    } finally {
      // Reset auto commit and close resources
      dbConnection.setAutoCommit(true)
      if (statement != null) statement.close()
    }
  }

  /**
   * Test with with the following command:
   * curl -X POST http://localhost:8890/workspaces.json \
   * -H "Content-Type: application/json" \
   * -d '{"name": "MyWorkspace", "description": "A description of MyWorkspace"}'
   * @param path
   * @param request
   * @param response
   * @return
   */
  @WebMethod(url = "/workspaces.json", method = "POST")
  def createWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val mapper = new ObjectMapper()
    val workspaceData = mapper.readTree(request.getInputStream)

    val name = workspaceData.get("name").asText()
    val description = workspaceData.get("description").asText()

    // Start a database transaction
    dbConnection.setAutoCommit(false)
    val statement = dbConnection.createStatement()

    try {
      // Insert the new workspace
      val sqlInsertWorkspace = s"INSERT INTO workspaces (name, description) VALUES ('$name', '$description')"
      statement.executeUpdate(sqlInsertWorkspace, java.sql.Statement.RETURN_GENERATED_KEYS)

      // Retrieve the generated workspace ID
      val generatedKeys = statement.getGeneratedKeys
      if (generatedKeys.next()) {
        val newWorkspaceId = generatedKeys.getInt(1)

        // Retrieve the ID of the "OSM" dataset
        val sqlFindOSMId = "SELECT id FROM datasets WHERE name = 'OSM'"
        val osmResult = statement.executeQuery(sqlFindOSMId)
        if (osmResult.next()) {
          val defaultDatasetId = osmResult.getInt("id")
          // Insert the default dataset into the workspace if it was found
          val defaultStyle = """{"opacity": 100, "visible": true}"""
          val sqlInsertDefaultDataset = s"INSERT INTO workspace_datasets (workspace_id, dataset_id, viz_style) VALUES ($newWorkspaceId, $defaultDatasetId, '$defaultStyle')"
          statement.executeUpdate(sqlInsertDefaultDataset)
        }
        // Commit the transaction
        dbConnection.commit()
        response.setStatus(HttpServletResponse.SC_CREATED)
        response.setContentType("application/json")
        val jsonResponse: Map[String, Any] = Map("id" -> newWorkspaceId, "name" -> name, "description" -> description)
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType("application/json")
        val writer = response.getWriter
        writer.write(WebUtil.scalaMapToJsonString(jsonResponse))
        writer.close()
      }

      true
    } catch {
      case e: Exception =>
        // Rollback in case of error
        dbConnection.rollback()
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        false
    } finally {
      // Reset auto commit and close resources
      dbConnection.setAutoCommit(true)
      if (statement != null) statement.close()
    }
  }

  @WebMethod(url = "/datasets.json", method = "GET")
  def listDatasets(target: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val statement = dbConnection.createStatement()
    val resultSet = statement.executeQuery("SELECT id, name, mbr, status, progress, error_message, viz_options FROM datasets")

    val mapper = new ObjectMapper()
    val factory = mapper.getFactory
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val jsonGenerator = factory.createGenerator(response.getOutputStream)

    jsonGenerator.writeStartArray()
    removeCompletedProcesses()
    while (resultSet.next()) {
      jsonGenerator.writeStartObject()

      val datasetID: Int = resultSet.getInt("id")
      jsonGenerator.writeNumberField("id", datasetID)
      val datasetName: String = resultSet.getString("name")
      jsonGenerator.writeStringField("name", datasetName)

      val mbrAr = resultSet.getArray("mbr")
      if (!resultSet.wasNull()) {
        // Assuming mbr is stored as an Array of Double
        val mbrArrayDb: Array[Float] = mbrAr.getArray.asInstanceOf[Array[AnyRef]].map(_.asInstanceOf[Float])
        jsonGenerator.writeArrayFieldStart("mbr")
        mbrArrayDb.foreach(jsonGenerator.writeNumber)
        jsonGenerator.writeEndArray()
      }
      jsonGenerator.writeStringField("status", resultSet.getString("status"))
      jsonGenerator.writeNumberField("progress", resultSet.getInt("progress"))

      if (datasetsInProgress.exists(_._1 == datasetID))
        jsonGenerator.writeBooleanField("in_progress", true)

      val errorMessage: String = resultSet.getString("error_message")
      if (!resultSet.wasNull())
        jsonGenerator.writeStringField("error_message", errorMessage)
      val vizOptionsStr: String = resultSet.getString("viz_options")
      if (!resultSet.wasNull()) {
        // Add visualization information
        val parsed_opts = mapper.readTree(vizOptionsStr)
        var vizOpts: Map[String, Any] = WebUtil.jsonNodeToScala(parsed_opts).asInstanceOf[Map[String, Any]]
        vizOpts += ("visualization_page" -> s"/datasets/$datasetName/tiles/index.html")
        jsonGenerator.writeFieldName("visualization_options")
        jsonGenerator.writeTree(WebUtil.scalaMapToJsonNode(vizOpts))
      }
      jsonGenerator.writeEndObject()
    }

    jsonGenerator.writeEndArray()
    jsonGenerator.close()
    true
  }

  @WebMethod(url = "/datasets/dangling.json", method = "GET")
  def listDanglingDatasets(target: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val statement = dbConnection.createStatement()
    val query =
      """
      SELECT d.id, d.name, d.size
      FROM datasets d
      LEFT JOIN workspace_datasets wd ON d.id = wd.dataset_id
      WHERE wd.dataset_id IS NULL
    """
    val resultSet = statement.executeQuery(query)

    val mapper = new ObjectMapper()
    val factory = mapper.getFactory
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val jsonGenerator = factory.createGenerator(response.getOutputStream)

    jsonGenerator.writeStartArray()
    while (resultSet.next()) {
      jsonGenerator.writeStartObject()

      val datasetID: Int = resultSet.getInt("id")
      jsonGenerator.writeNumberField("id", datasetID)
      val datasetName: String = resultSet.getString("name")
      jsonGenerator.writeStringField("name", datasetName)
      val datasetSize: Long = resultSet.getLong("size")
      jsonGenerator.writeNumberField("size", datasetSize)

      jsonGenerator.writeEndObject()
    }
    jsonGenerator.writeEndArray()
    jsonGenerator.close()
    true
  }

  @WebMethod(url = "/workspaces/{workspaceId}/datasets.json", method = "GET")
  def listDatasetsInWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse,
                              workspaceId: Int): Boolean = {
    val statement = dbConnection.createStatement()

    val sqlQuery =
      s"""
      SELECT d.id, d.name, d.mbr, d.status, d.progress, d.error_message, d.viz_options, wd.viz_style, wd.dataset_order
      FROM datasets d
      INNER JOIN workspace_datasets wd ON d.id = wd.dataset_id
      WHERE wd.workspace_id = $workspaceId
      ORDER BY wd.dataset_order ASC
      """

    val resultSet = statement.executeQuery(sqlQuery)

    val mapper = new ObjectMapper()
    val factory = mapper.getFactory
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val jsonGenerator = factory.createGenerator(response.getOutputStream)

    jsonGenerator.writeStartArray()
    removeCompletedProcesses()

    while (resultSet.next()) {
      jsonGenerator.writeStartObject()

      val datasetID = resultSet.getInt("id")
      jsonGenerator.writeNumberField("id", datasetID)
      val datasetName: String = resultSet.getString("name")
      jsonGenerator.writeStringField("name", datasetName)

      val mbrAr = resultSet.getArray("mbr")
      if (!resultSet.wasNull()) {
        val mbrArrayDb: Array[Float] = mbrAr.getArray.asInstanceOf[Array[AnyRef]].map(_.asInstanceOf[Float])
        jsonGenerator.writeArrayFieldStart("mbr")
        mbrArrayDb.foreach(jsonGenerator.writeNumber)
        jsonGenerator.writeEndArray()
      }
      jsonGenerator.writeStringField("status", resultSet.getString("status"))
      jsonGenerator.writeNumberField("progress", resultSet.getInt("progress"))
      if (datasetsInProgress.exists(_._1 == datasetID))
        jsonGenerator.writeBooleanField("in_progress", true)
      val errorMessage: String = resultSet.getString("error_message")
      if (!resultSet.wasNull())
        jsonGenerator.writeStringField("error_message", errorMessage)

      // Visualization options for the dataset
      val vizOptionsStr: String = resultSet.getString("viz_options")
      if (!resultSet.wasNull()) {
        val parsed_opts = mapper.readTree(vizOptionsStr)
        var vizOpts: Map[String, Any] = WebUtil.jsonNodeToScala(parsed_opts).asInstanceOf[Map[String, Any]]
        vizOpts += ("visualization_page" -> s"/datasets/$datasetName/tiles/index.html")
        jsonGenerator.writeFieldName("visualization_options")
        jsonGenerator.writeTree(WebUtil.scalaMapToJsonNode(vizOpts))
      }

      // Style specific to the dataset in the workspace
      val styleStr: String = resultSet.getString("viz_style")
      if (!resultSet.wasNull()) {
        val styleNode = mapper.readTree(styleStr)
        jsonGenerator.writeFieldName("style")
        jsonGenerator.writeTree(styleNode)
      }

      jsonGenerator.writeEndObject()
    }

    jsonGenerator.writeEndArray()
    jsonGenerator.close()
    true
  }

  @WebMethod(url = "/datasets.json", method = "POST")
  def createDataset(target: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    import scala.concurrent.{ExecutionContext, Future}

    var preparedStatement: java.sql.PreparedStatement = null
    try {
      val objectMapper = new ObjectMapper()
      // Parse JSON from request body
      val jsonNode: JsonNode = objectMapper.readTree(request.getInputStream)

      // Extract required fields
      val datasetName = jsonNode.get("name").asText()
      val sourceUri = jsonNode.get("source_uri").asText()
      val sourceFormat = jsonNode.get("source_format").asText()

      // Check for optional beast_options
      val beastOptions = if (jsonNode.has("beast_options")) {
        val opts = jsonNode.get("beast_options")
        if (!opts.isObject)
          throw new RuntimeException("Invalid beast options provided. Should be a valid JSON string")
        jsonNode.get("beast_options").toString
      } else null

      // Prepare SQL statement
      val sql = "INSERT INTO datasets (name, dir_name, source_uri, source_format, status, progress, beast_options) VALUES (?, ?, ?, ?, 'created', 0, ?)"
      preparedStatement = dbConnection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)
      preparedStatement.setString(1, datasetName)
      preparedStatement.setString(2, datasetName)
      preparedStatement.setString(3, sourceUri)
      preparedStatement.setString(4, sourceFormat)
      preparedStatement.setString(5, beastOptions)

      // Execute update and process results
      val rowsAffected = preparedStatement.executeUpdate()
      val generatedKeys = preparedStatement.getGeneratedKeys
      if (generatedKeys.next()) {
        val newDatasetId = generatedKeys.getInt(1)
        val jsonResponse: Map[String, Any] = Map("id" -> newDatasetId, "name" -> datasetName, "status" -> "created", "progress" -> 0)
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType("application/json")
        val writer = response.getWriter
        writer.write(WebUtil.scalaMapToJsonString(jsonResponse))
        writer.close()

        implicit val ec: ExecutionContext = ExecutionContext.global
        val bgProcess = Future {
          new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession).run()
        }
        datasetsInProgress.synchronized({datasetsInProgress += newDatasetId -> bgProcess})
        true
      } else {
        false
      }
    } catch {
      case e: java.sql.SQLIntegrityConstraintViolationException =>
        logError("Invalid insertion", e)
        val errorResponse: Map[String, Any] = Map("error" -> s"A dataset with the same name already exists")
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        response.setContentType("application/json")
        val writer = response.getWriter
        writer.write(WebUtil.scalaMapToJsonString(errorResponse))
        writer.close()
        true
      case e: Exception =>
        logError("Error creating dataset", e)
        val errorResponse: Map[String, Any] = Map("error" -> e.getMessage)
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        response.setContentType("application/json")
        val writer = response.getWriter
        writer.write(WebUtil.scalaMapToJsonString(errorResponse))
        writer.close()
        true
    } finally {
      if (preparedStatement != null) preparedStatement.close()
    }
  }

  private def removeCompletedProcesses(): Unit = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    for ((datasetID, bgProcess) <- datasetsInProgress) {
      try {
        Await.result(bgProcess, 1.nanos)
        datasetsInProgress.synchronized({datasetsInProgress = datasetsInProgress.filter(_._1 != datasetID)})
      } catch {
        case _: Exception =>
      }
    }
  }

  @WebMethod(url = "/datasets/{datasetName}.json", method = "GET")
  def retrieveDataset(target: String, request: HttpServletRequest, response: HttpServletResponse,
                      datasetName: String): Boolean = {
    val statement = dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")
    statement.setString(1, datasetName)
    val resultSet: java.sql.ResultSet = statement.executeQuery()
    if (!resultSet.next()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' on the server"}""")
      out.close()
      return true
    }

    val mapper = new ObjectMapper()
    val factory = mapper.getFactory
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val jsonGenerator = factory.createGenerator(response.getOutputStream)

    jsonGenerator.writeStartObject()
    val datasetID = resultSet.getInt("id")
    jsonGenerator.writeNumberField("id", datasetID)
    jsonGenerator.writeStringField("name", resultSet.getString("name"))
    // Write status of the dataset
    jsonGenerator.writeStringField("status", resultSet.getString("status"))
    jsonGenerator.writeNumberField("progress", resultSet.getInt("progress"))
    removeCompletedProcesses()
    if (datasetsInProgress.exists(_._1 == datasetID))
      jsonGenerator.writeBooleanField("in_progress", true)

    val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()

    val beastOptionsStr = resultSet.getString("beast_options")
    if (!resultSet.wasNull()) {
      val beastOptions = objectMapper.readTree(beastOptionsStr)
      jsonGenerator.writeFieldName("beast_options")
      jsonGenerator.writeTree(beastOptions)
    }

    // Assuming mbr is stored as an Array of Double
    val mbrArray = resultSet.getArray("mbr")
    if (!resultSet.wasNull()) {
      val mbrArrayDb: Array[Float] = mbrArray.getArray.asInstanceOf[Array[AnyRef]].map(_.asInstanceOf[Float])
      jsonGenerator.writeArrayFieldStart("mbr")
      mbrArrayDb.foreach(jsonGenerator.writeNumber)
      jsonGenerator.writeEndArray()
    }

    // Include more summary information
    val numFeatures: Long = resultSet.getLong("num_features")
    if (!resultSet.wasNull())
      jsonGenerator.writeNumberField("num_features", numFeatures)
    val numPoints: Long = resultSet.getLong("num_points")
    if (!resultSet.wasNull())
      jsonGenerator.writeNumberField("num_points", numPoints)
    val geometryTYpe: String = resultSet.getString("geometry_type")
    if (!resultSet.wasNull())
      jsonGenerator.writeStringField("geometry_type", geometryTYpe)
    val size: Long = resultSet.getLong("size")
    if (!resultSet.wasNull())
      jsonGenerator.writeNumberField("size", size)

    // Include schema if exists
    val schema_str = resultSet.getString("schema")
    if (schema_str != null) {
      // Parse schema as a tree node
      val schema: JsonNode = objectMapper.readTree(schema_str)
      jsonGenerator.writeFieldName("schema")
      jsonGenerator.writeTree(schema)
    }

    // Write visualization options
    val viz_opts_str = resultSet.getString("viz_options")
    if (viz_opts_str != null) {
      val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val viz_opts: JsonNode = objectMapper.readTree(viz_opts_str)
      jsonGenerator.writeFieldName("visualization_options")
      jsonGenerator.writeTree(viz_opts)
    }

    // If there is an error message from past processing, include it
    val errorMessage = resultSet.getString("error_message")
    if (errorMessage != null)
      jsonGenerator.writeStringField("error_message", errorMessage)

    jsonGenerator.writeEndObject()
    jsonGenerator.close()
    true
  }

  /**
   * Processes a dataset form where it stopped or from the beginning
   * @param target
   * @param request
   * @param response
   * @param datasetName
   * @return
   */
  @WebMethod(url = "/datasets/{datasetName}/process", method = "POST")
  def processDataset(target: String, request: HttpServletRequest, response: HttpServletResponse, datasetName: String): Boolean = {
    try {
      // Verify if the dataset exists
      if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
        throw new NoSuchElementException(s"Dataset $datasetName not found")
      }

      // Assuming the DatasetProcessor has a method for processing named `processDataset`
      implicit val ec: ExecutionContext = ExecutionContext.global
      val process = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
      val bgProcess = Future { process.run() }
      datasetsInProgress.synchronized({
        datasetsInProgress += process.id -> bgProcess
      })

      // Prepare and send the response
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(WebUtil.scalaMapToJsonString(Map("datasetID" -> process.id, "status" -> "Processing")))
      out.close()
      true
    } catch {
      case e: Exception =>
        AbstractWebHandler.reportError(response, "Error processing dataset", e)
        true
    }
  }


  @WebMethod(url = "/datasets/{datasetName}/download\\.{_format}", method = "GET")
  def downloadDataset(target: String, request: HttpServletRequest, response: HttpServletResponse,
                      datasetName: String, _format: String): Boolean = {
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' on the server"}""")
      return true
    }
    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    val indexPath: Path = datasetProcessor.dataIndexPath

    // Check if there is a query rectangle in the request
    var filterMBR: EnvelopeNDLite = null
    var searchRegions: Array[Geometry] = null
    val filterMBRStr: String = request.getParameter("mbr")
    if (filterMBRStr != null) {
      val parts: Array[Double] = filterMBRStr.split(",").map(_.toDouble)
      // TODO download by MBR
    }
    // TODO check if there is a GeoJSON geometry in the body
    // TODO apply the filter

    // Retrieve the master file path to determine the modification time
    val masterFilePath: Path = SpatialFileRDD.getMasterFilePath(datasetFS, indexPath)
    if (masterFilePath == null) return false
    val masterFileModificationTime: Long = datasetFS.getFileStatus(masterFilePath).getModificationTime
    var clientCachedTimestamp: Long = 0
    if (request.getHeader("If-Modified-Since") != null)
      clientCachedTimestamp = request.getDateHeader("If-Modified-Since")
    if (filterMBR == null && clientCachedTimestamp >= masterFileModificationTime) {
      // The client already has an up-to-date copy. Skip the request.
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED)
      logInfo(String.format("Not returning data layer for '%s' since the client has an up-to-date version", target))
      return true
    }
    if (filterMBR == null) {
      // If the entire file is downloaded, add a header to make the file available for a day
      // This is helpful when the file is loaded for visualization as a GeoJSON file
      response.addDateHeader("Last-Modified", masterFileModificationTime)
      response.addDateHeader("Expires", masterFileModificationTime + AbstractWebHandler.OneDay)
      response.addHeader("Cache-Control", "max-age=86400")
    }
    // Prepare the output writer
    val counterOut: CounterOutputStream = new CounterOutputStream(response.getOutputStream)
    var out: OutputStream = counterOut
    // Search for the writer that supports the given format// Search for the writer that supports the given format
    var format: String = _format
    val codec: CompressionCodec = new CompressionCodecFactory(this.conf).getCodec(new Path(format))
    var resultIsCompressed = false
    if (codec != null) {
      resultIsCompressed = true
      format = format.substring(0, format.lastIndexOf('.'))
      out = codec.createOutputStream(out)
    }
    val matchingWriters = FeatureWriter.featureWriters.values().asScala.filter(writerClass => {
      val metadata: FeatureWriter.Metadata = writerClass.getAnnotation(classOf[FeatureWriter.Metadata])
      metadata.extension.equalsIgnoreCase("." + format)
    })
    if (matchingWriters.isEmpty) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Unrecognized extension '$format'"}""")
      return true
    }
    val matchingWriter: Class[_ <: FeatureWriter] = matchingWriters.head
    if ((matchingWriter eq classOf[KMZFeatureWriter]) || (matchingWriter eq classOf[CompressedShapefileWriter]))
      resultIsCompressed = true
    if (!resultIsCompressed && AbstractWebHandler.isGZIPAcceptable(request)) {
      out = new GZIPOutputStream(out)
      response.setHeader("Content-Encoding", "gzip")
    }
    val contentType = DataExplorerServer.contentTypes(matchingWriter)
    if (contentType == null)
      logWarning("Unknown Mime type for " + matchingWriter.getSimpleName)
    else
      response.setContentType(contentType)

    val opts = new BeastOptions(conf)
    if (matchingWriter eq classOf[CSVFeatureWriter]) {
      // TODO If the geometry is point, use comma separator and include x, y
      // TODO If the geometry is anything else, use WKT and tab separator
      val geometryType: String = datasetProcessor.geometryType
      geometryType.toLowerCase match {
        case "point" => opts.set("oformat", "point(0,1)").set("separator", ",")
        case _ => opts.set("oformat", "wkt(0)").set("separator", "\t")
      }
      opts.set("header", true)
    } else if (matchingWriter eq classOf[CompressedShapefileWriter]) {
      // Set the part size in ShapefileWriter to 32 MB to make the download start asap and avoid client timeout.
      opts.setLong(CompressedShapefileWriter.PartSize, 32 * 1024 * 1024)
    }
    val downloadFileNamePrefix = datasetName.replaceAll("[/\\s]", "_")
    val downloadFileName = downloadFileNamePrefix + "." + _format
    response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", downloadFileName))

    // Now, read all input splits one-by-one and write their contents to the writer
    val featureReaderClass = SpatialFileRDD.getFeatureReaderClass(indexPath.toString, opts)
    val partitions = SpatialFileRDD.createPartitions(indexPath.toString, opts, this.conf)

    // Prepare the writing
    val featureWriter = matchingWriter.getDeclaredConstructor().newInstance()
    response.setStatus(HttpServletResponse.SC_OK)
    // Initialize the feature writer
    featureWriter.initialize(out, opts.loadIntoHadoopConf(this.conf))

    try {
      for (partition <- partitions) {
        if (filterMBR == null) {
          // No search provided, match all geometries
          val features: Iterator[IFeature] = SpatialFileRDD.readPartition(partition, featureReaderClass, true, opts)
          for (feature <- features)
            featureWriter.write(feature)
        } else {
          // Check which search geometries overlap with this specific partition
          val matchingGeoms = partition match {
            case spatialPartition: SpatialFileRDD.SpatialFilePartition =>
              searchRegions.filter(g => spatialPartition.mbr.intersectsEnvelope(g.getEnvelopeInternal))
            case _ =>
              // Not spatial partition, use all search regions
              searchRegions
          }
          if (!matchingGeoms.isEmpty) {
            SpatialFileRDD.readPartition(partition, featureReaderClass, true, opts)
              .filter(f => matchingGeoms.exists(g => { // Filter only geometries that match
                try {
                  g.overlaps(f.getGeometry)
                }
                catch {
                  case _: TopologyException => false
                }
              })).foreach(f => featureWriter.write(f))
          }
        }
      }
    } finally {
      featureWriter.close()
    }
    val numWrittenBytes: Long = counterOut.getCount
    logInfo(s"Request '$target' resulted in $numWrittenBytes bytes")
    true
  }
  @WebMethod(url = "/datasets/{datasetName}/export", method = "POST")
  def exportDataset(path: String, request: HttpServletRequest, response: HttpServletResponse, datasetName: String): Boolean = {
    // Validate dataset name
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.write(s"""{"message": "Cannot find dataset '$datasetName' on the server"}""")
      out.close()
      return true
    }

    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    try {
      val objectMapper = new ObjectMapper()
      val jsonNode: JsonNode = objectMapper.readTree(request.getInputStream)

      // Extract required fields
      val exportFormat = jsonNode.get("format").asText()
      val exportPath = jsonNode.get("path").asText()
      val limit = if (jsonNode.has("limit")) Some(jsonNode.get("limit").asInt()) else None

      // Handle preview request
      if (jsonNode.has("preview") && jsonNode.get("preview").asBoolean()) {
        val datasetDF = datasetProcessor.loadIndexedData.toDataFrame(sparkSession)
        // Identify and remove geometry columns
        val geometryColumns = datasetDF.schema.fields
          .filter(_.dataType == GeometryDataType) // Replace with the actual type used for geometry
          .map(_.name)

        val filteredDF = datasetDF.drop(geometryColumns: _*)
        val limitedDf = limit match {
          case Some(n) => filteredDF.limit(n)
          case None => filteredDF
        }

        // Convert the DataFrame to JSON
        val previewJson = limitedDf.toJSON.collect().mkString("[", ",", "]")

        // Return the preview data to the frontend
        response.setStatus(HttpServletResponse.SC_OK)
        response.setContentType("application/json")
        val out = response.getWriter
        out.write(previewJson)
        out.close()
        return true
      }

      // Proceed with the actual export
      val bgProcess = datasetProcessor.exportDataset(exportFormat, exportPath, Map(), limit)

      // Track the background process
      datasetsInProgress.synchronized {
        datasetsInProgress += datasetProcessor.id -> bgProcess
      }

      // Respond with success message
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("application/json")
      val out = response.getWriter
      out.write(s"""{"message": "Dataset '$datasetName' is being exported"}""")
      out.close()
      return true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        response.setContentType("application/json")
        val out = response.getWriter
        out.write(s"""{"error": "Failed to export dataset due to an internal error"}""")
        out.close()
        return true
    }
  }


  @WebMethod(url = "/datasets/{datasetName}", method = "DELETE")
  def deleteDataset(target: String, request: HttpServletRequest, response: HttpServletResponse,
                    datasetName: String): Boolean = {
    // If the dataset does not exist, return 404 not found
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' on the server"}""")
      out.close()
      return true
    }
    // First, retrieve the dataset to locate its directory and delete it
    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    val filesDeleted: Boolean = datasetProcessor.deleteAllFiles()
    // Delete the given dataset from the database
    val deletedFromDB = datasetProcessor.deleteFromDB()
    response.setStatus(if (deletedFromDB) HttpServletResponse.SC_OK else HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    response.setContentType("application/json")
    val out = response.getWriter
    val message: String = (filesDeleted, deletedFromDB) match {
      case (true, true) => "Deleted both data files and from the database"
      case (true, false) => "Deleted only data files but could not delete from database"
      case (false, true) => "Deleted only from database but could not delete data files"
      case (false, false) => "Neither data files nor the database were deleted"
    }
    out.println(s"""{"message": "$message for dataset '$datasetName'"}""")
    out.close()
    true
  }

  /**
   * Retrieves an object that contains all necessary information about visualization.
   * It should contain the following information:
   * <pre>
   *   {
   *     "datasetName": "[name]",
   *     "visualizationPage": "/path/to/index.html",
   *     "type": "[VectorTile, RasterTile, GeoJSON]",
   *     "minLevel": [minimum visualization level],
   *     "maxLevel": [maximum visualization level],
   *     "style|aggregateStyle|featureStyle": [{} | "[javascript-code]"
   *   }
   * </pre>
   *  - *dataset_name*: Similar to the given one
   *  - *visualization_page*: a URL to an HTML page that visualizes this dataset
   *  - *visualization_type*: The type of the visualization. The three available types are:
   *    - *VectorTile*: Multilevel visualization with MVT tiles
   *    - *RasterTile*: Multilevel visualization with PNG tiles
   *    - *GeoJSON*: Data is available as one-level GeoJSON data. Typically used for small data.
   *  - *min_level*: (Optional) The minimum level of multilevel visualization. If not set, assume it is zero.
   *  - *max_level*: (Optional) The maximum level of multilevel visualization. If not set, any zoom level is supported.
   *  - *style|aggregate_style|feature_style*: A nested object that specifies the visualization style.
   *    Used only for VectorTile and GeoJSON.
   *    - *{}*: If a nested object is used, it can be passed as-is to visualization functions of OpenLayers.
   *    - "javascript": If a string is used, it is assumed to be a JavaScript code that returns a style object.
   *    - If *style* is used, it is used for both aggregate and feature layers.
   *    - If *aggregate_style* is specified, it is used only for the aggregate layer.
   *    - If *feature_style* is specified, it is used only for the feature layer.
   *
   * @param target the full path provided by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param datasetName the name of the dataset
   * @return a JSON object that has information about the visualization of the given dataset
   */
  @WebMethod(url = "/datasets/{datasetName}/tiles.json", method = "GET")
  def datasetVisualizationOptions(target: String, request: HttpServletRequest, response: HttpServletResponse,
                    datasetName: String): Boolean = {
    // If the dataset does not exist, return 404 not found
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' on the server"}""")
      out.close()
      return true
    }
    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    var visualizationOptions: Map[String, Any] = datasetProcessor.visualizationOptions
    visualizationOptions += ("dataset_name" -> datasetName)
    visualizationOptions += ("visualization_page" -> s"/datasets/$datasetName/tiles/index.html")
    // Now, write the results back as a JSON object
    response.setStatus(HttpServletResponse.SC_OK)
    response.setContentType("application/json")
    val out = response.getWriter
    out.println(WebUtil.scalaMapToJsonString(visualizationOptions))
    out.close()
    true
  }

  @WebMethod(url = "/datasets/{datasetName}/tiles/index.html", method = "GET")
  def datasetVisualizationPage(target: String, request: HttpServletRequest, response: HttpServletResponse,
                                  datasetName: String): Boolean = {
    // If the dataset does not exist, return 404 not found
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("text/html")
      val out = response.getWriter
      out.println(s"Cannot find dataset '$datasetName' on the server")
      out.close()
      return true
    }
    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    val visualizationPath: Path = datasetProcessor.visualizationPath
    val fileStatus = datasetFS.getFileStatus(visualizationPath)
    var htmlIn: FSDataInputStream = null
    var htmlFileLength: Long = 0
    if (fileStatus.isFile) {
      // Treat it as a ZIP file and return the index.html page inside it
      val indexHTMLFile: Option[(String, Long, Long)] = ZipUtil.lastNFiles(datasetFS, visualizationPath, 3)
        .find(p => p._1.toLowerCase() == "index.html")
      if (indexHTMLFile.isDefined) {
        // HTML file found, return it
        htmlIn = datasetFS.open(visualizationPath)
        htmlIn.seek(indexHTMLFile.get._2)
        htmlFileLength = indexHTMLFile.get._3
      }
    } else if (fileStatus.isDirectory) {
      // a directory. Extract the HTML file from it
      val indexHTMLFile: Option[FileStatus] = datasetFS.listStatus(visualizationPath)
        .find(_.getPath.getName.toLowerCase == "index.html")
      if (indexHTMLFile.isDefined) {
        // HTML file found, return it
        htmlIn = datasetFS.open(indexHTMLFile.get.getPath)
        htmlFileLength = indexHTMLFile.get.getLen
      }
    }
    if (htmlIn == null) {
      // No index.html file. Return 404 not found
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("text/html")
      val out = response.getWriter
      out.println(s"Cannot find index.html file for this dataset")
      out.close()
    } else {
      // HTML file found, return it
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("text/html")
      val out = response.getOutputStream
      IOUtils.copyBytes(htmlIn, out, htmlFileLength, true)
    }
    true
    // TODO make the returned page use the configured style for this dataset
  }

  @WebMethod(url = "/datasets/{datasetName}/tiles/tile-{z}-{x}-{y}.{format}", method = "GET")
  def datasetVisualizationPage(target: String, request: HttpServletRequest, response: HttpServletResponse,
                               datasetName: String, z: Int, x: Int, y: Int, format: String): Boolean = {
    // If the dataset does not exist, return 404 not found
    if (!DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/vnd.mapbox-vector-tile")
      return true
    }
    val datasetProcessor = new DatasetProcessor(datasetName, dbConnection, datasetsPath, datasetFS, sparkSession)
    val visualizationPath: Path = datasetProcessor.visualizationPath
    val tileID: Long = TileIndex.encode(z, x, y)
    val clientCachedTimestamp: LongWritable = new LongWritable(if (request.getHeader("If-Modified-Since") == null) 0
    else request.getDateHeader("If-Modified-Since"))
    response.setHeader("Cache-Control", "public,max-age=86400,immutable")
    val out = response.getOutputStream
    MVTDataVisualizer.retrieveOrCreateTile(datasetFS, visualizationPath, tileID,
      out, clientCachedTimestamp, opts)
    true
  }

  /**
   * Dataset Styling Options (JSON Encoding)
   *
   * Basic Styling:
   *  - Applies uniform style to all objects.
   *  - Example: `{"style_type": "basic", "stroke_color": "color",`
   *    `"fill_color": "color", "stroke_width": num, "point_radius": num}`
   *
   * Categorized Styling:
   *  - Different colors based on attribute values.
   *  - Example: `{"style_type": "categorized", "category_attribute": "name",`
   *    `"categories": [{"value": "val", "color": "color"}, ..., {"value": "all_others", "color": "color"}]}`
   *
   * Graduated Styling:
   *  - Color interpolation based on a numeric attribute.
   *  - Example: `{"style_type": "graduated", "graduated_attribute": "name", "min_value": min_val,`
   *    `"max_value": max_val, "min_color": "color", "max_color": "color"}`
   *
   * Icon Styling (For Points):
   *  - Renders each point as an image icon.
   *  - Example: `{"style_type": "icon", "icon_url": "url_to_icon", "icon_scale": scale}`
   *
   * Labels:
   *  - Displays attribute values as text labels.
   *  - Included in the main JSON object: `{"label_attribute": "attribute_name"}`
   *
   * Opacity:
   *  - Sets layer opacity level.
   *  - Included in the main JSON object: `{"opacity": num}`, where `num` goes from 0 (fully transparent) to 100
   *
   * Note:
   *  - "`color`" can be any HTML-accepted color format (e.g., "#hex", "rgb()", "rgba()", "color_name").
   *  - "`num`" is a numerical value.
   *  - "`val`" is a specific attribute value.
   *  - "`name`" is an attribute name.
   *  - "`url_to_icon`" is a URL to the image used as the icon.
   *  - "`scale`" is a scaling factor for the icon size.
   *
   * @param path
   * @param request
   * @param response
   * @param workspaceId
   * @param datasetName
   * @return
   */
  @WebMethod(url = "/workspaces/{workspaceId}/datasets/{datasetName}/style.json", method = "GET")
  def getWorkspaceDatasetStyle(path: String, request: HttpServletRequest, response: HttpServletResponse,
                               workspaceId: Int, datasetName: String): Boolean = {
    // Check if the workspace and dataset exist
    if (!WorkspaceProcessor.workspaceExists(workspaceId, dbConnection) ||
      !DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' or workspace ID '$workspaceId' on the server"}""")
      out.close()
      return true
    }

    // Assuming that the style is stored in workspace_datasets table with a JSON format
    val statement = dbConnection.createStatement()
    val sqlQuery =
      s"""
      SELECT wd.viz_style
      FROM workspace_datasets wd
      INNER JOIN datasets d ON wd.dataset_id = d.id
      WHERE wd.workspace_id = $workspaceId AND d.name = '$datasetName'
    """
    val resultSet = statement.executeQuery(sqlQuery)

    if (resultSet.next()) {
      val styleJson = resultSet.getString("viz_style")

      // Now, write the results back as a JSON object
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(styleJson)
      out.close()
      true
    } else {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Style not found for dataset '$datasetName' in workspace ID '$workspaceId'"}""")
      out.close()
      true
    }
  }

  @WebMethod(url = "/workspaces/{workspaceId}/datasets/{datasetName}/style.json", method = "PUT")
  def updateWorkspaceDatasetStyle(path: String, request: HttpServletRequest, response: HttpServletResponse,
                                  workspaceId: Int, datasetName: String): Boolean = {
    // Check if the workspace and the dataset exist
    if (!WorkspaceProcessor.workspaceExists(workspaceId, dbConnection) ||
      !DatasetProcessor.datasetExists(datasetName, dbConnection)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      response.setContentType("application/json")
      val out = response.getWriter
      out.println(s"""{"message": "Cannot find dataset '$datasetName' or workspace ID '$workspaceId' on the server"}""")
      out.close()
      return true
    }

    val objectMapper = new ObjectMapper()
    val inputStyle: JsonNode = objectMapper.readTree(request.getInputStream)

    // Update the style in the workspace_datasets table
    val statement = dbConnection.createStatement()
    try {
      val styleJson = objectMapper.writeValueAsString(inputStyle)
      val sqlUpdateStyle = s"""
      UPDATE workspace_datasets
      SET viz_style = '$styleJson'
      WHERE workspace_id = $workspaceId AND dataset_id = (
        SELECT id FROM datasets WHERE name = '$datasetName'
      )
    """
      val rowsAffected = statement.executeUpdate(sqlUpdateStyle)

      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("application/json")
      val out = response.getWriter
      val message = if (rowsAffected > 0) "Style updated" else "Style not updated"
      out.println(WebUtil.scalaMapToJsonString(Map("dataset" -> datasetName, "message" -> message)))
      out.close()
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        false
    } finally {
      if (statement != null) statement.close()
    }
  }

  @WebMethod(url = "/workspaces/{workspaceId}/datasets/{datasetName}", method = "POST")
  def addDatasetToWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse, workspaceId: Int, datasetName: String): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      // Check if the workspace exists
      val sqlWorkspaceExists = s"SELECT id FROM workspaces WHERE id = $workspaceId"
      val workspaceResult = statement.executeQuery(sqlWorkspaceExists)
      if (!workspaceResult.next()) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Workspace not found
        return true
      }

      // Retrieve the dataset ID based on the name
      val sqlFindDatasetId = s"SELECT id FROM datasets WHERE name = '$datasetName'"
      val datasetResult = statement.executeQuery(sqlFindDatasetId)
      if (datasetResult.next()) {
        val datasetId = datasetResult.getInt("id")

        // Check if the dataset is already in the workspace
        val sqlCheckAssociation = s"SELECT workspace_id FROM workspace_datasets WHERE workspace_id = $workspaceId AND dataset_id = $datasetId"
        val associationResult = statement.executeQuery(sqlCheckAssociation)
        if (associationResult.next()) {
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR) // Dataset already in workspace
          return true
        }

        // Insert the association into the workspace_datasets table
        val sqlInsertAssociation = s"INSERT INTO workspace_datasets (workspace_id, dataset_id) VALUES ($workspaceId, $datasetId)"
        statement.executeUpdate(sqlInsertAssociation)

        response.setStatus(HttpServletResponse.SC_CREATED) // Successfully added
        true
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Dataset not found
        true
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        true
    } finally {
      if (statement != null) statement.close()
    }
  }

  @WebMethod(url = "/workspaces/{workspaceId}/datasets/{datasetName}", method = "DELETE")
  def removeDatasetFromWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse,
                                 workspaceId: Int, datasetName: String): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      // Check if the workspace exists
      val sqlWorkspaceExists = s"SELECT id FROM workspaces WHERE id = $workspaceId"
      val workspaceResult = statement.executeQuery(sqlWorkspaceExists)
      if (!workspaceResult.next()) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Workspace not found
        return true
      }

      // Retrieve the dataset ID based on the name
      val sqlFindDatasetId = s"SELECT id FROM datasets WHERE name = '$datasetName'"
      val datasetResult = statement.executeQuery(sqlFindDatasetId)
      if (datasetResult.next()) {
        val datasetId = datasetResult.getInt("id")

        // Check if the dataset is part of the workspace
        val sqlCheckAssociation = s"SELECT workspace_id FROM workspace_datasets WHERE workspace_id = $workspaceId AND dataset_id = $datasetId"
        val associationResult = statement.executeQuery(sqlCheckAssociation)
        if (!associationResult.next()) {
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR) // Dataset not part of the workspace
          return true
        }

        // Delete the association from the workspace_datasets table
        val sqlDeleteAssociation = s"DELETE FROM workspace_datasets WHERE workspace_id = $workspaceId AND dataset_id = $datasetId"
        statement.executeUpdate(sqlDeleteAssociation)

        response.setStatus(HttpServletResponse.SC_OK) // Successfully removed
        true
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Dataset not found
        true
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        true
    } finally {
      if (statement != null) statement.close()
    }
  }

  @WebMethod(url = "/workspaces/{workspaceId}/datasets/reorder", method = "PUT")
  def reorderDatasetsInWorkspace(path: String, request: HttpServletRequest, response: HttpServletResponse, workspaceId: Int): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      // Start transaction
      dbConnection.setAutoCommit(false)

      // Check if the workspace exists
      val sqlWorkspaceExists = s"SELECT id FROM workspaces WHERE id = $workspaceId"
      val workspaceResult = statement.executeQuery(sqlWorkspaceExists)
      if (!workspaceResult.next()) {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND) // Workspace not found
        return true
      }

      val objectMapper = new ObjectMapper()
      val input: JsonNode = objectMapper.readTree(request.getInputStream)

      // Parse JSON array of dataset IDs into Scala Array[Int]
      val orderedDatasetIds = objectMapper.readValue(input.toString, classOf[Array[Int]])

      // Update dataset_order for each dataset in the provided order
      for ((datasetId, index) <- orderedDatasetIds.zipWithIndex) {
        // Check if the dataset is in the workspace
        val sqlCheckAssociation = s"SELECT workspace_id FROM workspace_datasets WHERE workspace_id = $workspaceId AND dataset_id = $datasetId"
        val associationResult = statement.executeQuery(sqlCheckAssociation)
        if (!associationResult.next()) {
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST) // Dataset not in workspace
          dbConnection.rollback()
          return true
        }

        // Update the dataset_order
        val sqlUpdateOrder = s"UPDATE workspace_datasets SET dataset_order = $index WHERE workspace_id = $workspaceId AND dataset_id = $datasetId"
        statement.executeUpdate(sqlUpdateOrder)
      }

      // Commit transaction
      dbConnection.commit()

      response.setStatus(HttpServletResponse.SC_OK) // Successfully reordered
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        dbConnection.rollback()
        true
    } finally {
      if (statement != null) statement.close()
      dbConnection.setAutoCommit(true)
    }
  }
}

object DataExplorerServer extends Logging {

  def contentTypes: Map[Class[_ <: FeatureWriter], String] = Map(
    classOf[CSVFeatureWriter] -> "text/csv",
    classOf[GeoJSONFeatureWriter] -> "application/geo+json",
    classOf[KMLFeatureWriter] -> "application/vnd.google-earth.kml+xml",
    classOf[KMZFeatureWriter] -> "application/vnd.google-earth.kmz",
    classOf[CompressedShapefileWriter] -> "application/zip",
  )
}