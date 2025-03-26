package edu.ucr.cs.bdlab.beast.dataExplorer

import com.fasterxml.jackson.databind.JsonNode
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.dataExplorer.DatasetProcessor
import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.hadoop.fs.FileSystem
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.sql.DriverManager

@RunWith(classOf[JUnitRunner])
class DatasetProcessorTest extends AnyFunSuite with BeastSpatialTest {
  test("Create simple dataset with step-by-step processing") {
    val datasetsPath = new File(scratchDir, "datasets")
    Class.forName("org.h2.Driver")
    using(DriverManager.getConnection(s"jdbc:h2:${datasetsPath}/beast", "sa", "")) {dbConnection =>
      DatasetProcessor.createDB(dbConnection)
      val testData = locateResource("/usa-major-cities.zip")
      val insertSQL: String = "INSERT INTO datasets(name, dir_name, source_uri, source_format, status) VALUES (?, ?, ?, ?, ?)"
      using(dbConnection.prepareStatement(insertSQL)) { insertStatement =>
        insertStatement.setString(1, "cities")
        insertStatement.setString(2, "cities")
        insertStatement.setString(3, testData.getPath)
        insertStatement.setString(4, "shapefile")
        insertStatement.setString(5, "created")
        insertStatement.executeUpdate()
      }

      val datasetProcessor = new DatasetProcessor("cities", dbConnection,
        datasetsPath.getPath, FileSystem.getLocal(sparkContext.hadoopConfiguration),
        sparkSession)
      // Test downloading the data to a local copy
      datasetProcessor.copyDataToLocal()
      val datasetDir = new File(datasetsPath, "cities")
      assert(datasetDir.isDirectory, "Dataset directory not created")
      val datasetFile = new File(new File(datasetDir, "download"), "usa-major-cities.zip")
      assert(datasetFile.isFile)

      // Try decompression now
      datasetProcessor.decompressDatasetFiles()
      assert(!datasetFile.isFile)
      val decompresssedDir = new File(new File(datasetDir, "download"), "usa-major-cities")
      assert(decompresssedDir.isDirectory)

      // Next step is summarization
      datasetProcessor.summarizeData()
      using (dbConnection.createStatement()) { statement =>
        using(statement.executeQuery("SELECT * FROM datasets WHERE name='cities'")) { data =>
          assert(data.next())
          val mbr = data.getArray("mbr").getArray.asInstanceOf[Array[AnyRef]].map(_.asInstanceOf[java.lang.Float])
          assert((mbr(0) - -157.8234361548175002).abs < 1E-3)
          assertResult(120)(data.getLong("num_features"))
        }

        // After that, build the index
        datasetProcessor.buildIndexes()

        using(statement.executeQuery("SELECT * FROM datasets WHERE name='cities'")) { data =>
          assert(data.next())
          val indexPath = new File(datasetDir, data.getString("index_path"))
          val indexedData = sparkContext.spatialFile(indexPath.getPath, "rtree")
          assert(indexedData.isSpatiallyPartitioned)
          assertResult(120)(indexedData.count())
        }
        // Finally, visualize data
        datasetProcessor.visualize()
        using(statement.executeQuery("SELECT * FROM datasets WHERE name='cities'")) { data =>
          assert(data.next())
          val opts_str: String = data.getString("viz_options")
          import com.fasterxml.jackson.databind.ObjectMapper
          val objectMapper = new ObjectMapper()
          val viz_opts: JsonNode = objectMapper.readTree(opts_str)
          assert(viz_opts.get("viz_url") != null)
        }
      }
    }
  }

  test("Create simple dataset with automated processing") {
    val datasetsPath = new File(scratchDir, "datasets")
    Class.forName("org.h2.Driver")
    using(DriverManager.getConnection(s"jdbc:h2:$datasetsPath/beast", "sa", "")) { dbConnection =>
      DatasetProcessor.createDB(dbConnection)
      val testData = locateResource("/usa-major-cities.zip")
      val insertSQL: String = "INSERT INTO datasets(name, dir_name, source_uri, source_format, status) VALUES (?, ?, ?, ?, ?)"
      using(dbConnection.prepareStatement(insertSQL)) { insertStatement =>
        insertStatement.setString(1, "cities")
        insertStatement.setString(2, "cities")
        insertStatement.setString(3, testData.getPath)
        insertStatement.setString(4, "shapefile")
        insertStatement.setString(5, "created")
        insertStatement.executeUpdate()
      }
      val datasetProcessor = new DatasetProcessor("cities", dbConnection,
        datasetsPath.getPath, FileSystem.getLocal(sparkContext.hadoopConfiguration),
        sparkSession)

      // TODO run in background
      datasetProcessor.run()
      // TODO run a while loop and check the database until it works. Add a timeout.
      using(dbConnection.createStatement()) { statement =>
        using(statement.executeQuery("SELECT * FROM datasets WHERE name='cities'")) { data =>
          assert(data.next())

          val status = data.getString("status")
          assertResult("ready")(status)

          val opts_str: String = data.getString("viz_options")
          import com.fasterxml.jackson.databind.ObjectMapper
          val objectMapper = new ObjectMapper()
          val viz_opts: JsonNode = objectMapper.readTree(opts_str)
          val datasetDir = new File(datasetsPath, "cities")
          val vizURL = viz_opts.get("viz_url")
          assert(vizURL != null)
        }
      }
    }
  }

  test("Create dataset from SQL") {
    val datasetsPath = new File(scratchDir, "datasets")
    Class.forName("org.h2.Driver")
    using(DriverManager.getConnection(s"jdbc:h2:$datasetsPath/beast", "sa", "")) { dbConnection =>
      DatasetProcessor.createDB(dbConnection)
      // Create first dataset from file
      val testData = locateResource("/usa-major-cities.zip")
      val insertSQL: String = "INSERT INTO datasets(name, dir_name, source_uri, source_format, status) VALUES (?, ?, ?, ?, ?)"
      using(dbConnection.prepareStatement(insertSQL)) { insertStatement =>
        insertStatement.setString(1, "cities")
        insertStatement.setString(2, "cities")
        insertStatement.setString(3, testData.getPath)
        insertStatement.setString(4, "shapefile")
        insertStatement.setString(5, "created")
        insertStatement.executeUpdate()
      }
      val datasetProcessor = new DatasetProcessor("cities", dbConnection,
        datasetsPath.getPath, FileSystem.getLocal(sparkContext.hadoopConfiguration),
        sparkSession)
      datasetProcessor.run()

      // Create a second datset from SQL
      val insertSQL2: String = "INSERT INTO datasets(name, dir_name, source_uri, source_format, status) VALUES (?, ?, ?, ?, ?)"
      using(dbConnection.prepareStatement(insertSQL)) { insertStatement =>
        insertStatement.setString(1, "cities2")
        insertStatement.setString(2, "cities2")
        insertStatement.setString(3, "SELECT geometry, NAME, ST FROM cities WHERE RuleID=2")
        insertStatement.setString(4, "sql")
        insertStatement.setString(5, "created")
        insertStatement.executeUpdate()
      }
      val datasetProcessor2 = new DatasetProcessor("cities2", dbConnection,
        datasetsPath.getPath, FileSystem.getLocal(sparkContext.hadoopConfiguration),
        sparkSession)
      datasetProcessor2.run()
      using(dbConnection.createStatement()) { statement =>
        using(statement.executeQuery("SELECT * FROM datasets WHERE name='cities2'")) { data =>
          assert(data.next())
          assertResult(27)(data.getLong("num_features"))
        }
      }
    }

  }
}
