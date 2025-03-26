package edu.ucr.cs.bdlab.beast.dataExplorer

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin.ReadWriteMixinFunctions
import edu.ucr.cs.bdlab.davinci.{MVTDataVisualizer, VectorTile}
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedOutputStream, ByteArrayInputStream, FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPInputStream
/**
 * Scala examples for Beast
 */
object BeastScala {

  def redirectAllOutputToFile[R](fileName: String)(block: => R): R = {
    val originalOut = System.out
    val originalErr = System.err
    val fileOut = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName, false))) // Open in append mode with auto-flush
    try {
      System.setOut(fileOut)
      System.setErr(fileOut)
      val result = block
      fileOut.flush() // Ensure all output is written to the file
      result
    } finally {
      fileOut.close()
      System.setOut(originalOut)
      System.setErr(originalErr)
    }
  }

  def main(args: Array[String]): Unit = {
    //redirectAllOutputToFile("application_log.txt") {
    // All output in this block will be redirected to "application_log.txt"
    // println("This is a test message.")
    // Initialize Spark context
    val startTime = System.nanoTime()
    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    //val sparkConf = new SparkConf()

    // Added these two lines to the template to remove some error:
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[edu.ucr.cs.bdlab.beast.util.BitArray]))



    // Start the CRSServer and store the information in SparkConf
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    CRSServer.startServer(sparkContext)
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
      // Import Beast features
      // TODO Insert your code here
      val opts: BeastOptions = "threshold" -> 0 ; "server" -> "enableStaticFileHandling"  //materialization threshold
      //val features = sparkContext.spatialFile("checkins.txt") // points dataset
      //val features = sparkContext.geojsonFile("sample.geojson")
      val features = sparkContext.shapefile("provinces.zip")
      //val features = sparkContext.shapefile("minerals.zip")
      //val features = sparkContext.geojsonFile("Crimes_-_2022.csv")
      //val features = sparkContext.readCSVPoint(filename ="/Users/tarlan/Downloads/cleaned_ebird1M.csv" , xColumn = 0, yColumn =1 )
      //val features = sparkContext.shapefile("/Users/tarlan/Downloads/Chicago_Crimes/chicago0.zip")
      //val features = sparkContext.shapefile("checkins.zip")
      //val features = sparkContext.shapefile("roads.zip")
      val tiles = MVTDataVisualizer.plotAllTiles(features,  levels=0 to 5 , resolution=256, buffer=5, opts)
      MVTDataVisualizer.saveTiles(tiles, "provinces_mar16_250__001", opts)
      //MVTDataVisualizer.saveTiles(tiles, "ebird_Nov3_250", opts)

      //MVTDataVisualizer.saveTiles(tiles, "ebird_Nov3_250", opts)
      //MVTDataVisualizer.saveTiles(tiles, "chicago_Nov18_250_zoom15", opts)

      //MVTDataVisualizer.saveTilesGreedyTarlan(tiles, "roads_Sep2_greedy_500", opts)
      // MVTDataVisualizer.saveTilesTarlan(tiles, "provinces_Drop_new_Jul22_col_250", opts)
      //MVTDataVisualizer.saveTilesCompact(tiles, "provinces_DROP_new_TEMPPP.zip", opts)
    } finally {
      sparkSession.stop()
      // Timing end and calculation
      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d // duration in seconds
      println(s"Total execution time: $duration seconds")
    }
  }
  //}
}