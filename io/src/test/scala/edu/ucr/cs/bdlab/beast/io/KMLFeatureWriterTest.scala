package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.geolite.Feature
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, CoordinateXY, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KMLFeatureWriterTest extends AnyFunSuite with ScalaSparkTest {

  test("Write points") {
    val geometryFactory = new GeometryFactory()
    val writer = new KMLFeatureWriter
    val kmlPath = new Path(scratchPath, "test.kml")
    writer.initialize(kmlPath, new Configuration())
    val feature = Feature.create(
      geometryFactory.createPoint(new CoordinateXY(10, 20)),
      Array("key1", "key2", "key3", "key4"),
      Array(null, null, BooleanType, null),
      Array("value1", 999, null, true)
    )
    writer.write(feature)
    writer.close()

    // Read the results back
    val content = readFile(kmlPath.toString).mkString("")
    assert(content.contains("key1"))
    assert(content.contains("value1"))
    assert(content.contains("999"))
  }

  test("Write MultiPoint") {
    val geometryFactory = new GeometryFactory()
    val writer = new KMLFeatureWriter
    val kmlPath = new Path(scratchPath, "test.kml")
    writer.initialize(kmlPath, new Configuration())
    val feature = Feature.create(geometryFactory.createMultiPoint(Array(
        geometryFactory.createPoint(new CoordinateXY(10, 20)),
        geometryFactory.createPoint(new CoordinateXY(10, 20)),
      )),
      Array("key1", "key2", "key3", "key4"),
      Array(null, null, BooleanType, null),
      Array("value1", 999, null, true)
    )
    writer.write(feature)
    writer.close()

    // Read the results back
    val content = readFile(kmlPath.toString).mkString("")
    assert(content.contains("MultiGeometry"))
  }

  test("Write LineString") {
    val geometryFactory = new GeometryFactory()
    val writer = new KMLFeatureWriter
    val kmlPath = new Path(scratchPath, "test.kml")
    writer.initialize(kmlPath, new Configuration())
    val feature: Feature = Feature.create(null, geometryFactory.createLineString(
      Array[Coordinate](
        new CoordinateXY(15.2, 20.3),
        new CoordinateXY(20.1, 20.5),
        new CoordinateXY(30.3, 30.4),
        new CoordinateXY(10.7, 20.9),
      )
    ))
    writer.write(feature)
    writer.close()

    // Read the results back
    val content = readFile(kmlPath.toString).mkString("")
    val numericRegex = "[-\\d+\\.]+".r
    val allNumbers = numericRegex.findAllIn(content)
    assert(allNumbers.contains("15.2"))
    assert(allNumbers.contains("20.3"))
    assert(allNumbers.contains("10.7"))
  }
}
