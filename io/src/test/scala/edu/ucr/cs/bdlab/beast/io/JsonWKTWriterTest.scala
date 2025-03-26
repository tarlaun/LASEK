package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.geolite.Feature
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateXY, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonWKTWriterTest extends AnyFunSuite with ScalaSparkTest {

  test("Write points") {
    val geometryFactory = new GeometryFactory()
    val writer = new JsonWKTWriter
    val jsonPath = new Path(scratchPath, "test.json")
    writer.initialize(jsonPath, new Configuration())
    val features = Array(Feature.create(
      geometryFactory.createPoint(new CoordinateXY(10, 20)),
      Array("key1", "key2", "key3", "key4"),
      Array(null, null, BooleanType, null),
      Array("value1", 999, null, true)
      ),
      Feature.create(
        geometryFactory.createPoint(new CoordinateXY(100, 200)),
        null,
        null,
        Array("value2", 555)
      ),
      Feature.create(
        geometryFactory.createPoint(new CoordinateXY(1000, 2000)),
        null,
        null,
        null
      )
    )
    for (feature <- features)
      writer.write(feature)
    writer.close()

    // Read the results back
    val content: Array[String] = readFile(jsonPath.toString)
    assert(content.length == features.length)
    assert(!content(0).startsWith(","))
    assert(!content(0).endsWith(","))
    assert(!content(1).startsWith(","))
    assert(!content(1).endsWith(","))
  }
}
