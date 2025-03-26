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
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.test.ScalaSparkTest
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.{AffineTransform, Point2D}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

@RunWith(classOf[JUnitRunner])
class RasterMetadataTest extends AnyFunSuite with ScalaSparkTest {

  test("kryo serialization") {
    val kryo = new Kryo()
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    val metadata = new RasterMetadata(0, 1, 100, 200, 32, 64, 4326,
      AffineTransform.getQuadrantRotateInstance(33))
    kryo.writeObject(output, metadata)
    output.close()

    val input = new Input(new ByteArrayInputStream(baos.toByteArray))
    val metadata2 = kryo.readObject(input, classOf[RasterMetadata])
    assertResult(metadata)(metadata2)

    val point1 = new Point2D.Double()
    metadata.gridToModel(100, 200, point1)
    val point2 = new Point2D.Double()
    metadata2.gridToModel(100, 200, point2)
    assertResult(point1)(point2)
  }

  test("regrid with EPSG:4326") {
    val metadata = new RasterMetadata(0, 0, 360, 180, 90, 90, 4326,
      new AffineTransform(1, 0, 0, -1, -180, 90))
    val metadata2 = metadata.rescale(180, 90)
    val point1 = new Point2D.Double(0, 0)
    metadata2.g2m.transform(point1, point1)
    assertResult(-180)(point1.x)
    assertResult(90)(point1.y)
    assertResult(new AffineTransform(2, 0, 0, -2, -180, 90))(metadata2.g2m)
  }

  test("reproject to a different CRS") {
    val metadata = RasterMetadata.create(-180, 45, -90, 0, 4326, 90, 45, 10, 10)
    val metadata2 = metadata.reproject(CRS.decode("EPSG:3857"))
    val point = Array[Double](0, 0)
    metadata2.g2m.transform(point, 0, point, 0, 1)
    assertResult(-20037508)(point(0).toInt)
    assertResult(5621521)(point(1).toInt)
  }

  test("reproject to a different CRS with invalid region") {
    val metadata = RasterMetadata.create(-180, 90, 180, -90, 4326, 90, 45, 10, 10)
    val metadata2 = metadata.reproject(CRS.decode("EPSG:3857"))
    val point = Array[Double](0, 0)
    metadata2.g2m.transform(point, 0, point, 0, 1)
    assertResult(3857)(metadata2.srid)
    assertResult(-20037508)(point(0).toInt)
    assertResult(20037508)(point(1).toInt)
  }
}
