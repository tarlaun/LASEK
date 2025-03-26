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

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.http.HttpEntity
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.spark.test.ScalaSparkTest
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.{DefaultGeographicCRS, DefaultProjectedCRS}
import org.geotools.referencing.cs.{DefaultCartesianCS, DefaultEllipsoidalCS}
import org.geotools.referencing.datum.{DefaultEllipsoid, DefaultGeodeticDatum, DefaultPrimeMeridian}
import org.geotools.referencing.operation.DefaultMathTransformFactory
import org.junit.runner.RunWith
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.URL

@RunWith(classOf[JUnitRunner])
class CRSServerTest extends AnyFunSuite with ScalaSparkTest {

  test("Standard CRS") {
    CRSServer.startServer(sparkContext)
    try {
      val mercator = CRS.decode("EPSG:3857")
      val sridMercator = CRSServer.crsToSRID(mercator)
      val wgs84 = CRS.decode("EPSG:4326")
      val sridWGS84 = CRSServer.crsToSRID(wgs84)
      // Now retrieve them back
      assert(CRS.lookupEpsgCode(CRSServer.sridToCRS(sridMercator), false) == 3857)
      assert(CRS.lookupEpsgCode(CRSServer.sridToCRS(sridWGS84), false) == 4326)
    } finally {
      CRSServer.stopServer(true)
    }
  }

  test("Non-standard CRS with server running") {
    CRSServer.startServer(sparkContext)
    val port = sparkContext.getLocalProperty(CRSServer.CRSServerPort)
    try {
      val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
        new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
      // Create a new SRID
      val sridSinusoidal = CRSServer.crsToSRID(sinusoidal)
      assert(sridSinusoidal < 0)
      // Now retrieve it back
      assert(CRSServer.sridToCRS(sridSinusoidal).toWKT == sinusoidal.toWKT())
      // Retrieve through the HTTP API
      val url = new URL(s"http://localhost:$port/crs/$sridSinusoidal")
      val content = new ObjectInputStream(url.openStream())
      val crs = content.readObject().asInstanceOf[CoordinateReferenceSystem]
      assertResult("Sinusoidal")(crs.getName.toString)
    } finally {
      CRSServer.stopServer(true)
    }
  }

  test("Non-standard CRS with server not running") {
    val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
      new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
    // Create a new SRID
    val sridSinusoidal = CRSServer.crsToSRID(sinusoidal)
    assert(sridSinusoidal < 0)
    // Now retrieve it back
    assert(CRSServer.sridToCRS(sridSinusoidal).toWKT == sinusoidal.toWKT())
  }

  test("Non-standard CRS with sparkConf null") {
    val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
      new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
    // Create a new SRID
    val sridSinusoidal = CRSServer.crsToSRID(sinusoidal)
    assert(sridSinusoidal < 0)
    // Now retrieve it back
    assert(CRSServer.sridToCRS(sridSinusoidal).toWKT == sinusoidal.toWKT())
  }

  test("Multiple inserts should not create new IDs") {
    CRSServer.startServer(sparkContext)
    val address = sparkContext.getLocalProperty(CRSServer.CRSServerHost)
    val port = sparkContext.getLocalProperty(CRSServer.CRSServerPort)
    try {
      val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
        new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
      val baos = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(baos)
      out.writeObject(sinusoidal)
      out.close()

      // Insert directly in the server to avoid client-side caching
      val url: String = s"http://$address:$port/crs"
      val srid1 = new String(httpPost(url, baos.toByteArray)).toInt
      assert(srid1 != 0)
      val srid2 = new String(httpPost(url, baos.toByteArray)).toInt
      assert(srid1 == srid2)
    } finally {
      CRSServer.stopServer(true)
    }
  }

  test("Non-standard CRS and server not running") {
    val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
      new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
    // Create a new SRID
    val sridSinusoidal = CRSServer.crsToSRID(sinusoidal)
    assert(sridSinusoidal < 0)
    // Now retrieve it back
    assert(CRSServer.sridToCRS(sridSinusoidal).toWKT == sinusoidal.toWKT())
  }

  def httpPost(url: String, body: Array[Byte]): Array[Byte] = {
    val httpClient: HttpClient = HttpClients.createDefault()
    val httpPost: HttpPost = new HttpPost(url)
    httpPost.setEntity(new ByteArrayEntity(body))
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
      buffer
    } else Array()
  }
}
