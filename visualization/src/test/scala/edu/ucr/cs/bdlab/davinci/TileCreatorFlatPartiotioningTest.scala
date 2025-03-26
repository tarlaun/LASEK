package edu.ucr.cs.bdlab.davinci

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, Feature, PointND}
import javax.imageio.ImageIO
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.GeometryFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TileCreatorFlatPartiotioningTest extends AnyFunSuite with ScalaSparkTest {

  test("Simple Create") {
    val mbr = new EnvelopeNDLite(2, 0, 0, 4, 4)
    val features = Array(
      (TileIndex.encode(0, 0, 0), Feature.create(null, new PointND(new GeometryFactory, 2, 0, 0))),
      (TileIndex.encode(1, 0, 0), Feature.create(null, new PointND(new GeometryFactory, 2, 0, 0))),
      (TileIndex.encode(2, 0, 0), Feature.create(null, new PointND(new GeometryFactory, 2, 0, 0))),
      (TileIndex.encode(0, 0, 0), Feature.create(null, new PointND(new GeometryFactory, 2, 3.5, 3.5))),
      (TileIndex.encode(1, 1, 1), Feature.create(null, new PointND(new GeometryFactory, 2, 3.5, 3.5))),
      (TileIndex.encode(2, 3, 3), Feature.create(null, new PointND(new GeometryFactory, 2, 3.5, 3.5))),
    )
    val plotter = new GeometricPlotter
    plotter.setup(new BeastOptions())
    val tiles: Iterator[(Long, Canvas)] = new TileCreatorFlatPartiotioning(features.iterator, mbr, plotter, 256, 256)
    assert(tiles.size == 5)
  }

  test("Create many records") {
    val mbr = new EnvelopeNDLite(2, 0, 0, 1024, 1024)
    val features = new Array[(Long, Feature)](1000)
    for (i <- features.indices) {
      features(i) = (TileIndex.encode(10, i, i), Feature.create(null, new PointND(new GeometryFactory, 2, i, i)))
    }
    val plotter = new GeometricPlotter
    plotter.setup(new BeastOptions())
    val tiles: Iterator[(Long, Canvas)] = new TileCreatorFlatPartiotioning(features.iterator, mbr, plotter, 256, 256)
    var size = 0
    tiles.foreach(tileIDCanvas => {
      val baos = new ByteArrayOutputStream
      val canvas = tileIDCanvas._2
      plotter.writeImage(canvas, baos, false)
      baos.close()
      val image: BufferedImage = ImageIO.read(new ByteArrayInputStream(baos.toByteArray))
      assert(new Color(image.getRGB(0, 0), true).getAlpha > 0)
      size += 1
    })
    assert(size == 1000)
  }
}
