package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.{EmptyGeometry, Feature}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VectorPlotterTest extends AnyFunSuite with ScalaSparkTest {

  test("Extract feature label") {
    val plotter = new SVGPlotter
    // Try without attribute names
    plotter.svgTitle = "simple"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, null, null, Array())) == "simple")
    // Try with attribute names
    plotter.svgTitle = "record #${id}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id"), null, Array(15))) == "record #15")
    plotter.svgTitle = "the ${name-full}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("name-full"), null, Array("Santo"))) == "the Santo")
    // Try with attribute numbers
    plotter.svgTitle = "record #${#0}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id"), null, Array(15))) == "record #15")
    plotter.svgTitle = "the ${#1}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id", "name-full"), null, Array(5, "Santo"))) == "the Santo")
  }
}
