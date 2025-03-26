/*
 * Copyright 2021 University of California, Riverside
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
package edu.ucr.cs.bdlab.davinci
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.util.OperationParam
import org.locationtech.jts.geom.Envelope

import java.awt.Color
import java.io.{ObjectInput, ObjectOutput, OutputStream}

/**
 * A plotter that creates SVG images for geometries (currently only supports points)
 */
@Plotter.Metadata(shortname = "svgplot",
  imageExtension = ".svg",
  description = "A plotter that draws the geometric shape of objects to SVG file")
class SVGPlotter extends Plotter {

  /**The configured point size*/
  var pointSize: Float = 1.0f

  /**The configured color for drawing the geometries*/
  var fillColor: Int = new Color(0, true).getRGB

  /**The stroke color*/
  var strokeColor: Int = Color.BLACK.getRGB

  /**Width of stroke for polygons and lines in pixels*/
  var strokeWidth: Float = 1.0f

  /**An optional title to show when an element is hovered or clicked*/
  var svgTitle: String = _

  override def setup(opts: BeastOptions): Unit = {
    super.setup(opts)
    this.pointSize = opts.getFloat(SVGPlotter.PointSize, 7.0f)
    this.fillColor = CommonVisualizationHelper.getColor(opts.getString(SVGPlotter.FillColor, "#8888")).getRGB
    this.strokeColor = CommonVisualizationHelper.getColor(opts.getString(SVGPlotter.StrokeColor, "black")).getRGB
    this.strokeWidth = opts.getFloat(SVGPlotter.StrokeWidth, 1.0f)
    this.svgTitle = opts.getString(SVGPlotter.Title)
  }

  override def getBufferSize: Int = pointSize.toInt

  override def createCanvas(width: Int, height: Int, mbr: Envelope, tileID: Long): Canvas =
    new VectorCanvas(mbr, width, height, tileID, (this.pointSize / 2).ceil.toInt)

  override def plot(layer: Canvas, feature: IFeature): Boolean =
    layer.asInstanceOf[VectorCanvas].addGeometry(feature.getGeometry, if (svgTitle != null) getTitle(feature) else null)

  override def merge(finalCanvas: Canvas, intermediateCanvas: Canvas): Canvas =
    finalCanvas.asInstanceOf[VectorCanvas].mergeWith(intermediateCanvas.asInstanceOf[VectorCanvas])

  override def writeImage(layer: Canvas, out: OutputStream, vflip: Boolean): Unit =
    layer.asInstanceOf[VectorCanvas].writeAsSVG(out, pointSize / 2.0f, strokeWidth, new Color(strokeColor), new Color(fillColor), vflip)

  /**
   * Extract the title of the given feature by interpolating the [[svgTitle]] with feature attributes
   * @param feature the feature to extract the elements from
   * @return the string that interpolates the given string with the feature
   */
  private[davinci] def getTitle(feature: IFeature): String = {
    var title = SVGPlotter.AttributeNameRegexp.replaceAllIn(svgTitle, m => feature.getAs(m.group(1)).toString)
    title = SVGPlotter.AttributeNumberRegexp.replaceAllIn(title, m => feature.get(m.group(1).toInt + 1).toString)
    title
  }
}

object SVGPlotter {
  @OperationParam(
    description = "The color of the stroke of the objects. Used for points, envelopes, lines, and polygons.",
    defaultValue = "black") val StrokeColor = "stroke"

  @OperationParam(
    description = "The color of the fill of the objects. Used for envelopes and polygons",
    defaultValue = "#0008") val FillColor = "fill"

  @OperationParam(description = "The size of points in pixels when there are few points in the canvas",
    defaultValue = "7") val PointSize = "pointsize"

  @OperationParam(description = "The width of the stroke for polylines and polygons",
    defaultValue = "1") val StrokeWidth = "strokewidth"

  @OperationParam(description =
"""
An optional title to add to features in the SVG file to show when clicked or hovered.
The title can interpolate feature attributes using the syntax `${name}` or `${#number}`
where `name` is the name of the attribute (case sensitive) and `number` is the index of the attribute (zero-based).
The geometry attributes do not count and they are skipped when numbering the attributes.
""",
    defaultValue = "") val Title = "svgtitle"

  /**A regular expression for detecting attribute names in the form `${name}`*/
  private val AttributeNameRegexp = "\\$\\{([\\w\\d_\\s\\-]+)\\}".r

  /**A regular expression for detecting attribute numbers in the form `${#name}`*/
  private val AttributeNumberRegexp = "\\$\\{#([\\d]+)\\}".r
}