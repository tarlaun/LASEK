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

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, GeometryHelper, GeometryReader, GeometryWriter, PointND}
import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.internal.Logging
import org.locationtech.jts.geom._
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import java.awt.Color
import java.io.{Externalizable, ObjectInput, ObjectOutput, OutputStream}
import javax.xml.stream.{XMLOutputFactory, XMLStreamWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A canvas that holds a bunch of geometries
 */
class VectorCanvas(_inputMBR: Envelope, _width: Int, _height: Int, _tileID: Long, var bufferSize: Int)
  extends Canvas(_inputMBR, _width, _height, _tileID) with Logging with Externalizable {

  /**The list of geometries stored in the canvas and their titles*/
  val geometries = new mutable.ArrayBuffer[(Geometry, String)]()

  /**
   * A bit array that marks pixels that are not empty. This is used with points to avoid adding many points that all
   * end up in one pixel location.
   */
  val occupiedPixels: BitArray = new BitArray((width + 2 * bufferSize) * (height + 2 * bufferSize))

  /**
   * The boundary of the image + buffer region as a polygon which is used to clip polygons that go outside the range
   */
  @transient var boundary: Geometry = GeometryReader.DefaultGeometryFactory.toGeometry(
    new Envelope(-bufferSize, width + bufferSize, -bufferSize, height + bufferSize)
  )

  /**
   * Total number of points for all geometries added
   */
  var numPoints: Long = 0

  def this() {
    this(null, 0, 0, 0, 0)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    GeometryHelper.writeEnvelope(inputMBR, out)
    out.writeInt(width)
    out.writeInt(height)
    out.writeLong(tileID)
    out.writeLong(numPoints)
    val geometryWriter = new GeometryWriter()
    out.writeInt(bufferSize)
    occupiedPixels.writeBitsMinimal(out)
    out.writeInt(geometries.length)
    for (geoTitle <- geometries) {
      geometryWriter.write(geoTitle._1, out, true)
      out.writeUTF(if (geoTitle._2 == null) "" else geoTitle._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    GeometryHelper.readEnvelope(inputMBR, in)
    this.width = in.readInt
    this.height = in.readInt
    this.tileID = in.readLong
    this.numPoints = in.readLong()
    calculateScale()
    this.bufferSize = in.readInt()
    boundary = GeometryReader.DefaultGeometryFactory.toGeometry(
      new Envelope(-bufferSize, width + bufferSize, -bufferSize, height + bufferSize)
    )
    val geometryReader = GeometryReader.DefaultInstance
    occupiedPixels.resize((this.width + 2 * bufferSize) * (this.height + 2 * bufferSize))
    occupiedPixels.readBitsMinimal(in)
    this.geometries.clear()
    val numGeometries: Int = in.readInt()
    for (_ <- 0 until numGeometries) {
      val geometry = geometryReader.parse(in)
      val title = in.readUTF()
      this.geometries.append((geometry, if (title.isEmpty) null else title))
    }
  }

  /**
   * Convert the given coordinate sequence to the image space
   * @param cs a coordinate sequence in the data space
   * @return a new coordinate sequence after converting all coordinates to the image space
   */
  private[davinci] def convertCoordinatesToImageScale(cs: CoordinateSequence): CoordinateSequence = {
    val convertedCS: CoordinateSequence = cs.copy()
    for (i <- 0 until convertedCS.size()) {
      convertedCS.setOrdinate(i, 0, transformX(convertedCS.getOrdinate(i, 0)))
      convertedCS.setOrdinate(i, 1, transformY(convertedCS.getOrdinate(i, 1)))
    }
    convertedCS
  }

  /**
   * Convert the given geometry form data space to the image space by applying the functions [[transformX()]]
   * and [[transformY()]] to each coordinate. No simplification or clipping is done to the geometry.
   * @param g the original geometry in the input space
   * @return a new geometry converted to the image scale
   */
  def convertGeometryToImageScale(g: Geometry): Geometry = g match {
    case p: Point  =>
      val cs = p.getCoordinateSequence
      g.getFactory.createPoint(convertCoordinatesToImageScale(cs))
    case p: PointND =>
      val cs = g.getFactory.getCoordinateSequenceFactory.create(1, p.getCoordinateDimension)
      for (d <- 0 until p.getCoordinateDimension)
        cs.setOrdinate(0, d, p.getCoordinate(d))
      g.getFactory.createPoint(convertCoordinatesToImageScale(cs))
    case e: EnvelopeND =>
      val cs = g.getFactory.getCoordinateSequenceFactory.create(2, e.getCoordinateDimension)
      for (d <- 0 until e.getCoordinateDimension) {
        cs.setOrdinate(0, d, e.getMinCoord(d))
        cs.setOrdinate(1, d, e.getMaxCoord(d))
      }
      val ccs = convertCoordinatesToImageScale(cs)
      val convertedE = new EnvelopeND(g.getFactory, e.getCoordinateDimension)
      for (d <- 0 until e.getCoordinateDimension) {
        convertedE.setMinCoord(d, ccs.getOrdinate(0, d))
        convertedE.setMaxCoord(d, ccs.getOrdinate(1, d))
      }
      convertedE
    case l: LineString =>
      val ccs = convertCoordinatesToImageScale(l.getCoordinateSequence)
      if (l.isInstanceOf[LinearRing])
        g.getFactory.createLinearRing(ccs)
      else
        g.getFactory.createLineString(ccs)
    case p: Polygon =>
      val shell: LinearRing = convertGeometryToImageScale(p.getExteriorRing).asInstanceOf[LinearRing]
      val holes = new Array[LinearRing](p.getNumInteriorRing)
      for (i <- holes.indices)
        holes(i) = convertGeometryToImageScale(p.getInteriorRingN(i)).asInstanceOf[LinearRing]
      g.getFactory.createPolygon(shell, holes)
    case _: GeometryCollection =>
      val geometries = new Array[Geometry](g.getNumGeometries)
      for (i <- geometries.indices)
        geometries(i) = convertGeometryToImageScale(g.getGeometryN(i))
      g.getFactory.createGeometryCollection(geometries)
  }

  /**
   * Adds the given geometry to the canvas. This method might simplify, drop, or combine geometries to accommodate
   * the given geometry without getting too big.
   *
   * @param geometry the geometry to add
   * @param title an optional title to attach to the geometry in the SVG file
   * @return `true` if the state of the canvas was modified.
   */
  def addGeometry(geometry: Geometry, title: String): Boolean = {
    // Convert the geometry to the scale of the image
    if (geometry.isEmpty)
      return false
    val convertedGeometry: Geometry = convertGeometryToImageScale(geometry)
    val changed = convertedGeometry match {
      case _: Point | _: PointND =>
        val coordinate: Coordinate = convertedGeometry.getCoordinate
        val pixelOffset: Int = pointOffset(coordinate.x.round.toInt, coordinate.y.round.toInt)
        if (pixelOffset == -1)
          return false
        if (occupiedPixels.get(pixelOffset))
          return false
        occupiedPixels.set(pixelOffset, true)
        this.geometries.append((convertedGeometry, title))
        numPoints += 1
        true
      case _ =>
        val simplifiedGeometry = DouglasPeuckerSimplifier.simplify(convertedGeometry, 0.5)
        if (simplifiedGeometry.isEmpty) {
          val centroid = convertedGeometry.getCentroid
          val coordinate = centroid.getCoordinate
          val pixelOffset: Int = pointOffset(coordinate.x.round.toInt, coordinate.y.round.toInt)
          if (pixelOffset == -1)
            return false
          if (occupiedPixels.get(pixelOffset))
            return false
          occupiedPixels.set(pixelOffset, true)
          this.geometries.append((centroid, title))
          numPoints += 1
          true
        } else {
          val clippedGeometry = simplifiedGeometry.intersection(boundary)
          numPoints += clippedGeometry.getNumPoints
          geometries.append((clippedGeometry, title))
          true
        }
    }
    if (numPoints > 1000) {
      for (geometry <- geometries)
        rasterizeGeometry(geometry._1)
      geometries.clear()
    }
    changed
  }

  /**
   * Plot the given geometry to the blocked pixels
   * @param geometry the geometry to plot. The goemetry should already by in the image space.
   * @return `true` if the pixels changed as a result of this step. Not 100% accurate, through
   */
  private[davinci] def rasterizeGeometry(geometry: Geometry): Boolean = {
    var changed: Boolean = false
    geometry match {
      case _: Point | _: PointND =>
        val coordinate: Coordinate = geometry.getCoordinate
        val x = coordinate.x.round.toInt
        val y = coordinate.y.round.toInt
        // The point is rasterized to the fill pixels
        val offset = pointOffset(x, y)
        if (!occupiedPixels.get(offset)) {
          occupiedPixels.set(offset, true)
          changed = true
        }
      case e: EnvelopeND =>
        val x1 = e.getMinCoord(0).round.toInt
        val x2 = e.getMaxCoord(0).round.toInt
        val y1 = e.getMinCoord(1).round.toInt
        val y2 = e.getMaxCoord(1).round.toInt
        // The interior of the envelope is rasterized to the fill region
        for (y <- y1 until y2) {
          val offset = pointOffset(x1, y)
          occupiedPixels.setRange(offset, offset + (x2 - x1), true)
        }
        // For simplicity, we always set it to true to avoid checking which pixels changed
        changed = true
      case ls: LineString =>
        val coordinates: CoordinateSequence = ls.getCoordinateSequence
        if (coordinates.size() > 1) {
          changed = true
          var x1: Int = coordinates.getX(0).round.toInt
          var y1: Int = coordinates.getY(0).round.toInt
          for (i <- 1 until coordinates.size()) {
            val x2: Int = coordinates.getX(i).round.toInt
            val y2: Int = coordinates.getY(i).round.toInt
            rasterizeLineToBits(x1, y1, x2, y2, occupiedPixels)
            x1 = x2
            y1 = y2
          }
        }
      case p: Polygon =>
        changed = true
        // For a polygon, fill all the pixels inside it without the holes
        // First, compute all intersections between polygon segments and horizontal scan lines
        val allIntersections = new mutable.ArrayBuffer[(Int, Int)]
        for (iHole <- 0 to p.getNumInteriorRing) {
          val ring: LinearRing = if (iHole == 0) p.getExteriorRing else p.getInteriorRingN(iHole - 1)
          val coordinates: CoordinateSequence = ring.getCoordinateSequence
          if (coordinates.size() > 1) {
            var x1: Double = coordinates.getX(0)
            var y1: Double = coordinates.getY(0)
            for (i <- 1 until coordinates.size()) {
              val x2: Double = coordinates.getX(i)
              val y2: Double = coordinates.getY(i)
              // Find the intersection between the line (x1, y1) - (x2, y2) and all center horizontal lines
              findIntersections(x1, y1, x2, y2, allIntersections)
              // Update to the next line segment
              x1 = x2
              y1 = y2
            }
          }
        }
        val sortedPixels = allIntersections.sortWith((a,b) => {
          if (a._2 != b._2)
            a._2 < b._2
          else
            a._1 < b._1
        })
        assert(sortedPixels.length % 2 == 0, s"Failed to rasterize polygon $p")
        var i = 0
        while (i < sortedPixels.length) {
          val p1 = sortedPixels(i)
          val p2 = sortedPixels(i + 1)
          assert(p1._2 == p2._2, s"Failed to rasterize polygon $p")
          val startOffset = pointOffset(p1._1 max -bufferSize, p1._2)
          val endOffset = pointOffset(p2._1 min (width + bufferSize - 1), p2._2)
          occupiedPixels.setRange(startOffset, endOffset + 1, true)
          i += 2
        }
      case gc: GeometryCollection =>
        for (i <- 0 until gc.getNumGeometries)
          rasterizeGeometry(gc.getGeometryN(i))
    }
    changed
  }

  /**
   * Find all intersections between the given line segment and the horizontal scan line centers
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   * @param intersections all computed intersections will be appended to this list
   */
  private[davinci] def findIntersections(_x1: Double, _y1: Double, _x2: Double, _y2: Double,
                                         intersections: mutable.ArrayBuffer[(Int, Int)]): Unit = {
    var x1 = _x1
    var y1 = _y1
    var x2 = _x2
    var y2 = _y2
    if (y1 > y2) {
      // Swap the two points
      var temp = x1
      x1 = x2
      x2 = temp
      temp = y1
      y1 = y2
      y2 = temp
    }
    var y = (y1 - 0.5).ceil + 0.5
    while (y < y2) {
      val x = x1 + (y - y1) * (x2-x1) / (y2-y1)
      intersections.append((x.toInt, y.toInt))
      y += 1
    }
  }


  /**
   * Draw a single line to the given bit array
   * @param x1
   * @param y1
   * @param x2
   * @param y2
   */
  private[davinci] def rasterizeLineToBits(_x1: Int, _y1: Int, _x2: Int, _y2: Int, bits: BitArray): Unit = {
    var x1 = _x1
    var x2 = _x2
    var y1 = _y1
    var y2 = _y2
    if (y1 == y2 || x1 == x2) {
      if (x1 > x2) {
        x1 ^= x2
        x2 ^= x1
        x1 ^= x2
      }
      if (y1 > y2) {
        y1 ^= y2
        y2 ^= y1
        y1 ^= y2
      }
      if (x2 < 0 || y2 < 0) return
      if (x1 >= width && x1 >= height) return
      if (x1 < 0) x1 = 0
      if (y1 < 0) y1 = 0
      if (x2 >= width) x2 = width - 1
      if (y2 >= height) y2 = height - 1
      // Draw an orthogonal line
      var offset: Int = pointOffset(x1, y1)
      val shift: Int = if (x1 == x2) width + 2 * bufferSize else 1
      for (_ <- (x1 + y1) to (x2 + y2)) {
        bits.set(offset, true)
        offset += shift
      }
    }
    else {
      val dx = Math.abs(x2 - x1)
      val dy = Math.abs(y2 - y1)
      if (dx > dy) {
        if (x1 > x2) { // Ensure that x1 <= x2
          x1 ^= x2
          x2 ^= x1
          x1 ^= x2
          y1 ^= y2
          y2 ^= y1
          y1 ^= y2
        }
        val incy = if (y1 < y2) 1
        else -1
        var p = dy - dx / 2
        var y = y1
        for (x <- x1 to x2) { // Use dumpRect rather than setPixel because we do not want to apply the transformation
          bits.set(pointOffset(x, y), true)
          if (p > 0) {
            y += incy
            p += dy - dx
          }
          else p += dy
        }
      }
      else {
        if (y1 > y2) { // Ensure that y1 < y2
          x1 ^= x2
          x2 ^= x1
          x1 ^= x2
          y1 ^= y2
          y2 ^= y1
          y1 ^= y2
        }
        val incx = if (x1 < x2) 1
        else -1
        var p = dx - dy / 2
        var x = x1
        for (y <- y1 to y2) {
          bits.set(pointOffset(x, y), true)
          if (p > 0) {
            x += incx
            p += dx - dy
          }
          else p += dx
        }
      }
    }
  }

  /**
   * Returns the offset of the given pixel in the [[occupiedPixels]] bit array.
 *
   * @param x the column of the pixel
   * @param y the row of the pixel
   * @return the offset of the pixel in the bit mask or -1 if it is out of bounds
   */
  private def pointOffset(x: Int, y: Int): Int =
    if (x < -bufferSize || x >= width + bufferSize || y < -bufferSize || y >= height + bufferSize) -1
    else (y + bufferSize) * (width + 2 * bufferSize) + (x + bufferSize)

  /**
   * Merges this canvas with another vector canvas and returns this canvas after the merge.
   * @param another the other canvas to merge with
   * @return this canvas after the merge so that you can chain a number of mergeWidth operations.
   */
  def mergeWith(another: VectorCanvas): VectorCanvas = {
    for (g <- another.geometries) {
      if (g._1.getGeometryType == "Point") {
        // Skip point geometries if their pixel is already occupied
        val coordinate: Coordinate = g._1.getCoordinate
        val pixelOffset: Int = pointOffset(coordinate.x.round.toInt, coordinate.y.round.toInt)
        if (pixelOffset != -1 && !occupiedPixels.get(pixelOffset))
          this.geometries.append(g)
      } else {
        this.geometries.append(g)
      }
    }
    this.occupiedPixels.inplaceOr(another.occupiedPixels)
    this.numPoints += another.numPoints
    // Check if any of the current points should be removed because of falling in a blocked region
    if (this.numPoints > 1000) {
      // Rasterize all geometries
      for (geometry <- this.geometries)
        this.rasterizeGeometry(geometry._1)
      this.geometries.clear()
    }
    this
  }

  /**
   * Write the geometries in this canvas as an SVG image to the given output.
   * This method follows the Tiny SVG standard [[https://www.w3.org/TR/SVGTiny12/index.html]] for a wide compatibility
   * of devices and applications.
   * @param out the output to write the SVG image to.
   * @param pointRadius the radius of points
   * @param strokeWidth the width of the stroke in pixels for path objects only
   * @param strokeColor the color of the stroke
   * @param fillColor the color of the fill
   * @param vflip whether to vertically flip the image at the end
   */
  def writeAsSVG(out: OutputStream, pointRadius: Double, strokeWidth: Double,
                 strokeColor: Color, fillColor: Color, vflip: Boolean): Unit = {
    val xmlOutputFactory = XMLOutputFactory.newInstance
    val xmlWriter: XMLStreamWriter = xmlOutputFactory.createXMLStreamWriter(out, "UTF-8")
    try {
      // Write the header of the SVG image
      xmlWriter.writeStartDocument("UTF-8", "1.0")
      xmlWriter.writeStartElement("svg")
      xmlWriter.writeAttribute("xmlns", "http://www.w3.org/2000/svg")
      xmlWriter.writeAttribute("version", "1.2")
      xmlWriter.writeAttribute("baseProfile", "tiny")
      xmlWriter.writeAttribute("width", s"${width}px")
      xmlWriter.writeAttribute("height", s"${height}px")

      // Combine all geometries in one group to avoid styling each element individually
      xmlWriter.writeStartElement("g")
      if (fillColor.getAlpha != 0) {
        xmlWriter.writeAttribute("fill", f"#${fillColor.getRed}%02x${fillColor.getGreen}%02x${fillColor.getBlue}%02x")
        if (fillColor.getAlpha != 255)
          xmlWriter.writeAttribute("fill-opacity", f"${fillColor.getAlpha / 255.0}")
      } else {
        xmlWriter.writeAttribute("fill", f"none")
      }
      if (strokeColor.getAlpha != 0) {
        xmlWriter.writeAttribute("stroke", f"#${strokeColor.getRed}%02x${strokeColor.getGreen}%02x${strokeColor.getBlue}%02x")
        if (strokeColor.getAlpha != 255)
          xmlWriter.writeAttribute("stroke-opacity", f"${strokeColor.getAlpha / 255.0}")
      } else {
        xmlWriter.writeAttribute("stroke", "none")
      }
      if (vflip) {
        // Flip the image vertically using a combination of scale and translate
        xmlWriter.writeAttribute("transform", "scale(1,-1)")
        xmlWriter.writeStartElement("g")
        xmlWriter.writeAttribute("transform", s"translate(0,-$height)")
      }

      // Write the geometries
      for (g <- geometries)
        plotGeometryToSVG(g._1, g._2, pointRadius, strokeWidth, xmlWriter)

      if (geometries.isEmpty) {
        // If no geometries exist, it means that all of them have been rasterized
        val rings: Array[LinearRing] = createRingsForOccupiedPixels
        if (rings.nonEmpty) {
          // Add all rings to one path
          val definition = new StringBuilder()
          for (ring <- rings)
            appendLineStringToPathDefinition(ring, definition)
          xmlWriter.writeEmptyElement("path")
          xmlWriter.writeAttribute("d", definition.toString())
        }
      }

      // Close the group
      xmlWriter.writeEndElement()
    } finally {
      // Close the document
      xmlWriter.writeEndDocument()
      xmlWriter.close()
    }
  }

  /**
   * Append the given line string to the path definition given as a StringBuilder
   * @param linestring the linestring to append its definition
   * @param definition the definition to append to
   */
  private[davinci] def appendLineStringToPathDefinition(linestring: LineString, definition: StringBuilder): Unit = {
    val lastPoint: Int = if (linestring.isClosed) linestring.getNumPoints - 1 else linestring.getNumPoints
    for (i <- 0 until lastPoint) {
      if (i == 0)
        definition.append("M")
      else if (i == 1)
        definition.append("L")
      val coordinate = linestring.getCoordinateN(i)
      definition.append(coordinate.x.toInt)
      definition.append(" ")
      definition.append(coordinate.y.toInt)
      definition.append(" ")
    }
    if (linestring.isClosed)
      definition.append("Z ")
  }

  /**
   * Plot the given geometry to SVG and write it to the given writer
   * @param geometry the geometry to plot. The geometry should be already in the image space.
   * @param radius the radius of point or the stroke width
   * @param strokeWidth the stroke width for non-point, i.e, path, geometries
   * @param xmlWriter the XML writer to write the SVG to
   */
  def plotGeometryToSVG(geometry: Geometry, title: String, radius: Double, strokeWidth: Double, xmlWriter: XMLStreamWriter): Unit = {
    try {
      geometry match {
        case _: Point | _: PointND =>
          val coordinate: Coordinate = geometry.getCoordinate
          val x: Double = coordinate.x
          val y: Double = coordinate.y
          // Calculate the radius of the point based on the nearest occupied pixel
          var pointRadius: Int = 1
          var xCheck: Int = x.round.toInt
          var yCheck: Int = y.round.toInt
          var direction: (Int, Int) = (1, 0) // Start by moving right
          var hitBlock: Boolean = false
          while (!hitBlock && pointRadius < 2 * radius) {
            xCheck -= 1
            yCheck -= 1
            val sideLength: Int = pointRadius * 2
            var steps: Int = sideLength * 4
            while (steps > 0 && !hitBlock) {
              val offset = pointOffset(xCheck, yCheck)
              hitBlock = offset != -1 && occupiedPixels.get(offset)
              xCheck += direction._1
              yCheck += direction._2
              steps -= 1
              if (steps % sideLength == 0)
                direction = turnRight(direction)
            }
            if (!hitBlock)
              pointRadius += 1
          }
          if (title == null) xmlWriter.writeEmptyElement("circle") else xmlWriter.writeStartElement("circle")
          xmlWriter.writeAttribute("cx", f"$x%.1f")
          xmlWriter.writeAttribute("cy", f"$y%.1f")
          xmlWriter.writeAttribute("r", (pointRadius / 2).toString)
          if (title != null) {
            xmlWriter.writeStartElement("title")
            xmlWriter.writeCharacters(title)
            xmlWriter.writeEndElement()
            xmlWriter.writeEndElement()
          }
        case l: LineString =>
          val pathDefinition = new StringBuilder()
          appendLineStringToPathDefinition(l, pathDefinition)
          if (pathDefinition.nonEmpty) {
            if (title == null) xmlWriter.writeEmptyElement("path") else xmlWriter.writeStartElement("path")
            xmlWriter.writeAttribute("d", pathDefinition.toString())
            xmlWriter.writeAttribute("fill", "none")
            if (strokeWidth != 0)
              xmlWriter.writeAttribute("stroke-width", strokeWidth.toString)
            if (title != null) {
              xmlWriter.writeStartElement("title")
              xmlWriter.writeCharacters(title)
              xmlWriter.writeEndElement()
              xmlWriter.writeEndElement()
            }
          }
        case polygon: Polygon =>
          val pathDefinition = new StringBuilder()
          appendLineStringToPathDefinition(polygon.getExteriorRing, pathDefinition)
          for (i <- 0 until polygon.getNumInteriorRing)
            appendLineStringToPathDefinition(polygon.getInteriorRingN(i), pathDefinition)
          if (pathDefinition.nonEmpty) {
            if (title == null) xmlWriter.writeEmptyElement("path") else xmlWriter.writeStartElement("path")
            xmlWriter.writeAttribute("d", pathDefinition.toString())
            if (strokeWidth != 0)
              xmlWriter.writeAttribute("stroke-width", strokeWidth.toString)
            if (title != null) {
              xmlWriter.writeStartElement("title")
              xmlWriter.writeCharacters(title)
              xmlWriter.writeEndElement()
              xmlWriter.writeEndElement()
            }
          }
        case gc: GeometryCollection =>
          for (i <- 0 until gc.getNumGeometries)
            plotGeometryToSVG(gc.getGeometryN(i), title, radius, strokeWidth, xmlWriter)
      }
    } catch {
      case e: Exception => throw new RuntimeException(s"Error plotting '$geometry' to tile $tileID", e);
    }
  }

  /**
   * transforms the given linestring to image space and appends its definition to the given StringBuilder.
   * @param l the line string to transform
   * @param pathDefinition the path definition to append to
   */
  private [davinci] def transformAndAppendLineStringToPathDefinition(l: LineString,
                                                                     pathDefinition: mutable.StringBuilder): Unit = {
    val coordinates: CoordinateSequence = l.getCoordinateSequence
    for (i <- 0 until coordinates.size()) {
      coordinates.setOrdinate(i, 0, transformX(coordinates.getOrdinate(i, 0)))
      coordinates.setOrdinate(i, 1, transformY(coordinates.getOrdinate(i, 1)))
    }
    appendLineStringToPathDefinition(l, pathDefinition)
  }

  /**
   * Creates one linear ring for each contiguous part of occupied pixels. A pixel is connected to the four pixels
   * to its west, east, north, and south. The linear ring is returned in image space based on the pixel location.
   * Think of the corners of pixels as points in space that are connected with orthogonal lines to form the
   * linear rings.
   * @return
   */
  private[davinci] def createRingsForOccupiedPixels: Array[LinearRing] = {
    // Draw the blocked regions
    // Vertices at the top starting at each column, or null if there is no vertical line
    val topVertices: Array[Vertex] = new Array[Vertex](width + 2 * bufferSize + 1)
    // The location of the left vertex or null if there is no left vertex
    var leftVertex: Vertex = null
    // Store a list of top-left corners to be able to trace all rings afterward
    val corners = new ArrayBuffer[Vertex]()
    // Row width is used to advance the offset more efficiently
    val scanlineSize: Int = width + 2 * bufferSize
    var offset: Int = pointOffset(-bufferSize, -bufferSize)
    for (y <- -bufferSize to height + bufferSize) {
      assert(y == height + bufferSize || offset == pointOffset(-bufferSize, y))
      for (x <- -bufferSize to width + bufferSize) {
        val blocked0: Int = if (x > -bufferSize && y > -bufferSize && occupiedPixels.get(offset - scanlineSize - 1)) 1 else 0
        val blocked1: Int = if (x < width + bufferSize && y > -bufferSize && occupiedPixels.get(offset - scanlineSize)) 2 else 0
        assert((blocked0 == 0) == (blocked1 == 0) || topVertices(x + bufferSize) != null)
        val blocked2: Int = if (x > -bufferSize && y < height + bufferSize && occupiedPixels.get(offset - 1)) 4 else 0
        val blocked3: Int = if (x < width + bufferSize && y < height + bufferSize && occupiedPixels.get(offset)) 8 else 0
        assert((blocked0 == 0) == (blocked2 == 0) || leftVertex != null)
        val pixelType: Int = blocked0 | blocked1 | blocked2 | blocked3
        pixelType match {
          case 0 | 3 | 5 | 10 | 12 | 15 => // Do nothing
          case 1 =>
            // +---+---+
            // +XXX^   +
            // +->-+---+
            // +   +   +
            // +---+---+
            // Add a new vertex that connects left to top
            val newVertex = Vertex(x, y, topVertices(x + bufferSize))
            leftVertex.next = newVertex
            topVertices(x + bufferSize) = null
          case 2 =>
            // +---+---+
            // +   vXXX+
            // +---+->-+
            // +   +   +
            // +---+---+
            // Add a new vertex that follows the one at the top
            // Keep its next link open until we find the next corner on the same line
            val newVertex = Vertex(x, y, null)
            topVertices(x + bufferSize).next = newVertex
            leftVertex = newVertex
          case 4 =>
            // +---+---+
            // +   +   +
            // +-<-+---+
            // +XXX^   +
            // +---+---+
            // Add a new vertex that connects to the left. Keep it open until we find the next corner down
            val newVertex = Vertex(x, y, leftVertex)
            topVertices(x + bufferSize) = newVertex
            leftVertex = null
          case 6 =>
            // +---+---+
            // +   vXXX+
            // +-<-+->-+
            // +XXX^   +
            // +---+---+
            // Create two coinciding vertices at the center.
            // One connects to the left and is left open to find the bottom vertex
            // The second follows the one at the top and is left open until we find the vertex to the right
            val newVertex1 = Vertex(x, y, leftVertex)
            val newVertex2 = Vertex(x, y, null)
            topVertices(x + bufferSize).next = newVertex2
            topVertices(x + bufferSize) = newVertex2
            leftVertex = newVertex2
            topVertices(x + bufferSize) = newVertex1
          case 7 =>
            // +---+---+
            // +XXX+XXX+
            // +---+->-+
            // +XXX^   +
            // +---+---+
            // Create a new vertex that connects the bottom (not known yet) to the right (not known yet)
            val newVertex = Vertex(x, y, null)
            topVertices(x + bufferSize) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 8 =>
            // +---+---+
            // +   +   +
            // +---+-<-+
            // +   vXXX+
            // +---+---+
            // Create a new vertex that connects the right (not known yet) to the bottom (not known yet)
            val newVertex = Vertex(x, y, null)
            topVertices(x + bufferSize) = newVertex
            leftVertex = newVertex
            corners.append(newVertex)
          case 9 =>
            // +---+---+
            // +XXX^   +
            // +->-+-<-+
            // +   vXXX+
            // +---+---+
            // Create two coinciding vertices at the center
            // 1- First vertex connects the left (known) to the top (known)
            // 2- Second vertex connects the right (unknown) to the bottom (unknown)
            val newVertex1 = Vertex(x, y, topVertices(x + bufferSize))
            leftVertex.next = newVertex1
            val newVertex2 = Vertex(x, y, null)
            leftVertex = newVertex2
            topVertices(x + bufferSize) = newVertex2
            corners.append(newVertex2)
          case 11 =>
            // +---+---+
            // +XXX+XXX+
            // +->-+---+
            // +   vXXX+
            // +---+---+
            // Create a vertex that connects left (known) to the bottom (unknown)
            val newVertex = Vertex(x, y, null)
            leftVertex.next = newVertex
            leftVertex = null
            topVertices(x + bufferSize) = newVertex
          case 13 =>
            // +---+---+
            // +XXX^   +
            // +---+-<-+
            // +XXX+XXX+
            // +---+---+
            // Create a new vertex that connects right (unknown) to top (known)
            val newVertex = Vertex(x, y, topVertices(x + bufferSize))
            leftVertex = newVertex
            topVertices(x + bufferSize) = null
          case 14 =>
            // +---+---+
            // +   vXXX+
            // +-<-+---+
            // +XXX+XXX+
            // +---+---+
            // Create a new vertex that connects top (known) to left (known)
            val newVertex = Vertex(x, y, leftVertex)
            topVertices(x + bufferSize).next = newVertex
            leftVertex = null
            topVertices(x + bufferSize) = null
        }
        offset += 1
      }
      offset = offset - (width + 2 * bufferSize + 1) + scanlineSize
    }
    // Phase II: Trace all linear rings to produce the final output. Always start with a non-visited corner
    val factory: GeometryFactory = GeometryReader.DefaultGeometryFactory
    val rings = new ArrayBuffer[LinearRing]()
    for (corner <- corners; if !corner.visited) {
      // First, count the number of corners to prepare a CoordinateSequence of the right size
      var iCorner = 0
      var p = corner
      do {
        p = p.next
        iCorner += 1
      } while (p != corner)
      val coords = factory.getCoordinateSequenceFactory.create(iCorner + 1, 2)
      p = corner
      iCorner = 0
      do {
        coords.setOrdinate(iCorner, 0, p.x)
        coords.setOrdinate(iCorner, 1, p.y)
        p.visited = true
        p = p.next
        iCorner += 1
      } while (p != corner)
      // Make last coordinate similar to the first one
      coords.setOrdinate(iCorner, 0, p.x)
      coords.setOrdinate(iCorner, 1, p.y)
      // Create the linear ring
      rings.append(factory.createLinearRing(coords))
    }
    rings.toArray
  }

  /**
   * Return the direction that turn 90 degrees to the right of the given direction
   * @param direction the given direction
   * @return the direction that turns right
   */
  private def turnRight(direction: (Int, Int)): (Int, Int) = (direction._2, -direction._1)
}

/**
 * A vertex in a linked list of vertices
 * @param x the x-coordinate of the vertex on this edge
 * @param y the y-coordinate of the vertex on this edge
 * @param next a link to the next connected edge or `null` if this is the last edge found so far
 */
case class Vertex(x: Int, y: Int, var next: Vertex, var visited: Boolean = false)