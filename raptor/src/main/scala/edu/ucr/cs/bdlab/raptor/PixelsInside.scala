package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.beast.util.CompactLongArray
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform}
import org.locationtech.jts.geom.{CoordinateSequence, Geometry, MultiPolygon, Polygon}
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.AffineTransform
import scala.collection.mutable.ArrayBuffer


/**
 * Computes and iterates over the pixels that are completely inside a set of query polygons
 * @param polygons the set of query polygons
 * @param metadata the metadata of the raster
 */
class PixelsInside(polygons: Array[_<:Geometry], metadata: RasterMetadata)
  extends Iterator[(Int, Int, Int)] {

  /**
   * Stores the an intersection location in pixel/grid coordinates
   * @param pID the query polygon ID
   * @param i the index of the intersection column
   * @param j the index of the intersection row
   */
  case class Intersection(pID: Int, i: Int, j: Int) extends Comparable[Intersection] {
    override def compareTo(o: Intersection): Int = {
      var diff: Int = this.j - o.j
      if (diff == 0)
        diff = this.pID - o.pID
      if (diff == 0)
        diff = this.i - o.i
      diff
    }
  }

  /**
   * A range of intersections in raster coordinates
   * @param pID the ID of the query polygon
   * @param j the row of the intersection
   * @param i1 the first column of the intersection (inclusive)
   * @param i2 the last column of the intersection (inclusive)
   */
  case class IntersectionRange(pID: Int, j: Int, i1: Int, i2: Int) extends Comparable[IntersectionRange] {
    override def compareTo(o: IntersectionRange): Int = {
      var diff: Int = this.j - o.j
      if (diff == 0)
        diff = this.pID - o.pID
      if (diff == 0)
        diff = this.i1 - o.i1
      if (diff == 0)
        diff = this.i2 - o.i2
      diff
    }
  }

  /** The query polygon ID for intersections */
  var intersectionPId: CompactLongArray.CLLIterator = _

  /** The x-coordinate in raster space of pixels that are completely inside */
  var intersectionX: CompactLongArray.CLLIterator = _

  /** The y-coordinate of the pixels that are completely inside */
  var intersectionY: CompactLongArray.CLLIterator = _

  compute(polygons, metadata)

  /**
   * Compute intersections for a set of geometries.
   * @param geometries the geometries to compute the intersections for. Must be polygons.
   * @param metadata the metadata of the raster data
   * @return the set of pixels that are inside any of the polygons
   */
  protected def compute(geometries: Array[_<:Geometry], metadata: RasterMetadata): Unit = {
    /** Intersections between polygon boundaries and horizontal grid lines */
    val horizontalIntersections = new collection.mutable.ArrayBuffer[Intersection]()

    /** Intersections between polygon boundaries and vertical grid lines */
    val verticalIntersections = new collection.mutable.ArrayBuffer[Intersection]()

    /** Pixels that contain at least one polygon vertex */
    val cornerPixels = new collection.mutable.ArrayBuffer[Intersection]()

    /**
     * Compute the intersections for a query polygon
     * @param pID the ID of the query polygon
     * @param polygon the geometry of the query polygon
     * @param transform the transformation that transforms the query polygon to the raster space
     * @param w the width of the raster in pixels
     * @param h the height of the raster in pixels
     */
    def computeForPolygon(pID: Int, polygon: Polygon, transform: MathTransform, w: Int, h: Int): Unit = {
      for (iRing <- 0 to polygon.getNumInteriorRing) {
        val ring = if (iRing == 0) polygon.getExteriorRing else polygon.getInteriorRingN(iRing - 1)
        val cs = ring.getCoordinateSequence.copy()
        val coords = new Array[Double](cs.size() * 2)
        for (iPoint <- 0 until cs.size()) {
          coords(iPoint * 2) = cs.getX(iPoint)
          coords(iPoint * 2 + 1) = cs.getY(iPoint)
        }
        transform.transform(coords, 0, coords, 0, coords.length / 2)
        for (iPoint <- 0 until cs.size()) {
          cs.setOrdinate(iPoint, 0, coords(iPoint * 2))
          cs.setOrdinate(iPoint, 1, coords(iPoint * 2 + 1))
        }
        compute(pID, cs, w, h)
      }
    }

    /**
     * Compute the intersections for the given linear ring
     * @param pID the ID of the polygon
     * @param ring the list of coordinates that make the ring already projected to the raster space
     * @param w the width of the raster in pixels
     * @param h the height of the raster in pixels
     */
    def compute(pID: Int, ring: CoordinateSequence, w: Int, h: Int): Unit = {
      var x1 = ring.getX(0)
      var y1 = ring.getY(0)
      for (k <- 1 until ring.size()) {
        cornerPixels.append(Intersection(pID, x1.toInt, y1.toInt))
        val x2 = ring.getX(k)
        val y2 = ring.getY(k)
        // Compute horizontal intersections
        for (j <- ((y1 min y2).ceil.toInt max 0) until ((y1 max y2).ceil.toInt min h)) {
          val i: Int = (x1 * (y2-j) / (y2-y1) + x2 * (j-y1) / (y2 - y1)).floor.toInt
          horizontalIntersections.append(Intersection(pID, i, j))
        }
        // Compute vertical intersections
        for (i <- ((x1 min x2).ceil.toInt max 0) until ((x1 max x2).ceil.toInt min w)) {
          val j: Double = y1 * (x2-i) / (x2-x1) + y2 * (i-x1) / (x2 - x1)
          val jj: Int = j.floor.toInt
          // Skip the special case where the intersection is exactly at the pixel corner
          if (j != jj)
            verticalIntersections.append(Intersection(pID, i, jj))
        }
        x1 = x2
        y1 = y2
      }
    }

    for (pID <- geometries.indices) {
      val g = geometries(pID)
      var transform: MathTransform = new AffineTransform2D(new AffineTransform())
      if (g.getSRID != metadata.srid)
        transform = Reprojector.findTransformationInfo(g.getSRID, metadata.srid).mathTransform
      transform = ConcatenatedTransform.create(transform, new AffineTransform2D(metadata.g2m.createInverse()))
      g match {
        case polygon: Polygon => computeForPolygon(pID, polygon, transform, metadata.rasterWidth, metadata.rasterHeight)
        case ps: MultiPolygon =>
          for (n <- 0 until ps.getNumGeometries) {
            computeForPolygon(pID, ps.getGeometryN(n).asInstanceOf[Polygon], transform, metadata.rasterWidth, metadata.rasterHeight)
          }
      }

    }
    assert(horizontalIntersections.length % 2 == 0)
    // From the intersections, compute pixels that are completely inside
    val sortedH = horizontalIntersections.sorted.toArray
    val h: Array[IntersectionRange] = new Array[IntersectionRange](sortedH.length / 2)
    for (i <- h.indices) {
      assert(sortedH(i*2).j == sortedH(i*2+1).j)
      assert(sortedH(i*2).pID == sortedH(i*2+1).pID)
      h(i) = IntersectionRange(sortedH(i*2).pID, sortedH(i*2).j, sortedH(i*2).i, sortedH(i*2+1).i)
    }
    val vSet = verticalIntersections.toSet
    val cpSet = cornerPixels.toSet
    var k1 = 0
    var k2 = 0
    val pids = new ArrayBuffer[Long]()
    val xs = new ArrayBuffer[Long]()
    val ys = new ArrayBuffer[Long]()
    while (k1 < h.length) {
      var pID = h(k1).pID
      val j = h(k1).j
      val i1 = h(k1).i1
      val i2 = h(k1).i2
      while (k2 < h.length && (h(k2).j < j + 1 ||
            (h(k2).j == j + 1 && (h(k2).pID < pID || (h(k2).pID == pID && h(k2).i2 < i1)))))
        k2 += 1
      while (k2 < h.length && h(k2).pID == pID && h(k2).j == j+1 && h(k2).i1 < i2) {
        for (i <- (i1 max h(k2).i1) + 1 until (i2 min h(k2).i2)) {
          if (!vSet.contains(Intersection(pID, i, j)) && !vSet.contains(Intersection(pID, i+1, j))) {
            if (!cpSet.contains(Intersection(pID, i, j))) {
              pids.append(pID)
              xs.append(i)
              ys.append(j)
            }
          }
        }
        if (h(k2).i2 < i2)
          k2 += 1
        else // Force breaking the loop
          pID += 1
      }
      k1 += 1
    }
    intersectionPId = CompactLongArray.constructLongArray(pids.toArray).iterator()
    intersectionX = CompactLongArray.constructLongArray(xs.toArray).iterator()
    intersectionY = CompactLongArray.constructLongArray(ys.toArray).iterator()
  }

  override def hasNext: Boolean = intersectionPId.hasNext

  override def next(): (Int, Int, Int) = {
    val pid: Int = intersectionPId.next().toInt
    val i: Int = intersectionX.next().toInt
    val j: Int = intersectionY.next().toInt
    (pid, i, j)
  }
}
