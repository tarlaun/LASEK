/*
 * Copyright 2023 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.cg

import org.geotools.geometry.DirectPosition2D
import org.geotools.referencing.operation.transform.IdentityTransform
import org.locationtech.jts.geom.Envelope
import org.opengis.geometry.DirectPosition
import org.opengis.referencing.operation.{MathTransform, Matrix}

/**
 * A MathTransform that snaps points that are outside bounds to the nearest point on the bounds.
 * @param bounds the bounds that define the output range
 */
class SnapTransform(bounds: Envelope) extends MathTransform {

  override def getSourceDimensions: Int = 2

  override def getTargetDimensions: Int = 2

  override def transform(ptSrc: DirectPosition, ptDst: DirectPosition): DirectPosition = {
    val x = bounds.getMinX max ptSrc.getOrdinate(0) min bounds.getMaxX
    val y = bounds.getMinY max ptSrc.getOrdinate(1) min bounds.getMaxY
    if (ptDst == null) {
      new DirectPosition2D(x, y)
    } else {
      ptDst.setOrdinate(0, x)
      ptDst.setOrdinate(1, y)
      ptDst
    }
  }

  override def transform(srcPts: Array[Double], srcOff: Int, dstPts: Array[Double], dstOff: Int, numPts: Int): Unit = {
    if (bounds == null) {
      System.arraycopy(srcPts, srcOff, dstPts, dstOff, numPts * 2)
      return
    }
    var iSrc = srcOff
    var iDst = dstOff
    for (_ <- 0 until numPts) {
      // ignore when not overlap
      var overlap = true
      if ((srcPts(iSrc) > bounds.getMinX && srcPts(iSrc) > bounds.getMaxX) || (srcPts(iSrc) < bounds.getMinX && srcPts(iSrc) < bounds.getMaxX) ||
        (srcPts(iSrc + 1) > bounds.getMinY && srcPts(iSrc + 1) > bounds.getMaxY) || (srcPts(iSrc + 1) < bounds.getMinY && srcPts(iSrc + 1) < bounds.getMaxY)) {
        overlap = false
      }
      if (overlap) { // partial overlap
        dstPts(iDst) = bounds.getMinX max srcPts(iSrc) min bounds.getMaxX
        dstPts(iDst + 1) = bounds.getMinY max srcPts(iSrc + 1) min bounds.getMaxY
      }
      iSrc += 2
      iDst += 2
    }
  }

  override def transform(srcPts: Array[Float], srcOff: Int, dstPts: Array[Float], dstOff: Int, numPts: Int): Unit = {
    var iSrc = srcOff
    var iDst = dstOff
    for (_ <- 0 until numPts) {
      dstPts(iDst) = (bounds.getMinX max srcPts(iSrc) min bounds.getMaxX).toFloat
      dstPts(iDst + 1) = (bounds.getMinY max srcPts(iSrc + 1) min bounds.getMaxY).toFloat
      iSrc += 2
      iDst += 2
    }
  }

  override def transform(srcPts: Array[Float], srcOff: Int, dstPts: Array[Double], dstOff: Int, numPts: Int): Unit = {
    var iSrc = srcOff
    var iDst = dstOff
    for (_ <- 0 until numPts) {
      dstPts(iDst) = bounds.getMinX max srcPts(iSrc) min bounds.getMaxX
      dstPts(iDst + 1) = bounds.getMinY max srcPts(iSrc + 1) min bounds.getMaxY
      iSrc += 2
      iDst += 2
    }
  }

  override def transform(srcPts: Array[Double], srcOff: Int, dstPts: Array[Float], dstOff: Int, numPts: Int): Unit = {
    var iSrc = srcOff
    var iDst = dstOff
    for (_ <- 0 until numPts) {
      dstPts(iDst) = (bounds.getMinX max srcPts(iSrc) min bounds.getMaxX).toFloat
      dstPts(iDst + 1) = (bounds.getMinY max srcPts(iSrc + 1) min bounds.getMaxY).toFloat
      iSrc += 2
      iDst += 2
    }
  }

  override def derivative(point: DirectPosition): Matrix = ???

  /** This is not exactly an invertible transformation since it is lossless */
  override def inverse(): MathTransform = IdentityTransform.create(2)

  override def isIdentity: Boolean = false

  override def toWKT: String = s"Unsupported: Source Bounds ${bounds}"
}
