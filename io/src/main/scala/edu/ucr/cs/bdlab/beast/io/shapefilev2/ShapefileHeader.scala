/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io.shapefilev2

import edu.ucr.cs.bdlab.beast.util.IOUtil
import org.locationtech.jts.geom.Envelope

import java.io.{DataInput, DataOutput}

case class ShapefileHeader(fileLength: Int,
                           version: Int,
                           shapeType: Int,
                           zmin: Double,
                           zmax: Double,
                           mmin: Double,
                           mmax: Double
                           ) extends Envelope {
  override def toString: String = {
    var parts = Array(s"fileLength=$fileLength",
        s"version=$version",
        s"shapeType=$shapeType",
        s"envelope=($getMinX,$getMinY)-($getMaxX,$getMaxY)")
    if (!zmin.isNaN)
      parts = parts :+ s"zrange=[$zmin,$zmax]"
    if (!mmin.isNaN)
      parts = parts :+ s"mrange=[$mmin,$mmax]"
    s"Header{${parts.mkString(", ")}"
  }
}

object ShapefileHeader {
  /** The signature at the beginning of the Shapefile */
  val Signature: Int = 9994

  /** Marker for empty shapes in Shapefile */
  val NullShape: Int = 0

  /** Marker for point shapes in Shapefile with x and y coordinates */
  val PointShape: Int = 1

  /** Marker for multi-point shapes in Shapefile */
  val MultiPointShape: Int = 8

  /** Marker for polyline (linestring) shapes in Shapefile */
  val PolylineShape: Int = 3

  /** Marker for polygon shapes in Shapefile */
  val PolygonShape: Int = 5

  /** Marker for point shapes in Shapefile with x, y, and m attributes */
  val PointMShape: Int = 21

  /** Marker for multi-point shapes in Shapefile with x, y, and m attributes */
  val MultiPointMShape: Int = 28

  /** Marker for polyline (linestring) shapes in Shapefile with x, y, and m attributes */
  val PolylineMShape: Int = 23

  /** Marker for polygon shapes in Shapefile with x, y, and m attributes */
  val PolygonMShape: Int = 25

  /** Marker for point shapes in Shapefile with x, y, z, and m attributes */
  val PointMZShape: Int = 11

  /** Marker for multi-point shapes in Shapefile with x, y, z, and m attributes */
  val MultiPointMZShape: Int = 18

  /** Marker for polyline (linestring) shapes in Shapefile with x, y, z, and m attributes */
  val PolylineMZShape: Int = 13

  /** Marker for polygon shapes in Shapefile with x, y, z, and m attributes */
  val PolygonMZShape: Int = 15

  /** Marker for multipatch shapes in Shapefile with x, y, z, and m attributes */
  val MultiPatchMZShape: Int = 31

  def readHeader(in: DataInput): ShapefileHeader = {
    val code = in.readInt()
    if (code != Signature)
      throw new RuntimeException(s"Invalid Shapefile code $code. Expected $Signature")
    in.skipBytes(5 * 4) // Skip the five unused integers
    val fileLength = in.readInt()
    val version = IOUtil.readIntLittleEndian(in)
    val shapeType = IOUtil.readIntLittleEndian(in)
    val xmin = IOUtil.readDoubleLittleEndian(in)
    val ymin = IOUtil.readDoubleLittleEndian(in)
    val xmax = IOUtil.readDoubleLittleEndian(in)
    val ymax = IOUtil.readDoubleLittleEndian(in)
    val zmin = IOUtil.readDoubleLittleEndian(in)
    val zmax = IOUtil.readDoubleLittleEndian(in)
    val mmin = IOUtil.readDoubleLittleEndian(in)
    val mmax = IOUtil.readDoubleLittleEndian(in)
    val header = ShapefileHeader(fileLength, version, shapeType, zmin, zmax, mmin, mmax)
    header.init(xmin, xmax, ymin, ymax)
    header
  }

  def writeHeader(out: DataOutput, header: ShapefileHeader): Unit = {
    out.writeInt(Signature)
    out.writeInt(0) // FIve unused integers
    out.writeInt(0)
    out.writeInt(0)
    out.writeInt(0)
    out.writeInt(0)
    out.writeInt(header.fileLength)
    IOUtil.writeIntLittleEndian(out, header.version)
    IOUtil.writeIntLittleEndian(out, header.shapeType)
    IOUtil.writeDoubleLittleEndian(out, header.getMinX)
    IOUtil.writeDoubleLittleEndian(out, header.getMinY)
    IOUtil.writeDoubleLittleEndian(out, header.getMaxX)
    IOUtil.writeDoubleLittleEndian(out, header.getMaxY)
    IOUtil.writeDoubleLittleEndian(out, header.zmin)
    IOUtil.writeDoubleLittleEndian(out, header.zmax)
    IOUtil.writeDoubleLittleEndian(out, header.mmin)
    IOUtil.writeDoubleLittleEndian(out, header.mmax)
  }
}
