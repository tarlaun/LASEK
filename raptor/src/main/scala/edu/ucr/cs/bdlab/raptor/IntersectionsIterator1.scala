package edu.ucr.cs.bdlab.raptor

import java.util


class IntersectionsIterator1(FileIDs:util.ArrayList[Int], intersectionsList : util.ArrayList[Intersections]) extends Iterator[(Long, PixelRange)]  {
  var i: Int = 0
  var j: Int = -1
  var intersections:Intersections = null
  var sizeI: Int = 0
  var TileID = 0
  var RasterSplitID : Long = 0
  var PolygonIndex: Int = 0
  var  Y = 0
  var X1 = 0
  var X2 = 0
  var FileID: Int = 0

  override def hasNext: Boolean = i < intersectionsList.size()
  //ToDo: what if all intersections are zero
  override def next(): (Long, PixelRange) = {
    if(j == -1)
    {
      intersections = intersectionsList.get(i)
      sizeI = intersections.getNumIntersections
      j=0
      FileID = FileIDs.get(i)
    }

    TileID = intersections.getTileID(j)
    PolygonIndex = intersections.getPolygonIndex(j)
    Y = intersections.getY(j)
    X1 = intersections.getX1(j)
    X2 = intersections.getX2(j)
    j=j+1
    RasterSplitID = ((FileID.toLong << 32) + TileID)

    if(j==sizeI)
    {
      i = i + 1
      j = -1
    }

    // System.out.println(PolygonIndex)
    (RasterSplitID, PixelRange(PolygonIndex,Y,X1,X2))
  }

  implicit def orderingByPolygonID[A <: IntersectionsIterator1 ] : Ordering[A] = {
    Ordering.by(x => (x.RasterSplitID,x.PolygonIndex,x.Y,x.X1))
  }
}
