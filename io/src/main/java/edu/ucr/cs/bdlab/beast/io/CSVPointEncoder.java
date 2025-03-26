package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Point;

import java.util.function.BiFunction;

public class CSVPointEncoder implements BiFunction<IFeature, StringBuilder, StringBuilder> {

  /**The indexes of the columns that store the point coordinates*/
  protected int[] coordCols;

  /**Field separator*/
  protected char fieldSeparator;

  public CSVPointEncoder(char fieldSeparator, int ... coordColumns) {
    this.coordCols = coordColumns;
    this.fieldSeparator = fieldSeparator;
  }

  @Override
  public StringBuilder apply(IFeature feature, StringBuilder str) {
    if (str == null)
      str = new StringBuilder();
    if (feature.getGeometry().isEmpty()) {
      for (int i = 1; i < this.coordCols.length; i++)
        str.append(fieldSeparator);
      return str;
    }
    if (feature.getGeometry() instanceof PointND) {
      PointND p = (PointND) feature.getGeometry();
      assert p.getCoordinateDimension() == this.coordCols.length;
      // Number of columns should replace the geometry field with the coordinates
      int numCols = coordCols.length + feature.length() - 1;
      int iCoord = 0; // The index of the coordinate to be written next
      int iAtt = 0; // The index of the next feature attribute to write
      for (int iCol = 0; iCol < numCols; iCol++) {
        if (iCol > 0)
          str.append(fieldSeparator);
        if (iCoord < coordCols.length && coordCols[iCoord] == iCol) {
          // Time to write the next coordinate
          str.append(p.getCoordinate(iCoord++));
        } else {
          // Time to write the next feature attribute
          CSVHelper.encodeValue(feature.get(++iAtt), str);
        }
      }
    } else if (feature.getGeometry() instanceof Point) {
      Point p = (Point) feature.getGeometry();
      CoordinateSequence cs = p.getCoordinateSequence();
      // Number of columns should replace the geometry field with the coordinates
      int numCols = coordCols.length + feature.length() - 1;
      int iCoord = 0; // The index of the coordinate to be written next
      int iAtt = 0; // The index of the next feature attribute to write
      for (int iCol = 0; iCol < numCols; iCol++) {
        if (iCol > 0)
          str.append(fieldSeparator);
        if (iCoord < coordCols.length && coordCols[iCoord] == iCol) {
          // Time to write the next coordinate
          str.append(cs.getOrdinate(0, iCoord++));
        } else {
          // Time to write the next feature attribute
          CSVHelper.encodeValue(feature.get(++iAtt), str);
        }
      }
    } else {
      throw new RuntimeException("Unsupported class type "+feature.getGeometry().getClass());
    }
    return str;
  }
}
