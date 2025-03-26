package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.locationtech.jts.geom.Geometry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;

public class CSVWKTEncoder implements BiFunction<IFeature, StringBuilder, StringBuilder>, Externalizable {

  /**The index of the field that will contains the WKT-encoded geometry*/
  protected int wktCol;

  /**Field separator*/
  protected char fieldSeparator;

  public CSVWKTEncoder() {
    // Default constructor for the Externalizable and Writable interfaces
  }

  public CSVWKTEncoder(char fieldSeparator, int wktColumn) {
    this.wktCol = wktColumn;
    this.fieldSeparator = fieldSeparator;
  }

  @Override
  public StringBuilder apply(IFeature feature, StringBuilder str) {
    if (str == null)
      str = new StringBuilder();
    Geometry g = feature.getGeometry();
    int numCols = feature.length();
    int iAtt = 0; // The index of the next feature attribute to write
    for (int iCol = 0; iCol < numCols; iCol++) {
      if (iCol > 0)
        str.append(fieldSeparator);
      if (iCol == this.wktCol) {
        // Time to write the next coordinate
        String wkt = g.toText();
        if (wkt.indexOf(fieldSeparator) != -1) {
          // Need to escape the WKT representation
          wkt = '"' + wkt + '"';
        }
        str.append(wkt);
      } else {
        if (iAtt == feature.iGeom())
          iAtt++;
        // Time to write the next feature attribute
        CSVHelper.encodeValue(feature.get(iAtt++), str);
      }
    }
    return str;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(wktCol);
    out.writeChar(fieldSeparator);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    this.wktCol = in.readInt();
    this.fieldSeparator = in.readChar();
  }
}
