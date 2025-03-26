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
package edu.ucr.cs.bdlab.raptor;

import org.geotools.metadata.i18n.Errors;
import org.geotools.util.Utilities;

public final class GeoKeyEntry implements Comparable<GeoKeyEntry> {
  private int keyID;
  private int tiffTagLocation;
  private int count;
  private int valueOffset;

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof GeoKeyEntry)) {
      return false;
    } else {
      GeoKeyEntry that = (GeoKeyEntry)obj;
      return this.keyID == that.keyID && this.count == that.count && this.valueOffset == that.valueOffset && this.tiffTagLocation == that.tiffTagLocation;
    }
  }

  public int hashCode() {
    int hash = Utilities.hash(this.keyID, 1);
    hash = Utilities.hash(this.count, hash);
    hash = Utilities.hash(this.valueOffset, hash);
    hash = Utilities.hash(this.tiffTagLocation, hash);
    return hash;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("GeoKeyEntry (").append(this.count == 0 ? "VALUE" : "OFFSET").append("\n");
    builder.append("ID: ").append(this.keyID).append("\n");
    builder.append("COUNT: ").append(this.count).append("\n");
    builder.append("LOCATION: ").append(this.tiffTagLocation).append("\n");
    builder.append("VALUE_OFFSET: ").append(this.valueOffset).append("\n");
    return builder.toString();
  }

  public GeoKeyEntry(int keyID, int tagLoc, int count, int offset) {
    this.setKeyID(keyID);
    this.setCount(count);
    this.setTiffTagLocation(tagLoc);
    this.setValueOffset(offset);
  }

  private static void ensureNotNegative(String argument, int value) {
    if (value < 0) {
      throw new IllegalArgumentException(Errors.format(58, argument, value));
    }
  }

  public int getKeyID() {
    return this.keyID;
  }

  public int getTiffTagLocation() {
    return this.tiffTagLocation;
  }

  public int getCount() {
    return this.count;
  }

  public int getValueOffset() {
    return this.valueOffset;
  }

  public void setCount(int count) {
    ensureNotNegative("COUNT", count);
    this.count = count;
  }

  public void setKeyID(int keyID) {
    ensureNotNegative("ID", keyID);
    this.keyID = keyID;
  }

  public void setTiffTagLocation(int tagLoc) {
    ensureNotNegative("LOCATION", tagLoc);
    this.tiffTagLocation = tagLoc;
  }

  public void setValueOffset(int valueOffset) {
    ensureNotNegative("VALUE_OFFSET", valueOffset);
    this.valueOffset = valueOffset;
  }

  public int[] getValues() {
    return new int[]{this.keyID, this.tiffTagLocation, this.count, this.valueOffset};
  }

  public int compareTo(GeoKeyEntry o) {
    return this.keyID > o.keyID ? 1 : (this.keyID == o.keyID ? 0 : 1);
  }
}