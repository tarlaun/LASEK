package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.util.CompactLongArray;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Stores {@link Intersections} using {@link edu.ucr.cs.bdlab.beast.util.CompactLongArray} to save memory.
 */
public class CompactIntersections implements Iterable<CompactIntersections.Intersection>, Serializable {

  static class Intersection {
    int y;
    int x1, x2;
    long featureID;
    int tileID;
  }

  /**The list of x ranges in each intersection, [x1, x2] pairs*/
  protected CompactLongArray xs;
  /**The list of y intersections*/
  protected CompactLongArray ys;
  /**The list of geometry indexes in the intersections*/
  protected CompactLongArray geometryIndexes;
  /**The list of (metadataID, tileID) pairs*/
  protected CompactLongArray tileID;
  /**The list of feature IDs. This array is randomly accessed so we do not use CompactLongArray with it*/
  protected long[] featureIDs;

  CompactIntersections(CompactLongArray tileIDs, CompactLongArray ys, CompactLongArray xs, CompactLongArray geometryIndexes,
                       long[] featureIDs) {
    this.tileID = tileIDs;
    this.xs = xs;
    this.ys = ys;
    this.geometryIndexes = geometryIndexes;
    this.featureIDs = featureIDs;
  }

  public int getNumIntersections() {
    return ys.size();
  }

  @Override
  public Iterator<Intersection> iterator() {
    return new Iter();
  }

  /**
   * Iterates over all intersections.
   */
  class Iter implements Iterator<Intersection> {

    final Intersection intersection = new Intersection();

    final Iterator<Long> xIter = xs.iterator();
    final Iterator<Long> yIter = ys.iterator();
    final Iterator<Long> polygonIter = geometryIndexes.iterator();
    final Iterator<Long> tileIDIter = tileID.iterator();

    @Override
    public boolean hasNext() {
      return yIter.hasNext();
    }

    @Override
    public Intersection next() {
      if (!hasNext())
        return null;
      intersection.x1 = (int)(long) xIter.next();
      intersection.x2 = (int)(long) xIter.next();
      intersection.y = (int)(long) yIter.next();
      intersection.featureID = featureIDs[(int) (long) polygonIter.next()];
      intersection.tileID = (int)(long) tileIDIter.next();
      return intersection;
    }
  }

}
