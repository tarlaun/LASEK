package edu.ucr.cs.bdlab.beast.util;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A constant array of long integers stored in a compact and compressed way.
 */
public abstract class CompactLongArray implements Iterable<Long>, Serializable {
  /**The data of all longs*/
  protected long[] data;

  /**Number of elements*/
  protected int numEntries;

  /**
   * If Compact encoding, this is the number of bits to store each value.
   * If Delta encoding, this is the number of bits for the delta part.
   */
  protected int numBits;

  /**
   * Encoding type used to encode the values.
   * <ul>
   *   <li>Compact: values are stored as-is but they are bit-packed to remove unused bits.</li>
   *   <li>Delta: First, a delta is applied for each two consecutive values and then bit packed.</li>
   *   <li>DeltaZigzag: A delta and then zigzag are applied for each two consecutive values and then bit packed.</li>
   * </ul>
   */
  enum Encoding {Compact, Delta, DeltaZigzag}

  /**
   * Constructs a new LongArray for the given list of values.
   * This method will automatically choose the best implementation based on the given values so as to
   * minimize the total memory overhead.
   * @param values the list of values to test.
   */
  public static CompactLongArray constructLongArray(long[] values) {
    return constructLongArray(values, 0, values.length);
  }

  /**
   * Constructs a new LongArray for the given list of values.
   * This method will automatically choose the best implementation based on the given values so as to
   * minimize the total memory overhead.
   * @param values the list of values to read from
   * @param start the first value to consider
   * @param end the position after the last value to consider
   * @return the constructed compact long array
   */
  public static CompactLongArray constructLongArray(long[] values, int start, int end) {
    // Handle special cases
    int length = end - start;
    if (length <= 1)
      return new BitCompactLongArray(values, start, end, 64, 64L * length);
    // Number of values that need at least this number of bits to be stored with each encoding
    long[] countsCompact = new long[65];
    long[] countsDelta = new long[65];
    long[] countsDeltaZigzag = new long[65];

    if (values[start] == -1)
      countsCompact[64]++;
    else
      countsCompact[numberOfSignificantBits(values[start] + 1)]++;
    countsDelta[64]++;
    countsDeltaZigzag[64]++;
    for (int i = start + 1; i < end; i++) {
      // The +1 is to account for the marker that cannot be used
      int numBitsCompact = values[i] == -1 ? 64 : numberOfSignificantBits(values[i] + 1);
      countsCompact[numBitsCompact]++;

      long delta = values[i] - values[i-1];
      int numBitsDelta = delta == -1? 64 : numberOfSignificantBits(delta + 1);
      countsDelta[numBitsDelta]++;

      long deltaZigzag = (delta >> 63) ^ (delta << 1);
      int numBitsDeltaZigzag = deltaZigzag == -1? 64 : numberOfSignificantBits(deltaZigzag + 1);
      countsDeltaZigzag[numBitsDeltaZigzag]++;
    }

    // Now, count the best number of bits to use with each technique
    // Compute suffix sums since we will need all values greater than or less than a specific num of bits
    for (int numBits = 63; numBits >= 0; numBits--) {
      countsCompact[numBits] += countsCompact[numBits + 1];
      countsDelta[numBits] += countsDelta[numBits + 1];
      countsDeltaZigzag[numBits] += countsDeltaZigzag[numBits + 1];
    }

    long minSize = 64L * length;
    int minNumBits = 64;
    Encoding minEncoding = Encoding.Compact;
    for (int numBits = 0; numBits < 64; numBits++) {
      // For all techniques, a value is stored in one of two ways.
      // 1- If it has numBits or less, it will be stored in numBits
      // 2- If it has more than numBits, it will be stored as a special marker in numBits + the full 64-bit value
      long sizeCompact = length * (long) numBits + countsCompact[numBits + 1] * 64;
      if (sizeCompact < minSize) {
        minSize = sizeCompact;
        minNumBits = numBits;
        minEncoding = Encoding.Compact;
      }
      long sizeDelta = (length - 1) * (long) numBits + countsDelta[numBits + 1] * 64;
      if (sizeDelta < minSize) {
        minSize = sizeDelta;
        minNumBits = numBits;
        minEncoding = Encoding.Delta;
      }
      long sizeDeltaZigzag = (length - 1) * (long) numBits + countsDeltaZigzag[numBits + 1] * 64;
      if (sizeDeltaZigzag < minSize) {
        minSize = sizeDeltaZigzag;
        minNumBits = numBits;
        minEncoding = Encoding.DeltaZigzag;
      }
    }

    switch (minEncoding) {
      case Compact: return new BitCompactLongArray(values, start, end, minNumBits, minSize);
      case Delta: return new DeltaLongArray(values, start, end, minNumBits, minSize);
      case DeltaZigzag: return new DeltaZigzagLongArray(values, start, end, minNumBits, minSize);
      default: throw new RuntimeException("Unrecognized encoding type "+minEncoding);
    }
  }

  public static CompactLongArray constructLongArray(int[] values, int start, int end) {
    // TODO find a way to do that without constructing a separate array
    long[] lvalues = new long[end - start];
    for (int i = start; i < end; i++)
      lvalues[i - start] = values[i];
    return constructLongArray(lvalues);
  }

  public static CompactLongArray constructLongArray(int[] values) {
    return constructLongArray(values, 0, values.length);
  }

  /**
   * The total number of elements stored in this LongArray
   * @return the total size in terms of number of elements
   */
  public int size() {
    return numEntries;
  }

  /**
   * Whether this list is empty or not
   * @return {@code true} if the list has no elements
   */
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public abstract CLLIterator iterator();

  @Override
  public String toString() {
    StringBuffer str = new StringBuffer();
    str.append('[');
    boolean first = true;
    for (long l : this) {
      if (first)
        first = false;
      else
        str.append(',');
      str.append(l);
    }
    str.append(']');
    return str.toString();
  }

  /**
   * Creates and returns a CompactLongArray that repeats the given value for the given number of elements.
   * @param value the value to repeat
   * @param size the number of repetitions
   * @return an array of the given size and contains the given value at all positions
   */
  public static CompactLongArray fill(long value, int size) {
    // Return delta-encoded array with numBits set to zero
    CompactLongArray l = new DeltaLongArray();
    l.numEntries = size;
    l.numBits = 0;
    l.data = new long[] {value};
    return l;
  }

  public interface CLLIterator extends Iterator<Long> {
    /***
     * Return the next n values as another CompactLongArray. If less than n entries are remaining, all remaining
     * values will be consumed and returned. Callers can use {@link CompactLongArray#size()} to verify.
     * @param n the number of values to return
     * @return a new CompactLongArray with the remaining values. If {@code n=0}, {@code null} is returned.
     */
    default CompactLongArray next(int n) {
      long[] values = new long[n];
      for (int i = 0; i < n; i++)
        values[i] = next();
      return CompactLongArray.constructLongArray(values);
    }
  }

  /**
   * Computes the number of significant bits in the given 64-bit long. This also represents the minimum number of bits
   * needed to represent this number.
   * Special case, if {@code x} is 0, the return value is 0.
   * @param x any integer
   * @return number of significant bits
   */
  public static int numberOfSignificantBits(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }

}
