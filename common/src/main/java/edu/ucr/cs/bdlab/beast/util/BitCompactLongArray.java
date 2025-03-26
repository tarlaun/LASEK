package edu.ucr.cs.bdlab.beast.util;

/**
 * A constant array of long integers stored in a compact and compressed way.
 */
public class BitCompactLongArray extends CompactLongArray {

  protected BitCompactLongArray(long[] values, int start, int end, int numBits, long sizeInBits) {
    int numEntries = (int)((sizeInBits + 63) / 64);
    this.data = new long[numEntries];
    this.numEntries = end - start;
    this.numBits = numBits;
    long marker = -1L >>> (64 - numBits);
    long offset = 0;
    for (int i = start; i < end; i++) {
      long value = values[i];
      if (numBits == 64 || value >= 0 && value < marker) {
        MathUtil.setBits(data, offset, numBits, value);
        offset += numBits;
      } else {
        MathUtil.setBits(data, offset, numBits, marker);
        offset += numBits;
        MathUtil.setBits(data, offset, 64, value);
        offset += 64;
      }
    }
    assert offset == sizeInBits : String.format("Unexpected size %d. Expected %d.", offset, sizeInBits);
  }

  @Override
  public CLLIterator iterator() {
    return new LongIteratorCompact();
  }

  class LongIteratorCompact implements CLLIterator {
    /**The index of the next value to return*/
    int i = 0;

    /**The bit position to read from*/
    long bitPosition = 0;

    /**The marker that indicates an invalid value*/
    long marker = -1L >>> (64 - numBits);

    @Override
    public boolean hasNext() {
      return i < numEntries;
    }

    @Override
    public Long next() {
      assert i < numEntries;
      i++;
      long value = numBits == 0 ? 0 : MathUtil.getBits(data, bitPosition, numBits);
      bitPosition += numBits;
      if (value == marker) {
        // Ignore the marker and read the next value
        value = MathUtil.getBits(data, bitPosition, 64);
        bitPosition += 64;
      }
      return value;
    }
  }
}
