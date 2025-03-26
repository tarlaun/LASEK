package edu.ucr.cs.bdlab.beast.util;

/**
 * A constant array of long integers stored using delta-encoding to reduce storage size.
 */
public class DeltaZigzagLongArray extends CompactLongArray {

  protected DeltaZigzagLongArray(long[] values, int start, int end, int numBits, long sizeInBits) {
    int numEntries = (int)((sizeInBits + 63) / 64);
    this.data = new long[numEntries];
    this.numEntries = end - start;
    this.numBits = numBits;
    long marker = 0xffffffffffffffffL >>> (64 - numBits);
    this.data[0] = values[start];
    long offset = 64;
    for (int i = start + 1; i < end; i++) {
      long delta = values[i] - values[i-1];
      long deltaZigzag = (delta >> 63) ^ (delta << 1);
      if (deltaZigzag >= 0 && deltaZigzag < marker) {
        MathUtil.setBits(data, offset, numBits, deltaZigzag);
        offset += numBits;
      } else {
        MathUtil.setBits(data, offset, numBits, marker);
        offset += numBits;
        MathUtil.setBits(data, offset, 64, values[i]);
        offset += 64;
      }
    }
    assert (offset - 1) / 64 == numEntries - 1 : "Unexpected size";
  }

  @Override
  public CLLIterator iterator() {
    return new LongIteratorDeltaZigzag();
  }

  class LongIteratorDeltaZigzag implements CLLIterator {
    /**The index of the next value to return*/
    int i = 0;

    /**The bit position to read from*/
    long bitPosition = 0;

    /**The value that was last returned*/
    long previousValue;

    /**The marker that indicates an invalid delta*/
    long marker = 0xffffffffffffffffL >>> (64 - numBits);

    @Override
    public boolean hasNext() {
      return i < numEntries;
    }

    @Override
    public Long next() {
      assert i < numEntries;
      long nextValue;
      if (i == 0) {
        nextValue = numBits == 0 ? 0 : MathUtil.getBits(data, bitPosition, 64);
        bitPosition += 64;
      } else {
        long deltaZigzag = numBits == 0 ? 0 : MathUtil.getBits(data, bitPosition, numBits);
        bitPosition += numBits;
        if (deltaZigzag < marker) {
          long delta = (deltaZigzag >>> 1) ^ -(deltaZigzag & 1);
          nextValue = previousValue + delta;
        } else {
          nextValue = numBits == 0 ? 0 : MathUtil.getBits(data, bitPosition, 64);
          bitPosition += 64;
        }
      }
      i++;
      previousValue = nextValue;
      return nextValue;
    }
  }
}
