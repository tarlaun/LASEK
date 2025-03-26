package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.test.JavaSparkTest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class CompactLongArrayTest extends JavaSparkTest {

  public void testSimple() {
    long[] values = {1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    int i = 0;
    for (long x : longs) {
      assertEquals(values[i], x);
      i++;
    }
    assertEquals(values.length, i);
  }

  public void testDeltaZigzagEncoding() {
    long[] values = {3,4,4+127,2,1,5,3,1,0,4,5,3,4,5,6,3,5,3,5,3,5,4,3,2,5,2,5};
    for (int i = 0; i < values.length; i++)
      values[i] += 1000000000000L;
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    assertEquals(DeltaZigzagLongArray.class, longs.getClass());
    int i = 0;
    for (long x : longs) {
      assertEquals(values[i], x);
      i++;
    }
    assertEquals(values.length, i);
  }

  public void testWithOneValue() {
    long[] values = {1};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    int i = 0;
    for (long x : longs) {
      assertEquals(values[i], x);
      i++;
    }
    assertEquals(values.length, i);
  }


  public void testCompactEncoding() {
    long[] values = {50000, 50001, 50002, 50003};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    int i = 0;
    for (long x : longs) {
      assertEquals(values[i], x);
      i++;
    }
    assertEquals(values.length, i);
  }

  public void testCompactEncodingWithZero() {
    long[] values = {50000, 50000, 50000, 50000};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    assertEquals(BitCompactLongArray.class, longs.getClass());
    int i = 0;
    for (long x : longs)
      assertEquals(values[i++], x);
    assertEquals(values.length, i);
  }

  public void testNextNDelta() {
    long[] values = {1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    for (int size1 = 1; size1 < values.length - 1; size1++) {
      Iterator<Long> iter = longs.iterator();
      int size2 = values.length - size1;
      for (int i = 0; i < size1; i++) {
        assertTrue("Iterator should have a value", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      CompactLongArray subarray = ((CompactLongArray.CLLIterator)iter).next(size2);
      assertNotNull(subarray);
      assertFalse("Iterator should have no more values", iter.hasNext());
      iter = subarray.iterator();
      assertEquals(size2, subarray.size());
      for (int i = size1; i < values.length; i++) {
        assertTrue("Iterator should have values", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      assertFalse("Iterator should have no more values", iter.hasNext());
    }
  }

  public void testNextNDeltaDoubleCall() {
    long[] values = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    for (int size1 = 1; size1 < values.length - 1; size1++) {
      Iterator<Long> iter = longs.iterator();
      int size2 = values.length - size1;
      CompactLongArray subarray1 = ((CompactLongArray.CLLIterator)iter).next(size1);
      CompactLongArray subarray2 = ((CompactLongArray.CLLIterator)iter).next(size2);
      // Read first array
      iter = subarray1.iterator();
      for (int i = 0; i < size1; i++) {
        assertTrue("Iterator should have a value", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      assertFalse("Iterator should have no more values", iter.hasNext());
      // Read second array
      iter = subarray2.iterator();
      for (int i = size1; i < values.length; i++) {
        assertTrue("Iterator should have values", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      assertFalse("Iterator should have no more values", iter.hasNext());
    }
  }

  public void testSpecialCase() {
    long[] values = {
        6455, 6455, 6454, 6455, 6455,
        6455, 6075, 6076, 6075, 6075,
        6076, 6076, 6075, 6076, 6075,
        6075, 6076, 6077, 6077, 6078,
        6078, 6078
    };
    CompactLongArray array = CompactLongArray.constructLongArray(values);
    int i = 0;
    for (long v : array)
      assertEquals(values[i++], v);
  }

  public void testCompactCornerCase() {
    long[] values = new long[100];
    for (int i = 0; i < values.length; i+=2) {
      values[i] = 0;
      values[i+1] = 126;
    }
    values[values.length - 1] = 127;
    CompactLongArray array = CompactLongArray.constructLongArray(values);
    assertEquals(BitCompactLongArray.class, array.getClass());
    assertEquals(7, array.numBits);
    int i = 0;
    for (long l : array)
      assertEquals(values[i++], l);
  }

  public void testCompactMoreThan32Bits() {
    long[] values = new long[100];
    for (int i = 0; i < values.length; i+=2) {
      values[i] = (1L << 20) - 1;
      values[i+1] = (1L << 34) - 2;
    }
    values[values.length - 1] = (1L << 34) - 1;
    CompactLongArray array = CompactLongArray.constructLongArray(values);
    assertEquals(BitCompactLongArray.class, array.getClass());
    assertEquals(34, array.numBits);
    int i = 0;
    for (long l : array)
      assertEquals(values[i++], l);
  }

  public void testNextNCompactEncoding() {
    long[] values = {50000, 50001, 50002, 50003};
    CompactLongArray longs = CompactLongArray.constructLongArray(values);
    for (int size1 = 1; size1 < values.length - 1; size1++) {
      Iterator<Long> iter = longs.iterator();
      int size2 = values.length - size1;
      for (int i = 0; i < size1; i++) {
        assertTrue("Iterator should have a value", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      CompactLongArray subarray = ((CompactLongArray.CLLIterator)iter).next(size2);
      assertNotNull(subarray);
      assertFalse("Iterator should have no more values", iter.hasNext());
      iter = subarray.iterator();
      assertEquals(size2, subarray.size());
      for (int i = size1; i < values.length; i++) {
        assertTrue("Iterator should have values", iter.hasNext());
        assertEquals(values[i], (long) iter.next());
      }
      assertFalse("Iterator should have no more values", iter.hasNext());
    }
  }

  public void testRandomPartitioning() {
    if (shouldRunStressTest()) {
      for (int i = 0; i < 100; i++) {
        Random random = new Random(i);
        boolean sequential = random.nextBoolean();
        long[] input = new long[random.nextInt(100000) + 1];
        input[0] = random.nextInt();
        for (int j = 1; j < input.length; j++)
          input[j] = sequential? input[j-1]+1 : random.nextInt();

        CompactLongArray compactInput = CompactLongArray.constructLongArray(input);

        // Now, partition the data
        int[] partitions = new int[random.nextInt(30) + 2];
        for (int iPartition = 0; iPartition < partitions.length; iPartition++)
          partitions[iPartition] = random.nextInt(input.length);
        partitions[0] = 0;
        partitions[partitions.length - 1] = input.length;
        Arrays.sort(partitions);

        CompactLongArray.CLLIterator mainIterator = compactInput.iterator();
        int iPartition = 0;
        CompactLongArray subarray;
        Iterator<Long> subarrayIterator = null;
        for (int j = 0; j < input.length; j++) {
          while (j >= partitions[iPartition]) {
            assertTrue("Sub iterator cannot contain additional elements",
                subarrayIterator == null || !subarrayIterator.hasNext());
            subarray = mainIterator.next(partitions[iPartition+1] - partitions[iPartition]);
            if (subarray != null) {
              assertEquals("Should have enough elements for the subarray",
                  partitions[iPartition+1] - partitions[iPartition], subarray.size());
              subarrayIterator = subarray.iterator();
            }
            iPartition++;
          }
          assertEquals(input[j], (long) subarrayIterator.next());
        }
        assertFalse("Main iterator should be empty at the end", mainIterator.hasNext());
      }
    }
  }

  public void testRandomSubsets() {
    if (shouldRunStressTest()) {
      for (int i = 0; i < 1000; i++) {
        Random random = new Random(i);
        boolean sequential = random.nextBoolean();
        long[] input = new long[random.nextInt(100000) + 1];
        input[0] = random.nextInt();
        for (int j = 1; j < input.length; j++)
          input[j] = sequential ? input[j - 1] + 1 : random.nextInt();

        int start = random.nextInt(input.length / 2);
        int length = random.nextInt(input.length / 2);
        CompactLongArray compactInput = CompactLongArray.constructLongArray(input, start, start + length);

        int x = start;
        for (long l : compactInput) {
          assertEquals(input[x++], l);
        }
      }
    }
  }
}