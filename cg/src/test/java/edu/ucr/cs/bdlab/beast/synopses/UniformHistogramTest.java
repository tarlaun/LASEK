/*
 * Copyright 2018 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.synopses;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.test.JavaSparkTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class UniformHistogramTest extends JavaSparkTest {
  public void testWithMBR() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 10, 10);
    histogram.addPoint(new double[]{50.5, 100.5}, 10);
    histogram.addPoint(new double[]{149.5, 100.5}, 20);
    assertEquals(10, histogram.getValue(new int[]{0, 0}, new int[]{1, 1}));
    assertEquals(0, histogram.getValue(new int[]{0, 1}, new int[]{10, 9}));
    assertEquals(20, histogram.getValue(new int[]{5, 0}, new int[]{5, 1}));
  }

  public void testCellMBR() {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 10, 10);
    EnvelopeNDLite computedMBR = new EnvelopeNDLite(2);
    histogram.getCellEnvelope(new int[]{0, 0}, computedMBR);
    EnvelopeNDLite expectedMBR = new EnvelopeNDLite(2, 50.0, 100.0, 60.0, 110.0);
    assertEquals(expectedMBR, computedMBR);
  }

  public void testSumAlignedRectangle() {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 10, 10);
    histogram.addEntry(new int[]{0, 0}, 10);
    assertEquals(10, histogram.sumRectangle(50.0, 100.0, 80.0, 140.0));
    histogram.addEntry(new int[]{1, 1}, 15);
    assertEquals(25, histogram.sumRectangle(50.0, 100.0, 80.0, 140.0));
  }

  public void testCheckBounds() {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 10, 10);
    histogram.addEntry(new int[]{0, 0}, 10);
    histogram.addEntry(new int[]{1, 1}, 15);
    assertEquals(25, histogram.getValue(new int[]{-1, -1}, new int[]{100, 100}));
    assertEquals(25, histogram.sumRectangle(0.0, 0.0, 500.0, 500.0));
  }

  public void testMergeAligned() {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram1 = new UniformHistogram(mbr, 10, 10);
    UniformHistogram histogram2 = new UniformHistogram(mbr, 10, 10);
    histogram1.addEntry(new int[]{0, 0}, 10);
    histogram2.addEntry(new int[]{1, 1}, 15);
    histogram1.mergeAligned(histogram2);
    assertEquals(25, histogram1.getValue(new int[]{0, 0}, new int[]{10, 10}));
    assertEquals(15, histogram2.getValue(new int[]{0, 0}, new int[]{10, 10}));
  }

  public void testReadWrite() throws IOException, ClassNotFoundException {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, -180.0, -90.0, 180.0, 90.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 31, 16);
    histogram.values = new long[]{
        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 334, 4, 0, 1, 0, 0, 1, 0,
        0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1593, 14810, 1, 0, 0, 0, 0, 0, 591, 0, 0,
        0, 0, 0, 0, 0, 6, 5, 356, 1348, 0, 271, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5669, 10760, 50885, 2, 0, 0, 0, 68, 1951,
        663, 0, 0, 0, 0, 0, 0, 290, 5, 10, 563, 0, 0, 0, 0, 1, 0, 3, 1, 0, 0, 829, 197, 1050, 16495, 3769, 0, 1, 0, 11,
        69, 117, 16, 45, 0, 0, 0, 0, 7, 17, 1, 91, 11, 8, 0, 0, 0, 0, 1, 0, 0, 2, 1289, 173, 2654, 2296, 6483, 1, 1, 0,
        104, 0, 606, 0, 4, 8, 0, 9, 58941, 36690, 558, 143, 5, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 4197, 11809, 831, 320, 0,
        1, 25, 3320, 881, 1, 189, 4, 0, 126, 307, 753, 48051, 3114, 8288, 0, 0, 1, 0, 0, 185, 19, 0, 0, 1, 3571, 3494,
        897, 4664, 1272, 0, 0, 3, 347, 19, 100, 2, 2811, 1932, 38, 927, 668, 44, 21653, 31224, 2558, 65, 449, 0, 0, 0,
        0, 0, 1, 0, 1569, 1741, 11189, 14222, 84, 0, 0, 0, 14, 993, 1, 88, 185, 5372, 13514, 3055, 817, 929, 282, 33,
        644, 4894, 371, 1, 0, 1, 0, 0, 0, 0, 761, 4639, 1579, 12239, 44347, 27652, 170, 1, 0, 5, 10730, 31540, 6490,
        15663, 38329, 1480, 23, 349, 96, 7, 15, 176, 4875, 49101, 17, 0, 0, 0, 0, 1, 0, 1023, 467, 169, 1191, 406, 709,
        180, 0, 0, 0, 2840, 59881, 11874, 3249, 6320, 2450, 869, 369, 811, 433, 827, 43, 44, 240, 1, 12, 0, 0, 6, 81, 5,
        1, 30, 0, 1, 0, 4, 0, 1, 0, 51, 6, 1024, 1965, 2998, 1683, 629, 1393, 227, 236, 49, 11, 3, 566, 6, 10, 0, 1, 0,
        0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 2, 10, 68, 0, 0, 5, 0, 2, 13, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0};
    long totalSize = histogram.getValue(new int[] {0, 0}, new int[] {31, 16});
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream dos = new ObjectOutputStream(baos);
    dos.writeObject(histogram);
    dos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bais);
    UniformHistogram histogram2 = (UniformHistogram) in.readObject();
    assertEquals(31, histogram2.getNumPartitions(0));
    assertEquals(totalSize, histogram2.getValue(new int[] {0, 0}, new int[] {31, 16}));
  }

  public void testSkipOutOfBoundaryRecords() {
    EnvelopeNDLite mbr = new EnvelopeNDLite( 2, 50.0, 100.0, 150.0, 200.0);
    UniformHistogram histogram = new UniformHistogram(mbr, 10, 10);
    histogram.addEntry(new int[]{10, 10}, 10);
    assertEquals(0, histogram.sumRectangle(0.0, 0.0, 500.0, 500.0));
  }

  public void testComputeNumPartitions() {
    EnvelopeNDLite e = new EnvelopeNDLite(2, 0.0, 0.0, 4.0, 2.0);
    int[] numPartitions = UniformHistogram.computeNumPartitions(e, 8);
    assertEquals(2, numPartitions.length);
    assertEquals(4, numPartitions[0]);
    assertEquals(2, numPartitions[1]);

    e = new EnvelopeNDLite(3, 0.0, 0.0, 0.0, 2.0, 2.0, 2.0);
    numPartitions = UniformHistogram.computeNumPartitions(e, 8);
    assertEquals(3, numPartitions.length);
    assertEquals(2, numPartitions[0]);
    assertEquals(2, numPartitions[1]);
    assertEquals(2, numPartitions[2]);

    e = new EnvelopeNDLite(2, 0.0, 0.0, 1.99, 2.01);
    numPartitions = UniformHistogram.computeNumPartitions(e, 100);
    assertEquals(2, numPartitions.length);
    assertEquals(10, numPartitions[0]);
    assertEquals(10, numPartitions[1]);

    e = new EnvelopeNDLite(2, -180.0, -90.0, 180.0, 90.0);
    numPartitions = UniformHistogram.computeNumPartitions(e, 647);
    assertEquals(2, numPartitions.length);
    assertEquals(36, numPartitions[0]);
    assertEquals(17, numPartitions[1]);
  }

  public void testMergeNonAligned() {
    //start and end are same for both
    EnvelopeNDLite mbr = new EnvelopeNDLite(1, 0.0, 12.0);
    EnvelopeNDLite mbr1 = new EnvelopeNDLite(1, 0.0, 12.0);
    UniformHistogram histogram1 = new UniformHistogram(mbr, 4);
    UniformHistogram histogram2 = new UniformHistogram(mbr1, 3);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 15);
    histogram1.addEntry(new int[]{2}, 9);
    histogram1.addEntry(new int[]{3}, 30);
    UniformHistogram result = histogram2.mergeNonAligned(histogram1);
    assertSame("The target histogram (calle) should be the return value", result, histogram2);
    assertEquals(17, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(16, histogram2.getValue(new int[]{1}, new int[]{1}));

    //end is same. start of histogram2 is right of the start of histogram1
    mbr = new EnvelopeNDLite(1, 0.0, 12.0);
    mbr1 = new EnvelopeNDLite(1, 2.0, 12.0);
    histogram1 = new UniformHistogram(mbr, 4);
    histogram2 = new UniformHistogram(mbr1, 5);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 15);
    histogram1.addEntry(new int[]{2}, 9);
    histogram1.addEntry(new int[]{3}, 30);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(9, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(20, histogram2.getValue(new int[]{4}, new int[]{1}));

    //end is same. start of histogram1 is right of the start of histogram2
    mbr = new EnvelopeNDLite(1, 4.0, 12.0);
    mbr1 = new EnvelopeNDLite(1, 0.0, 12.0);
    histogram1 = new UniformHistogram(mbr, 4);
    histogram2 = new UniformHistogram(mbr1, 4);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 16);
    histogram1.addEntry(new int[]{2}, 10);
    histogram1.addEntry(new int[]{3}, 30);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(0, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(35, histogram2.getValue(new int[]{3}, new int[]{1}));

    //start is same. end of histogram 2 is left of the end of histogram1
    mbr = new EnvelopeNDLite(1, 0.0, 12.0);
    mbr1 = new EnvelopeNDLite(1, 0.0, 10.0);
    histogram1 = new UniformHistogram(mbr, 4);
    histogram2 = new UniformHistogram(mbr1, 5);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 15);
    histogram1.addEntry(new int[]{2}, 18);
    histogram1.addEntry(new int[]{3}, 30);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(8, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(16, histogram2.getValue(new int[]{4}, new int[]{1}));

    //start is same. end of histogram 1 is left of the end of histogram2
    mbr = new EnvelopeNDLite(1, 0.0, 10.0);
    mbr1 = new EnvelopeNDLite(1, 0.0, 12.0);
    histogram1 = new UniformHistogram(mbr, 5);
    histogram2 = new UniformHistogram(mbr1, 4);
    histogram1.addEntry(new int[]{0}, 20);
    histogram1.addEntry(new int[]{1}, 14);
    histogram1.addEntry(new int[]{2}, 10);
    histogram1.addEntry(new int[]{3}, 30);
    histogram1.addEntry(new int[]{3}, 50);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(27, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(0, histogram2.getValue(new int[]{3}, new int[]{1}));

    //start of histogram 2 is left of the start of histogram1,end of histogram 2 is left of the end of histogram1
    mbr = new EnvelopeNDLite(1, 4.0, 16.0);
    mbr1 = new EnvelopeNDLite(1, 0.0, 12.0);
    histogram1 = new UniformHistogram(mbr, 4);
    histogram2 = new UniformHistogram(mbr1, 4);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 18);
    histogram1.addEntry(new int[]{2}, 27);
    histogram1.addEntry(new int[]{3}, 30);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(0, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(24, histogram2.getValue(new int[]{3}, new int[]{1}));

    //start of histogram 2 is right of the start of histogram1,end of histogram 2 is right of the end of histogram1
    mbr = new EnvelopeNDLite(1, 0.0, 12.0);
    mbr1 = new EnvelopeNDLite(1, 4.0, 16.0);
    histogram1 = new UniformHistogram(mbr, 4);
    histogram2 = new UniformHistogram(mbr1, 4);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 18);
    histogram1.addEntry(new int[]{2}, 15);
    histogram1.addEntry(new int[]{3}, 30);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(17, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(0, histogram2.getValue(new int[]{3}, new int[]{1}));

    //start of histogram 2 is right of the start of histogram1,end of histogram 2 is left of the end of histogram1
    mbr = new EnvelopeNDLite(1, 0.0, 15.0);
    mbr1 = new EnvelopeNDLite(1, 4.0, 14.0);
    histogram1 = new UniformHistogram(mbr, 5);
    histogram2 = new UniformHistogram(mbr1, 5);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 18);
    histogram1.addEntry(new int[]{2}, 15);
    histogram1.addEntry(new int[]{3}, 30);
    histogram1.addEntry(new int[]{4}, 9);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(12, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(20, histogram2.getValue(new int[]{3}, new int[]{1}));

    //start of histogram 2 is left of the start of histogram1,end of histogram 2 is right of the end of histogram1
    mbr = new EnvelopeNDLite(1, 4.0, 14.0);
    mbr1 = new EnvelopeNDLite(1, 0.0, 15.0);
    histogram1 = new UniformHistogram(mbr, 5);
    histogram2 = new UniformHistogram(mbr1, 5);
    histogram1.addEntry(new int[]{0}, 12);
    histogram1.addEntry(new int[]{1}, 18);
    histogram1.addEntry(new int[]{2}, 4);
    histogram1.addEntry(new int[]{3}, 30);
    histogram1.addEntry(new int[]{4}, 16);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(0, histogram2.getValue(new int[]{0}, new int[]{1}));
    assertEquals(32, histogram2.getValue(new int[]{3}, new int[]{1}));

    //2-D cases
    //same end and start for  both dimensions
    mbr = new EnvelopeNDLite(2, 0.0, 0.0, 90.0, 90.0);
    mbr1 = new EnvelopeNDLite(2, 0.0, 0.0, 90.0, 90.0);
    histogram1 = new UniformHistogram(mbr, 3,3);
    histogram2 = new UniformHistogram(mbr1, 9,9);
    histogram1.addEntry(new int[]{0,0}, 36);
    histogram1.addEntry(new int[]{0,1}, 72);
    histogram1.addEntry(new int[]{0,2}, 108);
    histogram1.addEntry(new int[]{1,0}, 36);
    histogram1.addEntry(new int[]{1,1}, 72);
    histogram1.addEntry(new int[]{1,2}, 108);
    histogram1.addEntry(new int[]{2,0}, 36);
    histogram1.addEntry(new int[]{2,1}, 72);
    histogram1.addEntry(new int[]{2,2}, 108);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(4, histogram2.getValue(new int[]{0,0}, new int[]{1,1}));
    assertEquals(12, histogram2.getValue(new int[]{8,8}, new int[]{1,1}));

    //start and end of histogram 2 is to right of that of histogram 1 but they overlap
    mbr = new EnvelopeNDLite(2, 0.0, 0.0, 90.0, 90.0);
    mbr1 = new EnvelopeNDLite(2, 25.0, 25.0, 95.0, 95.0);
    histogram1 = new UniformHistogram(mbr, 3,3);
    histogram2 = new UniformHistogram(mbr1, 7,7);
    histogram1.addEntry(new int[]{0,0}, 36);
    histogram1.addEntry(new int[]{0,1}, 72);
    histogram1.addEntry(new int[]{0,2}, 108);
    histogram1.addEntry(new int[]{1,0}, 36);
    histogram1.addEntry(new int[]{1,1}, 72);
    histogram1.addEntry(new int[]{1,2}, 108);
    histogram1.addEntry(new int[]{2,0}, 36);
    histogram1.addEntry(new int[]{2,1}, 72);
    histogram1.addEntry(new int[]{2,2}, 108);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(6, histogram2.getValue(new int[]{0,0}, new int[]{1,1}));
    assertEquals(10, histogram2.getValue(new int[]{0,3}, new int[]{1,1}));

    //histogram 2 lies inside histogram 1
    mbr = new EnvelopeNDLite(2, 0.0, 0.0, 80.0, 80.0);
    mbr1 = new EnvelopeNDLite(2, 25.0, 25.0, 45.0, 45.0);
    histogram1 = new UniformHistogram(mbr, 4,4);
    histogram2 = new UniformHistogram(mbr1, 4,4);
    histogram1.addEntry(new int[]{0,0}, 64);
    histogram1.addEntry(new int[]{0,1}, 128);
    histogram1.addEntry(new int[]{0,2}, 192);
    histogram1.addEntry(new int[]{0,3}, 256);
    histogram1.addEntry(new int[]{1,0}, 64);
    histogram1.addEntry(new int[]{1,1}, 128);
    histogram1.addEntry(new int[]{1,2}, 192);
    histogram1.addEntry(new int[]{1,3}, 256);
    histogram1.addEntry(new int[]{2,0}, 64);
    histogram1.addEntry(new int[]{2,1}, 128);
    histogram1.addEntry(new int[]{2,2}, 192);
    histogram1.addEntry(new int[]{2,3}, 256);
    histogram1.addEntry(new int[]{3,0}, 64);
    histogram1.addEntry(new int[]{3,1}, 128);
    histogram1.addEntry(new int[]{3,2}, 192);
    histogram1.addEntry(new int[]{3,3}, 256);
    histogram2.mergeNonAligned(histogram1);
    assertEquals(12, histogram2.getValue(new int[]{3,3}, new int[]{1,1}));
    assertEquals(8, histogram2.getValue(new int[]{1,0}, new int[]{1,1}));


  }

}