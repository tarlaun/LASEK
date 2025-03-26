package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.util.BitArray;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class BitArrayTest extends TestCase {

  public void testReadWriteMinimal() throws IOException {
    Random r = new Random(0);
    for (int i = 0; i < 100; i++) {
      int size = r.nextInt(50) + 5;
      BitArray b = new BitArray(size);
      int pos1 = r.nextInt(size);
      int pos2 = r.nextInt(size);
      int pos3 = r.nextInt(size);
      b.set(pos1, true);
      b.set(pos2, true);
      b.set(pos3, true);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      b.writeBitsMinimal(dos);
      dos.close();

      BitArray b2 = new BitArray(size);
      b2.readBitsMinimal(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
      assertTrue(b2.get(pos1));
      assertTrue(b2.get(pos2));
      assertTrue(b2.get(pos3));
    }
  }

  public void testReadWriteBitsMinimal() throws IOException {
    BitArray bitArray = new BitArray(46);
    bitArray.entries[0] = 70361757896511L;
    assertFalse(bitArray.get(17));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    bitArray.writeBitsMinimal(dos);
    dos.close();

    bitArray = new BitArray(46);
    bitArray.readBitsMinimal(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
    assertFalse(bitArray.get(17));
  }
}