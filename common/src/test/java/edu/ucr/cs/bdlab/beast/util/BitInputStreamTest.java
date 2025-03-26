package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.test.JavaSparkTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

public class BitInputStreamTest extends JavaSparkTest {

  public void testWriteReadBits() throws IOException {
    if (shouldRunStressTest()) {
      Random random = new Random(0);
      for (int iRound = 0; iRound < 10; iRound++) {
        int nBits = random.nextInt(10) + 6;
        int bitMask = ~(0xffffffff << nBits);
        int length = random.nextInt(1000) + 500;
        int[] data = new int[length];
        // Generate random data and write in bit format
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BitOutputStream bos = new BitOutputStream(baos);
        for (int j = 0; j < length; j++) {
          data[j] = random.nextInt() & bitMask;
          bos.write(data[j], nBits);
        }
        bos.close();
        // Now read it back
        BitInputStream bis = new BitInputStream(baos.toByteArray());
        for (int j = 0; j < length; j++) {
          assertEquals(String.format("Error in element #%d in round #%d", j, iRound),
              data[j], bis.nextShort(nBits));
        }
      }
    }
  }
}