package com.sampullara.iscsi;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by sam on 11/1/14.
 */
public class FDBArrayTest {
  @Test
  public void testSimpleReadWrite() {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 1);
    FDBArray fdbArray = new FDBArray("test", "testReadWrite", 10);
    fdbArray.clear();
    fdbArray.write(10000, bytes).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
    fdbArray.clear();
  }

  @Test
  public void testRandomReadWrite() {
    FDBArray fdbArray = new FDBArray("test", "testReadWrite", 10);
    fdbArray.clear();
    Random r = new Random(1337);
    for (int i = 0; i < 1000; i++) {
      int length = r.nextInt(10000);
      byte[] bytes = new byte[length];
      r.nextBytes(bytes);
      int offset = r.nextInt(10000);
      fdbArray.write(offset, bytes).get();
      byte[] read = new byte[length];
      fdbArray.read(read, offset).get();
      assertArrayEquals(bytes, read);
    }
    fdbArray.clear();
  }

}
