package com.sampullara.iscsi;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by sam on 11/1/14.
 */
public class FDBArrayTest {
  @Test
  public void testReadWrite() {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 1);
    FDBArray fdbArray = new FDBArray("test", "testReadWrite", 10);
    fdbArray.clear();
    fdbArray.write(10000, bytes).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
  }
}
