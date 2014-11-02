package com.sampullara.iscsi;

import com.foundationdb.async.Future;
import org.jscsi.target.storage.IStorageModule;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

public class FDBStorage implements IStorageModule {

  private static final int BLOCKS = 1_000_000_000;
  private final FDBArray fa;
  private LongAdder writesInput = new LongAdder();
  private LongAdder writesComplete = new LongAdder();
  private LongAdder bytesWritten = new LongAdder();
  private LongAdder bytesRead = new LongAdder();
  private volatile boolean closed = false;

  public FDBStorage() {
    fa = new FDBArray("iscsi", "test", 100);
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    if (logicalBlockAddress > BLOCKS) {
      System.out.println("Bound failure: " + logicalBlockAddress);
//      return 1;
    }
    if (logicalBlockAddress + transferLengthInBlocks - 1> BLOCKS || transferLengthInBlocks < 0) {
      System.out.println("Bound failure: " + logicalBlockAddress + ", " + transferLengthInBlocks);
//      return 2;
    }
    return 0;
  }

  @Override
  public long getSizeInBlocks() {
    return BLOCKS;
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    if (closed) throw new IOException("Closed");
    bytesRead.add(bytes.length);
    System.out.println("Reading: " + bytes.length + ", " + writesInput + ", " + writesComplete + ", " + bytesRead);
    flush();
    fa.read(bytes, storageIndex).get();
  }

  private void flush() {
    synchronized (this) {
      long start = System.currentTimeMillis();
      long waitFor = writesInput.longValue();
      while (waitFor > writesComplete.longValue()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          // Ignore and spin
        }
      }
      long diff = System.currentTimeMillis() - start;
      if (diff > 10) {
        System.out.println("Flush paused for " + diff + "ms");
      }
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    if (closed) throw new IOException("Closed");
    writesInput.increment();
    Future<Void> write = fa.write(storageIndex, bytes);
    write.onReady(() -> {
      writesComplete.increment();
      bytesWritten.add(bytes.length);
      if (writesComplete.longValue() == writesInput.longValue()) {
        System.out.println("Writes complete: " + writesComplete + ", "  + bytesWritten);
      }
      synchronized (this) {
        this.notifyAll();
      }
    });
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      flush();
    }
  }
}
