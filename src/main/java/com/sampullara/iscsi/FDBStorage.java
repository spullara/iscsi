package com.sampullara.iscsi;

import com.foundationdb.Database;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import org.jscsi.target.storage.IStorageModule;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static java.util.Arrays.asList;

public class FDBStorage implements IStorageModule {

  private static final Logger log = Logger.getLogger("FDBStorage");

  private static final int BLOCKS = 1_000_000_000;
  private final FDBArray fa;
  private LongAdder writesInput = new LongAdder();
  private LongAdder writesComplete = new LongAdder();
  private LongAdder bytesWritten = new LongAdder();
  private LongAdder bytesRead = new LongAdder();
  private volatile boolean closed = false;

  public FDBStorage(Database db, String name) {
    DirectorySubspace ds = DirectoryLayer.getDefault().createOrOpen(db, asList("com.sampullara.fdb.storage", name)).get();
    fa = new FDBArray(db, ds, 512);
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    if (logicalBlockAddress > BLOCKS) {
      log.warning("Bound failure: " + logicalBlockAddress);
      return 1;
    }
    if (logicalBlockAddress + transferLengthInBlocks - 1 > BLOCKS || transferLengthInBlocks < 0) {
      log.warning("Bound failure: " + logicalBlockAddress + ", " + transferLengthInBlocks);
      return 2;
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
    log.info("Reading: " + bytes.length + ", " + writesInput + ", " + writesComplete + ", " + bytesRead);
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
        log.warning("Flush paused for " + diff + "ms");
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
        log.info("Writes complete: " + writesComplete + ", "  + bytesWritten);
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
