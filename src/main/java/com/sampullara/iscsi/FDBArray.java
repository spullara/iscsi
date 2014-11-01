package com.sampullara.iscsi;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;

import java.util.Arrays;
import java.util.concurrent.Semaphore;

/**
 * Block storage in FDB
 */
public class FDBArray {

  private static final FDB fdb = FDB.selectAPIVersion(200);

  // Ideal size for FDB
  private final int BLOCK_SIZE = 512;

  // Location in the database
  private final DirectorySubspace ds;
  private final Database db;
  private final Semaphore semaphore;

  /**
   * Arrays are 0 indexed byte arrays using valueSize bytes per value.
   *
   * @param type
   * @param name
   */
  public FDBArray(String type, String name, int concurrency) {
    db = fdb.open();
    ds = DirectoryLayer.getDefault().createOrOpen(db, Arrays.asList(type, name)).get();
    semaphore = new Semaphore(concurrency);
  }

  public Future<Void> write(long offset, byte[] write) {
    return db.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        long firstBlock = offset / BLOCK_SIZE;
        int length = write.length;
        int blockOffset = (int) (offset % BLOCK_SIZE);
        long lastBlock = (offset + length) / BLOCK_SIZE;
        int shift = BLOCK_SIZE - blockOffset;
        // Special case first block and last block
        byte[] firstBlockKey = ds.get(firstBlock).pack();
        semaphore.acquireUninterruptibly();
        Future<Void> result = tx.get(firstBlockKey).flatMap(new Function<byte[], Future<Void>>() {
          @Override
          public Future<Void> apply(byte[] bytes) {
            if (bytes == null) {
              bytes = new byte[BLOCK_SIZE];
            }
            int writeLength = Math.min(length, shift);
            System.arraycopy(write, 0, bytes, blockOffset, writeLength);
            tx.set(firstBlockKey, bytes);
            return ReadyFuture.DONE;
          }
        });
        if (lastBlock > firstBlock) {
          // For the blocks in the middle we can just blast values in without looking at the current bytes
          byte[] bytes = new byte[BLOCK_SIZE];
          for (long i = firstBlock + 1; i < lastBlock; i++) {
            byte[] key = ds.get(i).pack();
            int writeBlock = (int) (i - firstBlock);
            int position = (writeBlock - 1) * BLOCK_SIZE + shift;
            System.arraycopy(write, position, bytes, 0, BLOCK_SIZE);
            tx.set(key, bytes);
          }
          byte[] lastBlockKey = ds.get(lastBlock).pack();
          result = result.flatMap(new Function<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void aVoid) {
              return tx.get(lastBlockKey).flatMap(new Function<byte[], Future<Void>>() {
                @Override
                public Future<Void> apply(byte[] bytes) {
                  if (bytes == null) {
                    bytes = new byte[BLOCK_SIZE];
                  }
                  int position = (int) ((lastBlock - firstBlock - 1) * BLOCK_SIZE + shift);
                  System.arraycopy(write, position, bytes, 0, length - position);
                  tx.set(lastBlockKey, bytes);
                  return ReadyFuture.DONE;
                }
              });
            }
          });
        }
        result.onReady(semaphore::release);
        return result;
      }
    });
  }

  public Future<Void> read(byte[] read, long offset) {
    semaphore.acquireUninterruptibly();
    Future<Void> result = db.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        long firstBlock = offset / BLOCK_SIZE;
        int blockOffset = (int) (offset % BLOCK_SIZE);
        int length = read.length;
        long lastBlock = (offset + length) / BLOCK_SIZE;
        for (KeyValue keyValue : tx.getRange(ds.get(firstBlock).pack(), ds.get(lastBlock + 1).pack())) {
          long blockId = ds.unpack(keyValue.getKey()).getLong(0);
          byte[] value = keyValue.getValue();
          int blockPosition = (int) ((blockId - firstBlock) * BLOCK_SIZE);
          int shift = BLOCK_SIZE - blockOffset;
          if (blockId == firstBlock) {
            int firstBlockLength = Math.min(shift, read.length);
            System.arraycopy(value, blockOffset, read, 0, firstBlockLength);
          } else {
            int position = blockPosition - BLOCK_SIZE + shift;
            if (blockId == lastBlock) {
              int lastLength = read.length - position;
              System.arraycopy(value, 0, read, position, lastLength);
            } else {
              System.arraycopy(value, 0, read, position, BLOCK_SIZE);
            }
          }
        }
        return ReadyFuture.DONE;
      }
    });
    result.onReady(semaphore::release);
    return result;
  }

  public void clear() {
    db.run(new Function<Transaction, Void>() {
      @Override
      public Void apply(Transaction tx) {
        tx.clear(ds.pack());
        return null;
      }
    });
  }
}
