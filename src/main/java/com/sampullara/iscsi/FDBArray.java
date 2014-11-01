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
import java.util.function.Consumer;

/**
 * Created by sam on 11/1/14.
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
   *  @param type
   * @param name
   */
  public FDBArray(String type, String name, int concurrency) {
    db = fdb.open();
    DirectoryLayer dl = DirectoryLayer.getDefault();
    ds = dl.createOrOpen(db, Arrays.asList(type, name)).get();
    semaphore = new Semaphore(concurrency);
  }

  public Future<Void> write(long offset, byte[] write) {
    return db.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        long firstBlock = offset / BLOCK_SIZE;
        int blockOffset = (int) (offset % BLOCK_SIZE);
        int shift = BLOCK_SIZE - blockOffset;
        int length = write.length;
        long lastBlock = (offset + length) / BLOCK_SIZE;
        // Special case first block and last block
        byte[] firstBlockKey = ds.get(firstBlock).pack();
        semaphore.acquireUninterruptibly();
        Future<Void> result = tx.get(firstBlockKey).flatMap(new Function<byte[], Future<Void>>() {
          @Override
          public Future<Void> apply(byte[] bytes) {
            if (bytes == null) {
              bytes = new byte[BLOCK_SIZE];
            }
            int writeLength = Math.min(write.length, BLOCK_SIZE - blockOffset);
            System.out.println("Writing to first block: " + blockOffset + " -> " + blockOffset + writeLength);
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
            int position = writeBlock * BLOCK_SIZE - shift;
            System.out.println("Writing to block: " + i + ", " + writeBlock + ", " + position);
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
                  int position = (int) ((lastBlock - firstBlock) * BLOCK_SIZE - shift);
                  System.out.println("Writing to block: " + lastBlock + ", " + position + ", " + (length - position));
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
    return db.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        long firstBlock = offset / BLOCK_SIZE;
        int blockOffset = (int) (offset % BLOCK_SIZE);
        int length = read.length;
        long lastBlock = (offset + length) / BLOCK_SIZE;
        tx.getRange(ds.get(firstBlock).pack(), ds.get(lastBlock + 1).pack()).forEach(new Consumer<KeyValue>() {
          @Override
          public void accept(KeyValue keyValue) {
            long blockId = ds.unpack(keyValue.getKey()).getLong(0);
            byte[] value = keyValue.getValue();
            int blockPosition = (int) ((blockId - firstBlock) * BLOCK_SIZE);
            int shift = BLOCK_SIZE - blockOffset;
            if (blockId == firstBlock) {
              System.arraycopy(value, blockOffset, read, 0, Math.min(shift, read.length));
            } else if (blockId == lastBlock) {
              System.arraycopy(value, 0, read, blockPosition + shift, read.length - blockPosition - shift);
            } else {
              System.arraycopy(value, 0, read, blockPosition + shift, BLOCK_SIZE);
            }
          }
        });
        return ReadyFuture.DONE;
      }
    });
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
