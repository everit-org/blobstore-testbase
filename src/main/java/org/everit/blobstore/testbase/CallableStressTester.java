package org.everit.blobstore.testbase;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.NoSuchBlobException;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.junit.Assert;

import junit.framework.AssertionFailedError;

/**
 * Helper class to stress test blobstores and check consistency between multiple blobstores.
 */
public class CallableStressTester {

  public static class CallableStressTestConfiguration {
    public int averageBlobSize = 2048;

    public int averageReadSize = 512;

    public int createActionDivision = 5;

    public int deleteActionDivision = 5;

    public int initialBlobNum = 1000;

    public int iterationNum = 1000;

    public int readActionDivision = 80;

    public int threadNum = 4;

    public int updateActionDivision = 10;

  }

  protected static class PossibleConcurrentException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PossibleConcurrentException(final String message, final Throwable cause) {
      super(message, cause);
    }

  }

  private static final Logger LOGGER = Logger.getLogger(CallableStressTester.class.getName());

  public static void runStressTest(final CallableStressTestConfiguration config,
      final TransactionHelper transactionHelper, final Blobstore... blobstores) {
    Objects.nonNull(blobstores);
    Objects.nonNull(config);

    CallableStressTester tester = new CallableStressTester(config, transactionHelper, blobstores);
    tester.runTest();
  }

  protected final List<long[]> availableBlobs = new ArrayList<>();

  protected final ReadWriteLock availableBlobsRWLock = new ReentrantReadWriteLock(false);

  protected final int averageBlobSize;

  protected final int averageReadSize;

  protected final Blobstore[] blobstores;

  protected final int createActionTopIndex;

  protected final int deleteActionTopIndex;

  protected final int initialBlobNum;

  protected final int iterationNum;

  protected final int readActionTopIndex;

  protected final int threadNum;

  protected final TransactionHelper transactionHelper;

  protected final int updateActionTopIndex;

  protected CallableStressTester(final CallableStressTestConfiguration config,
      final TransactionHelper transactionHelper,
      final Blobstore[] blobstores) {
    this.blobstores = blobstores;
    this.averageBlobSize = config.averageBlobSize;

    this.createActionTopIndex = config.createActionDivision;
    this.readActionTopIndex = config.readActionDivision + this.createActionTopIndex;
    this.updateActionTopIndex = config.updateActionDivision + this.readActionTopIndex;
    this.deleteActionTopIndex = config.deleteActionDivision + this.updateActionTopIndex;

    this.initialBlobNum = config.initialBlobNum;
    this.threadNum = config.threadNum;
    this.iterationNum = config.iterationNum;
    this.averageReadSize = config.averageReadSize;
    this.transactionHelper = transactionHelper;
  }

  protected void addBlobIdsToAvailableBlobs(final long[] blobIds) {
    Lock writeLock = availableBlobsRWLock.writeLock();
    writeLock.lock();
    try {
      availableBlobs.add(blobIds);
    } finally {
      writeLock.unlock();
    }
  }

  protected void applyPositionParameters(final Random r, final AtomicInteger sampleStart,
      final AtomicInteger sampleSize,
      final BlobReader blobReader) {
    long size = blobReader.size();
    int amountToRead = randomAmountToRead(r, size);
    sampleSize.set(amountToRead);

    int startPosition = (int) (size - amountToRead);
    if (startPosition > 0) {
      startPosition = r.nextInt(startPosition);
    }
    sampleStart.set(startPosition);
  }

  protected void createBlob() {
    Random r = new Random();
    int blobSize = r.nextInt(averageBlobSize * 2);
    byte[] blobContent = createRandomContent(blobSize);
    transactionHelper.required(() -> {
      long[] blobIds = new long[blobstores.length];
      for (int i = 0; i < blobstores.length; i++) {
        Blobstore blobstore = blobstores[i];
        BlobAccessor blobAccessor = blobstore.createBlob();
        try {
          blobAccessor.write(blobContent, 0, blobSize);
          blobIds[i] = blobAccessor.getBlobId();
        } finally {
          blobAccessor.close();
        }
      }

      addBlobIdsToAvailableBlobs(blobIds);
      return null;
    });
  }

  protected void createInitialBlobs() {
    for (int i = 0; i < initialBlobNum; i++) {
      createBlob();
    }
  }

  protected byte[] createRandomContent(final int blobSize) {
    Random r = new Random();
    byte[] result = new byte[blobSize];
    r.nextBytes(result);
    return result;
  }

  protected void deleteBlob() {
    long[] blobIds = getBlobIdsForRandomBlob();
    if (blobIds == null) {
      return;
    }
    try {
      transactionHelper.required(() -> {
        for (int i = 0; i < blobstores.length; i++) {
          blobstores[i].deleteBlob(blobIds[i]);
        }

        Lock writeLock = availableBlobsRWLock.writeLock();
        writeLock.lock();
        try {
          availableBlobs.remove(blobIds);
        } finally {
          writeLock.unlock();
        }
        return null;
      });
    } catch (NoSuchBlobException e) {
      throw new PossibleConcurrentException("Blob was deleted before other delete", e);
    }
  }

  protected long[] getBlobIdsForRandomBlob() {
    Random r = new Random();
    Lock readLock = availableBlobsRWLock.readLock();
    readLock.lock();
    try {
      int size = availableBlobs.size();
      if (size == 0) {
        LOGGER.warning("Blobstore is exhausted");
        return null;
      }
      int randomIndex = r.nextInt(size);
      return availableBlobs.get(randomIndex);
    } finally {
      readLock.unlock();
    }
  }

  protected int randomAmountToRead(final Random r, final long blobSize) {
    int amountToRead = averageReadSize * 2;
    if (amountToRead > 0) {
      amountToRead = r.nextInt(amountToRead);
    }
    if (blobSize < amountToRead) {
      amountToRead = (int) blobSize;
    }
    return amountToRead;
  }

  protected void readBlob() {
    Random r = new Random();
    AtomicReference<byte[]> sampleContent = new AtomicReference<byte[]>(null);
    AtomicInteger sampleStart = new AtomicInteger();
    AtomicInteger sampleSize = new AtomicInteger();
    AtomicReference<Long> version = new AtomicReference<Long>(null);
    runTestActionOnWithRandomBlob(
        (blobstore, blobId) -> {
          try {
            BlobReader blobReader = blobstore.readBlob(blobId);

            if (version.get() == null) {
              version.set(blobReader.version());
            } else if (!version.get().equals(blobReader.version())) {
              throw new PossibleConcurrentException("Blob version conflict", null);
            }

            return blobReader;
          } catch (NoSuchBlobException e) {
            throw new PossibleConcurrentException("Blob has been deleted before reading", e);
          }
        } ,
        (blobstore, blobReader) -> {
          try {
            // if (version.get() == null) {
            // version.set(blobReader.version());
            // } else if (!version.get().equals(blobReader.version())) {
            // throw new IllegalStateException("Blob version conflict");
            // }

            if (sampleContent.get() == null) {
              applyPositionParameters(r, sampleStart, sampleSize, blobReader);
            }
            byte[] content = readContent(blobReader, sampleStart.get(), sampleSize.get());
            if (sampleContent.get() == null) {
              sampleContent.set(content);
            } else {
              try {
                Assert.assertArrayEquals(sampleContent.get(), content);
              } catch (AssertionError e) {
                LOGGER.log(Level.INFO, "Conflict in Blob: " + blobReader.getBlobId());
                throw e;
              }
            }
          } catch (ConcurrentModificationException e) {
            throw new PossibleConcurrentException(
                "Concurrent blob modification occured during read: " + e.getMessage(), e);
          } finally {
            blobReader.close();
          }
        });
  }

  protected byte[] readContent(final BlobReader blobReader, final int start, final int amount) {
    blobReader.seek(start);
    byte[] result = new byte[amount];
    int offset = 0;
    while (offset < amount) {
      int r = blobReader.read(result, offset, amount - offset);
      if (r < 0) {
        throw new AssertionFailedError("Blob ended undexpectedly");
      }
      offset += r;
    }
    return result;
  }

  protected void runTest() {
    long startTime = System.currentTimeMillis();
    createInitialBlobs();
    long initializationEndTime = System.currentTimeMillis();

    LOGGER.info("Test blob set creation took " + (initializationEndTime - startTime) + " ms.");

    CountDownLatch countDownLatch = new CountDownLatch(threadNum);
    for (int i = 0; i < threadNum; i++) {
      new Thread(() -> {
        try {
          runTestIterationsInThread();
        } finally {
          countDownLatch.countDown();
        }
      }).start();
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    long endTime = System.currentTimeMillis();
    long duration = endTime - initializationEndTime;
    int operationNum = threadNum * iterationNum;
    LOGGER.info(operationNum + " operation took " + duration + " ms. " + (operationNum / duration)
        + " operation / ms");

    LOGGER.info(availableBlobs.size() + " blobs in the end");
  }

  protected <R> void runTestActionOnWithRandomBlob(
      final BiFunction<Blobstore, Long, R> preNonConcurrentAction,
      final BiConsumer<Blobstore, R> action) {
    long[] blobIds = getBlobIdsForRandomBlob();

    if (blobIds == null) {
      return;
    }

    transactionHelper.required(() -> {
      Object[] results = new Object[blobstores.length];
      synchronized (blobIds) {
        for (int i = 0; i < blobstores.length; i++) {
          results[i] = preNonConcurrentAction.apply(blobstores[i], blobIds[i]);
        }
      }
      for (int i = 0; i < blobstores.length; i++) {
        @SuppressWarnings("unchecked")
        R result = (R) results[i];
        action.accept(blobstores[i], result);
      }
      return null;
    });
  }

  protected void runTestIterationsInThread() {
    Random r = new Random();
    for (int i = 0; i < iterationNum; i++) {
      int actionIndex = r.nextInt(deleteActionTopIndex);
      try {
        if (actionIndex < createActionTopIndex) {
          createBlob();
        } else if (actionIndex < readActionTopIndex) {
          readBlob();
        } else if (actionIndex < updateActionTopIndex) {
          updateBlob();
        } else {
          deleteBlob();
        }
      } catch (PossibleConcurrentException e) {
        LOGGER.warning(e.getMessage());
      }
    }
  }

  protected void updateBlob() {
    Random r = new Random();
    AtomicReference<byte[]> sampleContent = new AtomicReference<byte[]>(null);
    AtomicInteger sampleStart = new AtomicInteger();
    AtomicInteger sampleSize = new AtomicInteger();
    runTestActionOnWithRandomBlob(
        (blobstore, blobId) -> {
          try {
            return blobstore.updateBlob(blobId);
          } catch (NoSuchBlobException e) {
            throw new PossibleConcurrentException("Blob has been deleted during update", e);
          }
        } ,
        (blobstore, blobAccessor) -> {
          try {
            byte[] randomContent = sampleContent.get();
            if (randomContent == null) {
              applyPositionParameters(r, sampleStart, sampleSize, blobAccessor);
              randomContent = createRandomContent(sampleSize.get());
              sampleContent.set(randomContent);
            }
            blobAccessor.seek(sampleStart.get());
            blobAccessor.write(randomContent, 0, randomContent.length);
          } finally {
            blobAccessor.close();
          }
        });
  }
}
