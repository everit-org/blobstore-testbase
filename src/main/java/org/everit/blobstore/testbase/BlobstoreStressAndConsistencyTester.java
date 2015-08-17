/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.blobstore.testbase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.everit.blobstore.BlobAccessor;
import org.everit.blobstore.BlobReader;
import org.everit.blobstore.Blobstore;
import org.everit.blobstore.NoSuchBlobException;
import org.everit.transaction.propagator.TransactionPropagator;
import org.junit.Assert;

/**
 * Helper class to stress test blobstores and check consistency between multiple blobstores.
 */
public class BlobstoreStressAndConsistencyTester {

  /**
   * Configuration to run stress tests on blobstores.
   */
  public static class BlobstoreStressTestConfiguration {

    public static final int DEFAULT_AVERAGE_INITIAL_BLOB_SIZE = 2048;

    public static final int DEFAULT_AVERAGE_READ_AMOUNT = 1536;

    public static final int DEFAULT_AVERAGE_READ_BUFFER_SIZE = 50;

    public static final int DEFAULT_AVERAGE_UPDATE_AMOUNT = 1536;

    public static final int DEFAULT_CONTINUOUS_UPDATING_CHANCE_PERCENTAGE = 50;

    public static final int DEFAULT_CREATE_ACTION_CHANCE_PART = 5;

    public static final int DEFAULT_DELETE_ACTION_CHANCE_PART = 5;

    public static final int DEFAULT_INITIAL_BLOB_NUM = 1000;

    public static final int DEFAULT_ITERTAION_NUM_PER_THREAD = 500;

    public static final int DEFAULT_READ_ACTION_CHANCE_PART = 80;

    public static final int DEFAULT_THREAD_NUM = 4;

    public static final int DEFAULT_UPDATE_ACTION_CHANCE_PART = 10;

    /**
     * The average size of newly created blobs.
     */
    public int averageInitialBlobSize = DEFAULT_AVERAGE_INITIAL_BLOB_SIZE;

    /**
     * The average amount to read from blobs.
     */
    public int averageReadAmount = DEFAULT_AVERAGE_READ_AMOUNT;

    /**
     * The average size of the buffer of reading actions.
     */
    public int averageReadBufferSize = DEFAULT_AVERAGE_READ_BUFFER_SIZE;

    public int averageUpdateAmount = DEFAULT_AVERAGE_UPDATE_AMOUNT;

    /**
     * The average size of the buffer of updating actions.
     */
    public int averageUpdateBufferSize = DEFAULT_CONTINUOUS_UPDATING_CHANCE_PERCENTAGE;

    /**
     * The chance to run create blob action. The values of {@link #createActionChancePart},
     * {@link #readActionChancePart}, {@link #updateActionChancePart} and
     * {@link #deleteActionChancePart} are relative to each other.
     */
    public int createActionChancePart = DEFAULT_CREATE_ACTION_CHANCE_PART;

    /**
     * The chance to run delete blob action. The values of {@link #createActionChancePart},
     * {@link #readActionChancePart}, {@link #updateActionChancePart} and
     * {@link #deleteActionChancePart} are relative to each other.
     */
    public int deleteActionChancePart = DEFAULT_DELETE_ACTION_CHANCE_PART;

    /**
     * The initial number of blobs to start the tests with.
     */
    public int initialBlobNum = DEFAULT_INITIAL_BLOB_NUM;

    /**
     * The number of iterations per threads.
     */
    public int iterationNumPerThread = DEFAULT_ITERTAION_NUM_PER_THREAD;

    /**
     * The chance to run read blob action. The values of {@link #createActionChancePart},
     * {@link #readActionChancePart}, {@link #updateActionChancePart} and
     * {@link #deleteActionChancePart} are relative to each other.
     */
    public int readActionChancePart = DEFAULT_READ_ACTION_CHANCE_PART;

    /**
     * The number of threads to run the tests on.
     */
    public int threadNum = DEFAULT_THREAD_NUM;

    /**
     * The chance to run update blob action. The values of {@link #createActionChancePart},
     * {@link #readActionChancePart}, {@link #updateActionChancePart} and
     * {@link #deleteActionChancePart} are relative to each other.
     */
    public int updateActionChancePart = DEFAULT_UPDATE_ACTION_CHANCE_PART;
  }

  /**
   * Simple holder class to pass integer values back from Lambda expressions.
   */
  protected static class IntegerHolder {
    public int value = 0;
  }

  /**
   * Internal exception class to communicate that an exception occured that can come from concurrent
   * read and write accesses of the same blob.
   *
   */
  protected static class PossibleConcurrentException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PossibleConcurrentException(final String message, final Throwable cause) {
      super(message, cause);
    }

  }

  /**
   * Simple holder class for references.
   *
   * @param <R>
   *          Type of the reference.
   */
  protected static class ReferenceHolder<R> {
    public R value = null;
  }

  private static final Logger LOGGER = Logger
      .getLogger(BlobstoreStressAndConsistencyTester.class.getName());

  /**
   * Runs a stress and optionally a consistency test.
   *
   * @param config
   *          The configuration of the stress tests.
   * @param transactionPropagator
   *          Transaction helper that makes it possible to run the same action on all blobstores
   *          atomically.
   * @param blobstores
   *          One or more blobstores to run the stress test on parallel. In case multiple blobstores
   *          are defined, a consistency test is checked during read operations that the blobs have
   *          the same data within the different stores.
   */
  public static void runStressTest(final BlobstoreStressTestConfiguration config,
      final TransactionPropagator transactionPropagator, final Blobstore... blobstores) {
    Objects.nonNull(blobstores);
    Objects.nonNull(config);

    if (blobstores.length == 0) {
      return;
    }

    BlobstoreStressAndConsistencyTester tester = new BlobstoreStressAndConsistencyTester(config,
        transactionPropagator, blobstores);
    tester.runTest();
  }

  protected final List<long[]> availableBlobs = new ArrayList<>();

  protected final ReadWriteLock availableBlobsRWLock = new ReentrantReadWriteLock(false);

  protected final int averageInitialBlobSize;

  protected final int averageReadAmount;

  protected final int averageReadBufferSize;

  protected final int averageUpdateAmount;

  protected final int averageUpdateBufferSize;

  protected final Blobstore[] blobstores;

  protected final int createActionTopIndex;

  protected final int deleteActionTopIndex;

  protected final int initialBlobNum;

  protected final int iterationNum;

  protected AtomicReference<Throwable> occuredThrowable = new AtomicReference<Throwable>();

  protected final int readActionTopIndex;

  protected final int threadNum;

  protected final TransactionPropagator transactionPropagator;

  protected final int updateActionTopIndex;

  /**
   * Constructor.
   *
   * @param config
   *          The configuration of the stress tests.
   * @param transactionPropagator
   *          Transaction helper that makes it possible to run the same action on all blobstores
   *          atomically.
   * @param blobstores
   *          One or more blobstores to run the stress test on parallel. In case multiple blobstores
   *          are defined, a consistency test is checked during read operations that the blobs have
   *          the same data within the different stores.
   */
  protected BlobstoreStressAndConsistencyTester(final BlobstoreStressTestConfiguration config,
      final TransactionPropagator transactionPropagator,
      final Blobstore[] blobstores) {
    this.blobstores = blobstores;
    this.averageInitialBlobSize = config.averageInitialBlobSize;
    this.averageReadAmount = config.averageReadAmount;
    this.averageReadBufferSize = config.averageReadBufferSize;
    this.averageUpdateAmount = config.averageUpdateAmount;
    this.averageUpdateBufferSize = config.averageUpdateBufferSize;
    this.createActionTopIndex = config.createActionChancePart;
    this.readActionTopIndex = config.readActionChancePart + this.createActionTopIndex;
    this.updateActionTopIndex = config.updateActionChancePart + this.readActionTopIndex;
    this.deleteActionTopIndex = config.deleteActionChancePart + this.updateActionTopIndex;

    this.initialBlobNum = config.initialBlobNum;
    this.threadNum = config.threadNum;
    this.iterationNum = config.iterationNumPerThread;

    this.transactionPropagator = transactionPropagator;

  }

  /**
   * Ads the specified blob ids to {@link #availableBlobs} within a writeLock.
   *
   * @param blobIds
   *          The ids of the same blob within the different {@link #blobstores}.
   */
  protected void addBlobIdsToAvailableBlobs(final long[] blobIds) {
    Lock writeLock = availableBlobsRWLock.writeLock();
    writeLock.lock();
    try {
      availableBlobs.add(blobIds);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Closes all closeable within the passed array.
   *
   * @param closeables
   *          An array of closeables that should be closed. The array might contain
   *          <code>null</code> entries.
   * @param throwable
   *          The throwable that caused this action to run. This throwable will be rethrown in the
   *          end of this function.
   */
  protected void closeCloseables(final Object[] closeables, final Throwable throwable) {
    for (Object closeable2 : closeables) {
      Closeable closeable = (Closeable) closeable2;
      if (closeable != null) {
        try {
          closeable.close();
        } catch (Exception e) {
          throwable.addSuppressed(e);
        }
      }
    }
    if (throwable instanceof Error) {
      throw (Error) throwable;
    } else if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    } else {
      throw new RuntimeException(throwable);
    }
  }

  /**
   * Creates a blob with random content.
   */
  protected void createBlob() {
    Random r = new Random();
    int blobSize = r.nextInt(averageInitialBlobSize * 2);
    byte[] blobContent = createRandomContent(blobSize);
    transactionPropagator.required(() -> {
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

  /**
   * Creates the specified initial amount of blobs with random content.
   */
  protected void createInitialBlobs() {
    for (int i = 0; i < initialBlobNum; i++) {
      createBlob();
    }
  }

  /**
   * Generates a byte array filled with random content.
   *
   * @param size
   *          The size of the generated byte array.
   * @return The byte array with random content.
   */
  protected byte[] createRandomContent(final int size) {
    Random r = new Random();
    byte[] result = new byte[size];
    r.nextBytes(result);
    return result;
  }

  /**
   * Deletes all blobs in all blobstores that were created by this tester.
   */
  protected void deleteAllBlobs() {
    transactionPropagator.required(() -> {
      for (long[] blobIds : availableBlobs) {
        for (int i = 0; i < blobIds.length; i++) {
          blobstores[i].deleteBlob(blobIds[i]);
        }
      }
      return null;
    });

  }

  /**
   * Deletes a random chosen blob from all Blobstores.
   */
  protected void deleteBlob() {
    long[] blobIds = getBlobIdsForRandomBlob();
    if (blobIds == null) {
      return;
    }
    try {
      transactionPropagator.required(() -> {
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

  /**
   * Selects a Blob with a random mechanism and returns the blobIds of it for each Blobstore.
   *
   * @return The blobIds for each Blobstore.
   */
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

  /**
   * Reads a random amount of content of a randomly chosen Blob.
   */
  protected void readBlobContent() {
    Random random = new Random();
    ReferenceHolder<byte[]> sampleContent = new ReferenceHolder<byte[]>();
    IntegerHolder startPositionHolder = new IntegerHolder();
    IntegerHolder readAmountHolder = new IntegerHolder();
    ReferenceHolder<Long> version = new ReferenceHolder<Long>();
    int bufferSize = random.nextInt(averageReadBufferSize * 2) + 1;
    runTestActionWithRandomBlob(
        (blobstore, blobId) -> {
          try {
            BlobReader blobReader = blobstore.readBlob(blobId);

            if (version.value == null) {
              version.value = blobReader.getVersion();
            } else if (!version.value.equals(blobReader.getVersion())) {
              throw new PossibleConcurrentException("Blob version conflict", null);
            }

            return blobReader;
          } catch (NoSuchBlobException e) {
            throw new PossibleConcurrentException("Blob has been deleted before reading", e);
          }
        } ,
        (blobstore, blobReader) -> {
          if (blobReader.getVersion() > 1) {
            System.out.flush();
          }
          try {
            if (sampleContent.value == null) {
              int blobSize = (int) blobReader.getSize();
              if (blobSize > 0) {
                startPositionHolder.value = random.nextInt(blobSize);
              }
              readAmountHolder.value = random.nextInt(averageReadAmount * 2);
            }
            byte[] content = readBlobContent(blobReader, startPositionHolder.value,
                readAmountHolder.value,
                bufferSize);
            if (sampleContent.value == null) {
              sampleContent.value = content;
            } else {
              try {
                Assert.assertArrayEquals(sampleContent.value, content);
              } catch (AssertionError e) {
                LOGGER.log(Level.INFO,
                    "Conflict in Blob: [blobId=" + blobReader.getBlobId() + ", version="
                        + blobReader.getVersion() + "]");
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

  /**
   * Reads an amount from a Blob used with a specific blobReader, starting from the specified start
   * position and a random buffer size. If the blob ends before the amount is read, the size of the
   * returned byte array will be smaller.
   *
   * @param blobReader
   *          The blob reader that is used to read the content.
   * @param startPosition
   *          The start position of the reading.
   * @param amount
   *          The maximum amount of bytes to read. If the blob ends before the specified amount,
   *          reading is discontinued.
   * @return The content part of the blob.
   */
  protected byte[] readBlobContent(final BlobReader blobReader, final int startPosition,
      final int amount, final int bufferSize) {
    blobReader.seek(startPosition);
    byte[] result = new byte[amount];
    int offset = 0;

    byte[] buffer = new byte[bufferSize];
    int remaining = amount - offset;

    while (remaining > 0) {
      int r = blobReader.read(buffer, 0, bufferSize);
      if (r < 0) {
        remaining = 0;
      } else {
        int amountToCopy = r;
        if (remaining < r) {
          amountToCopy = remaining;
        }
        System.arraycopy(buffer, 0, result, offset, amountToCopy);
        remaining -= amountToCopy;
        offset += r;
      }
    }
    return result;
  }

  /**
   * Runs the stress and optionally consistency test.
   */
  protected void runTest() {
    long startTime = System.currentTimeMillis();
    createInitialBlobs();
    long initializationEndTime = System.currentTimeMillis();

    LOGGER.info("Creation of " + initialBlobNum + " blob(s) took "
        + (initializationEndTime - startTime) + " ms.");

    CountDownLatch countDownLatch = new CountDownLatch(threadNum);
    for (int i = 0; i < threadNum; i++) {
      new Thread(() -> {
        try {
          runTestIterationsInThread();
        } catch (Throwable th) {
          occuredThrowable.compareAndSet(null, th);
        } finally {
          countDownLatch.countDown();
        }
      }).start();
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      if (occuredThrowable.get() != null) {
        Throwable occured = occuredThrowable.get();
        occured.addSuppressed(e);
        throwAsUnchecked(e);
      } else {
        throw new RuntimeException(e);
      }
    }
    if (occuredThrowable.get() != null) {
      throwAsUnchecked(occuredThrowable.get());
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - initializationEndTime;
    int operationNum = threadNum * iterationNum;
    LOGGER.info(
        operationNum + " blob operation on " + threadNum + " thread(s) took " + duration + " ms. ");

    deleteAllBlobs();
    long deleteBlobsEnd = System.currentTimeMillis();

    LOGGER.info("Deleting of " + availableBlobs.size() + " blobs took " + (deleteBlobsEnd - endTime)
        + " ms.");
  }

  /**
   * This is a helper method to run read and write operations on blobstores in the way that every
   * readers/writers are closed in case of an exception for sure.
   *
   * @param preNonConcurrentAction
   *          An action that accepts a Blobstore and a Blob id. The action should return a Closeable
   *          instance (a BlobReader or writer).
   * @param action
   *          The action that does the actual operation on the BlobReader or Writer.
   */
  protected <R extends Closeable> void runTestActionWithRandomBlob(
      final BiFunction<Blobstore, Long, R> preNonConcurrentAction,
      final BiConsumer<Blobstore, R> action) {
    long[] blobIds = getBlobIdsForRandomBlob();

    if (blobIds == null) {
      return;
    }

    transactionPropagator.required(() -> {
      Object[] results = new Object[blobstores.length];
      synchronized (blobIds) {
        try {
          for (int i = 0; i < blobstores.length; i++) {
            results[i] = preNonConcurrentAction.apply(blobstores[i], blobIds[i]);
          }
        } catch (RuntimeException | Error e) {
          closeCloseables(results, e);
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

  /**
   * Runs the test iterations within a thread.
   */
  protected void runTestIterationsInThread() {
    Random r = new Random();
    for (int i = 0; i < iterationNum; i++) {
      if (occuredThrowable.get() != null) {
        return;
      }
      int actionIndex = r.nextInt(deleteActionTopIndex);
      try {
        if (actionIndex < createActionTopIndex) {
          createBlob();
        } else if (actionIndex < readActionTopIndex) {
          readBlobContent();
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

  /**
   * Rethrows the passed throwable if unchecked otherwise throws a {@link RuntimeException} that
   * wraps the passed throwable.
   *
   * @param throwable
   *          The {@link Throwable} to throw as unchecked.
   */
  protected void throwAsUnchecked(final Throwable throwable) {
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    } else if (throwable instanceof Error) {
      throw (Error) throwable;
    } else {
      throw new RuntimeException(throwable);
    }
  }

  /**
   * Updates a random blob with random content.
   */
  protected void updateBlob() {
    Random random = new Random();
    AtomicReference<byte[]> randomContentHolder = new AtomicReference<byte[]>(null);
    IntegerHolder startPositionHolder = new IntegerHolder();
    runTestActionWithRandomBlob(
        (blobstore, blobId) -> {
          try {
            return blobstore.updateBlob(blobId);
          } catch (NoSuchBlobException e) {
            throw new PossibleConcurrentException("Blob has been deleted during update", e);
          }
        } ,
        (blobstore, blobAccessor) -> {
          try {
            byte[] randomContent = randomContentHolder.get();
            if (randomContent == null) {
              randomContent = createRandomContent(random.nextInt(averageUpdateAmount));
              randomContentHolder.set(randomContent);
              int blobSize = (int) blobAccessor.getSize();
              if (blobSize > 0) {
                startPositionHolder.value = random.nextInt(blobSize);
              }
            }

            updateBlobWithContent(blobAccessor, startPositionHolder.value, randomContent, random);
          } finally {
            blobAccessor.close();
          }
        });
  }

  /**
   * Updates the blob with the defined content.
   *
   * @param blobAccessor
   *          The accessor that can update data on the blob.
   * @param startPosition
   *          The starting position where the write should be started.
   * @param randomContent
   *          The content that should be written to the blob.
   * @param random
   *          A random instance to create the buffer with a random size.
   */
  protected void updateBlobWithContent(final BlobAccessor blobAccessor, final int startPosition,
      final byte[] randomContent, final Random random) {
    blobAccessor.seek(startPosition);
    int offset = 0;
    int remaining = randomContent.length;
    int amountToWrite = random.nextInt(averageUpdateBufferSize * 2) + 1;
    if (remaining < amountToWrite) {
      amountToWrite = remaining;
    }

    while (remaining > 0) {
      blobAccessor.write(randomContent, offset, amountToWrite);
      offset += amountToWrite;
      remaining -= amountToWrite;
      if (remaining < amountToWrite) {
        amountToWrite = remaining;
      }
    }
  }
}
