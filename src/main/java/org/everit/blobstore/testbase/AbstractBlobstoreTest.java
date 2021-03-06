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

import java.lang.Thread.State;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.everit.blobstore.BlobReader;
import org.everit.blobstore.Blobstore;
import org.everit.transaction.propagator.TransactionPropagator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Abstract class to test blobstore use-cases. Tests can be skipped if {@link #getBlobStore()}
 * returns <code>null</code>.
 */
public abstract class AbstractBlobstoreTest {

  private static final int DEFAULT_TIMEOUT = 5000;

  private LambdaBlobstore lambdaBlobstore;

  @After
  public void after() {
    getLambdaBlobStore().deleteAllCreatedBlobs();
    lambdaBlobstore = null;
  }

  /**
   * Returns the {@link Blobstore} to test with. If the method returns <code>null</code>, all tests
   * will be skipped.
   *
   * @return The {@link Blobstore} to test with or <code>null</code> if the tests should be skipped.
   */
  protected abstract Blobstore getBlobStore();

  /**
   * Gets a Java 8 Blobstore that will delete all blobs created by the tests in the end.
   *
   * @return The lambda based blobstore.
   */
  protected LambdaBlobstore getLambdaBlobStore() {
    if (lambdaBlobstore != null) {
      return lambdaBlobstore;
    }
    Blobstore blobStore = getBlobStore();
    Assume.assumeNotNull(blobStore);
    lambdaBlobstore = new LambdaBlobstore(blobStore, getTransactionPropagator());
    return lambdaBlobstore;
  }

  protected abstract TransactionPropagator getTransactionPropagator();

  @Test
  public void testBlobCreationWithContent() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 2, 1, 2, 1 }, 1, 2);
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(2, blobReader.getSize());
      final int bufferSize = 5;
      byte[] buffer = new byte[bufferSize];
      final int readLength = 3;
      int read = blobReader.read(buffer, 1, readLength);
      Assert.assertEquals(2, read);
      Assert.assertArrayEquals(new byte[] { 0, 1, 2, 0, 0 }, buffer);
    });
  }

  @Test
  public void testParallelBlobManipulationWithTwoTransactions() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 0 }, 0, 1);
    });

    BooleanHolder waitForAppendByBlockedThread = new BooleanHolder(false);

    getTransactionPropagator().required(() -> {
      blobStore.updateBlob(blobId, (blobAccessor) -> {
        blobAccessor.seek(blobAccessor.getSize());
        blobAccessor.write(new byte[] { 1 }, 0, 1);
      });
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.getSize()));

      getTransactionPropagator().requiresNew(() -> {
        blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(1, blobReader.getSize()));
        return null;
      });

      long blobId2 = blobStore.createBlob(null);
      blobStore.updateBlob(blobId2, (blobAccessor) -> {

      });
      blobStore.deleteBlob(blobId2);

      Thread otherUpdatingThread = new Thread(() -> {
        try {
          blobStore.updateBlob(blobId, (blobAccessor) -> {
            blobAccessor.seek(blobAccessor.getSize());
            blobAccessor.write(new byte[] { 2 }, 0, 1);

          });
        } finally {
          BlobstoreTestUtil.notifyAboutEvent(waitForAppendByBlockedThread);
        }
      });
      otherUpdatingThread.start();
      final int maxWaitTime = 1000;
      BlobstoreTestUtil.waitForThreadStateOrSocketRead(otherUpdatingThread, State.WAITING,
          maxWaitTime);
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.getSize()));

      return null;
    });

    Assert
        .assertTrue(BlobstoreTestUtil.waitForEvent(waitForAppendByBlockedThread, DEFAULT_TIMEOUT));
    final int expectedBlobSize = 3;

    blobStore.readBlob(blobId,
        (blobReader) -> Assert.assertEquals(expectedBlobSize, blobReader.getSize()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void testReadBlobDuringOngoingUpdateOnOtherThread() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 2 }, 0, 1);
    });

    BooleanHolder waitForReadCheck = new BooleanHolder(false);
    BooleanHolder waitForUpdate = new BooleanHolder(false);

    new Thread(() -> {
      try {
        blobStore.updateBlob(blobId, (blobAccessor) -> {
          blobAccessor.seek(blobAccessor.getSize());
          blobAccessor.write(new byte[] { 1 }, 0, 1);
          Assert.assertTrue(BlobstoreTestUtil.waitForEvent(waitForReadCheck, DEFAULT_TIMEOUT));
        });
      } finally {
        BlobstoreTestUtil.notifyAboutEvent(waitForUpdate);
      }
    }).start();

    try {
      // Do some test read before update finishes
      blobStore.readBlob(blobId, (blobReader) -> {
        byte[] buffer = new byte[2];
        int read = blobReader.read(buffer, 0, 2);
        Assert.assertEquals(1, read);
        Assert.assertEquals(2, buffer[0]);
      });

      // Create another blob until lock of first blob holds
      long blobIdOfSecondBlob = blobStore.createBlob(null);
      blobStore.readBlob(blobIdOfSecondBlob, (blobReader) -> {
        Assert.assertEquals(0, blobReader.getSize());
      });
      blobStore.deleteBlob(blobIdOfSecondBlob);

    } finally {
      BlobstoreTestUtil.notifyAboutEvent(waitForReadCheck);
      Assert.assertTrue(BlobstoreTestUtil.waitForEvent(waitForUpdate, DEFAULT_TIMEOUT));
    }

    blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.getSize()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void testSeek() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    final int sampleContentSize = 1024 * 1024;
    long blobId = blobStore.createBlob((blobAccessor) -> {

      byte[] sampleContent = new byte[sampleContentSize];
      final int byteMaxUnsignedDivider = 256;
      for (int i = 0; i < sampleContentSize; i++) {
        sampleContent[i] = (byte) (i % byteMaxUnsignedDivider);
      }

      blobAccessor.write(sampleContent, 0, sampleContentSize);

      testSeekAndRead(blobAccessor, sampleContentSize);
    });

    blobStore.readBlob(blobId, (blobReader) -> testSeekAndRead(blobReader, sampleContentSize));

    blobStore.updateBlob(blobId, (blobAccessor) -> {
      blobAccessor.seek(sampleContentSize);
      byte[] dataOnEnd = new byte[] { 2, 2 };
      blobAccessor.write(dataOnEnd, 0, dataOnEnd.length);
      blobAccessor.seek(sampleContentSize / 2);
      byte[] dataOnMiddle = new byte[] { 1, 1 };
      blobAccessor.write(dataOnMiddle, 0, dataOnMiddle.length);

      byte[] result = new byte[2];
      blobAccessor.seek(sampleContentSize);
      blobAccessor.read(result, 0, result.length);
      Assert.assertArrayEquals(dataOnEnd, result);
      blobAccessor.seek(sampleContentSize / 2);
      blobAccessor.read(result, 0, result.length);
      Assert.assertArrayEquals(dataOnMiddle, result);
    });
    blobStore.deleteBlob(blobId);
  }

  private void testSeekAndRead(final BlobReader blobAccessor, final int sampleContentSize) {
    byte[] buffer = new byte[2];
    blobAccessor.seek(sampleContentSize / 2);
    blobAccessor.read(buffer, 0, buffer.length);
    Assert.assertArrayEquals(new byte[] { 0, 1 }, buffer);

    blobAccessor.seek(1);
    blobAccessor.read(buffer, 0, buffer.length);
    Assert.assertArrayEquals(new byte[] { 1, 2 }, buffer);
  }

  @Test
  public void testVersionIsUpgradedDuringUpdate() {
    LambdaBlobstore blobstore = getLambdaBlobStore();
    Set<Long> usedVersions = Collections.synchronizedSet(new HashSet<>());
    final long blobId = blobstore.createBlob((blobAccessor) -> {
    });
    final int threadNum = 4;
    final int iterationNum = 50;

    AtomicBoolean sameVersionWasUsedTwice = new AtomicBoolean(false);

    CountDownLatch countDownLatch = new CountDownLatch(threadNum);
    for (int i = 0; i < threadNum; i++) {
      new Thread(() -> {
        try {
          for (int j = 0; j < iterationNum; j++) {
            blobstore.updateBlob(blobId, (blobAccessor) -> {
              boolean added = usedVersions.add(blobAccessor.getVersion());
              if (!added) {
                sameVersionWasUsedTwice.set(true);
              }
            });
          }
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
    Assert.assertFalse(sameVersionWasUsedTwice.get());
  }

  @Test
  public void testZeroLengthBlob() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(0, blobReader.getSize());
    });
  }

}
