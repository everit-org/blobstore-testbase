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

import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Abstract class to test blobstore use-cases. Tests can be skipped if {@link #getBlobStore()}
 * returns <code>null</code>.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractBlobstoreTest {

  /**
   * Simple holder to be able to pass value via lambda expressions.
   */
  private static final class BooleanHolder {
    public boolean value = false;

    public BooleanHolder(final boolean value) {
      this.value = value;
    }
  }

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
    lambdaBlobstore = new LambdaBlobstore(blobStore, getTransactionHelper());
    return lambdaBlobstore;
  }

  protected abstract TransactionHelper getTransactionHelper();

  private void notifyWaiter(final BooleanHolder shouldWait) {
    synchronized (shouldWait) {
      shouldWait.value = false;
      shouldWait.notifyAll();
    }
  }

  @Test
  public void test01ZeroLengthBlob() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(0, blobReader.size());
    });
  }

  @Test
  public void test02BlobCreationWithContent() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 2, 1, 2, 1 }, 1, 2);
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(2, blobReader.size());
      final int bufferSize = 5;
      byte[] buffer = new byte[bufferSize];
      final int readLength = 3;
      int read = blobReader.read(buffer, 1, readLength);
      Assert.assertEquals(2, read);
      Assert.assertArrayEquals(new byte[] { 0, 1, 2, 0, 0 }, buffer);
    });
  }

  @Test
  public void test03ParallelBlobUpdateAndRead() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 2 }, 0, 1);
    });

    BooleanHolder waitForReadCheck = new BooleanHolder(true);
    BooleanHolder waitForUpdate = new BooleanHolder(true);

    new Thread(() -> {
      try {
        blobStore.updateBlob(blobId, (blobAccessor) -> {
          blobAccessor.seek(blobAccessor.size());
          blobAccessor.write(new byte[] { 1 }, 0, 1);
          waitIfTrue(waitForReadCheck, DEFAULT_TIMEOUT);
        });
      } finally {
        notifyWaiter(waitForUpdate);
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
        Assert.assertEquals(0, blobReader.size());
      });
      blobStore.deleteBlob(blobIdOfSecondBlob);

    } finally {
      notifyWaiter(waitForReadCheck);
      waitIfTrue(waitForUpdate, DEFAULT_TIMEOUT);
    }

    blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void test04UpdateParallelBlobManipulationWithTransaction() {
    LambdaBlobstore blobStore = getLambdaBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 0 }, 0, 1);
    });

    BooleanHolder waitForAppendByBlockedThread = new BooleanHolder(true);

    getTransactionHelper().required(() -> {
      blobStore.updateBlob(blobId, (blobAccessor) -> {
        blobAccessor.seek(blobAccessor.size());
        blobAccessor.write(new byte[] { 1 }, 0, 1);
      });
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

      getTransactionHelper().requiresNew(() -> {
        blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(1, blobReader.size()));
        return null;
      });

      long blobId2 = blobStore.createBlob(null);
      blobStore.updateBlob(blobId2, (blobAccessor) -> {

      });
      blobStore.deleteBlob(blobId2);

      Thread otherUpdatingThread = new Thread(() -> {
        try {
          blobStore.updateBlob(blobId, (blobAccessor) -> {
            blobAccessor.seek(blobAccessor.size());
            blobAccessor.write(new byte[] { 2 }, 0, 1);

          });
        } finally {
          notifyWaiter(waitForAppendByBlockedThread);
        }
      });
      otherUpdatingThread.start();
      final int maxWaitTime = 1000;
      waitForThreadStateOrSocketRead(otherUpdatingThread, State.WAITING, maxWaitTime);
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

      return null;
    });

    waitIfTrue(waitForAppendByBlockedThread, DEFAULT_TIMEOUT);
    final int expectedBlobSize = 3;

    blobStore.readBlob(blobId,
        (blobReader) -> Assert.assertEquals(expectedBlobSize, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void test05Seek() {
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

  private boolean threadWaitingOnStreamRead(final Thread thread) {
    StackTraceElement[] stackTrace = thread.getStackTrace();
    if (stackTrace.length == 0) {
      return false;
    }
    StackTraceElement stackTraceElement = stackTrace[0];
    return stackTraceElement.isNativeMethod()
        && stackTraceElement.getClassName().contains("InputStream");
  }

  private void waitForThreadStateOrSocketRead(final Thread thread, final State state,
      final int maxWaitTime) {

    long startTime = System.currentTimeMillis();
    while (thread.getState() != state && !threadWaitingOnStreamRead(thread)) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - startTime > maxWaitTime) {
        StackTraceElement[] stackTrace = thread.getStackTrace();
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement stackTraceElement : stackTrace) {
          sb.append("\n").append(stackTraceElement.toString());
        }

        throw new IllegalStateException("Expected thread state is " + state
            + "; current thread state: " + thread.getState()
            + ", and thread is also not waiting on socket read:" + sb.toString());
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void waitIfTrue(final BooleanHolder shouldWait, final long timeout) {
    long startTime = System.currentTimeMillis();
    long timeElapsed = 0;
    while (shouldWait.value && timeElapsed < timeout) {
      synchronized (shouldWait) {
        timeElapsed = System.currentTimeMillis() - startTime;
        if (shouldWait.value && timeElapsed < timeout) {
          try {
            shouldWait.wait(timeout - timeElapsed);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    if (shouldWait.value) {
      Assert.fail("Expected operation has not been run until timeout");
    }
  }
}
