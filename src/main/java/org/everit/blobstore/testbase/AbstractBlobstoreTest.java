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
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Abstract class to test blobstore use-cases.
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

  protected abstract Blobstore getBlobStore();

  protected abstract TransactionHelper getTransactionHelper();

  private void notifyWaiter(final BooleanHolder shouldWait) {
    synchronized (shouldWait) {
      shouldWait.value = false;
      shouldWait.notifyAll();
    }
  }

  @Test
  public void test01ZeroLengthBlob() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(0, blobReader.size());
    });
  }

  @Test
  public void test02BlobCreationWithContent() {
    Blobstore blobStore = getBlobStore();
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
  public void test03UpdateParallelBlobManipulationWithoutTransaction() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 0 }, 0, 1);
    });

    BooleanHolder waitForReadCheck = new BooleanHolder(true);
    BooleanHolder waitForUpdate = new BooleanHolder(true);

    new Thread(() -> {
      blobStore.updateBlob(blobId, (blobAccessor) -> {
        blobAccessor.seek(blobAccessor.size());
        blobAccessor.write(new byte[] { 1 }, 0, 1);
        waitIfTrue(waitForReadCheck);
      });
      notifyWaiter(waitForUpdate);
    }).start();

    // Do some test read before update finishes
    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(1, blobReader.size());
    });

    // Create another blob until lock of first blob holds
    long blobIdOfSecondBlob = blobStore.createBlob(null);
    blobStore.readBlob(blobIdOfSecondBlob, (blobReader) -> {
      Assert.assertEquals(0, blobReader.size());
    });
    blobStore.deleteBlob(blobIdOfSecondBlob);

    notifyWaiter(waitForReadCheck);
    waitIfTrue(waitForUpdate);

    blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void test04UpdateParallelBlobManipulationWithTransaction() {
    Blobstore blobStore = getBlobStore();
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
        blobStore.updateBlob(blobId, (blobAccessor) -> {
          blobAccessor.seek(blobAccessor.size());
          blobAccessor.write(new byte[] { 2 }, 0, 1);

        });
        notifyWaiter(waitForAppendByBlockedThread);
      });
      otherUpdatingThread.start();
      final int maxWaitTime = 1000;
      waitForThreadState(otherUpdatingThread, State.WAITING, maxWaitTime);
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

      return null;
    });

    waitIfTrue(waitForAppendByBlockedThread);
    final int expectedBlobSize = 3;

    blobStore.readBlob(blobId,
        (blobReader) -> Assert.assertEquals(expectedBlobSize, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void test05Seek() {
    Blobstore blobStore = getBlobStore();
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

  private void waitForThreadState(final Thread otherUpdatingThread, final State state,
      final int maxWaitTime) {

    int i = 0;
    while (i < maxWaitTime && otherUpdatingThread.getState() != state) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      i++;
    }

    if (otherUpdatingThread.getState() != state) {
      throw new IllegalStateException("Expected thread state is " + state
          + "; current thread state: " + otherUpdatingThread.getState());
    }
  }

  private void waitIfTrue(final BooleanHolder shouldWait) {
    while (shouldWait.value) {
      synchronized (shouldWait) {
        if (shouldWait.value) {
          try {
            shouldWait.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
