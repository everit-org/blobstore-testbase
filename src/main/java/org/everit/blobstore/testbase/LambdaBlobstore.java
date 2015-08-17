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

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;

import org.everit.blobstore.BlobAccessor;
import org.everit.blobstore.BlobReader;
import org.everit.blobstore.Blobstore;
import org.everit.transaction.propagator.TransactionPropagator;

/**
 * Helper class to be able to use blobstore via Java 8 lambda expressions.
 */
public class LambdaBlobstore {

  private final Set<Long> blobIdsToDelete = new ConcurrentSkipListSet<>();

  private final TransactionPropagator transactionPropagator;

  private final Blobstore wrapped;

  public LambdaBlobstore(final Blobstore wrapped,
      final TransactionPropagator transactionPropagator) {
    this.wrapped = wrapped;
    this.transactionPropagator = transactionPropagator;
  }

  /**
   * See {@link Blobstore#createBlob()}.
   */
  public long createBlob(final Consumer<BlobAccessor> action) {
    long blobId = transactionPropagator.required(() -> {
      try (BlobAccessor blobAccessor = wrapped.createBlob()) {
        if (action != null) {
          action.accept(blobAccessor);
        }
        return blobAccessor.getBlobId();
      }
    });
    blobIdsToDelete.add(blobId);
    return blobId;
  }

  /**
   * Deletes all blobs that were created by this blobstore wrapper.
   */
  public void deleteAllCreatedBlobs() {
    Long[] blobIdArray = new Long[blobIdsToDelete.size()];
    blobIdsToDelete.toArray(blobIdArray);
    for (Long blobId : blobIdArray) {
      deleteBlob(blobId);
    }

  }

  /**
   * See {@link Blobstore#deleteBlob(long)}.
   */
  public void deleteBlob(final long blobId) {
    transactionPropagator.required(() -> {
      wrapped.deleteBlob(blobId);
      return null;
    });
    blobIdsToDelete.remove(blobId);
  }

  /**
   * See {@link Blobstore#readBlob(long)}.
   */
  public void readBlob(final long blobId, final Consumer<BlobReader> action) {
    transactionPropagator.required(() -> {
      try (BlobReader blobReader = wrapped.readBlob(blobId)) {
        if (action != null) {
          action.accept(blobReader);
        }
      }
      return null;
    });
  }

  /**
   * See {@link Blobstore#updateBlob(long)}.
   */
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> action) {
    transactionPropagator.required(() -> {
      try (BlobAccessor blobAccessor = wrapped.updateBlob(blobId)) {
        if (action != null) {
          action.accept(blobAccessor);
        }
      }
      return null;
    });
  }
}
