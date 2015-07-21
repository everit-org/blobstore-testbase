package org.everit.blobstore.testbase;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.osgi.transaction.helper.api.TransactionHelper;

/**
 * Helper class to be able to use blobstore via Java 8 lambda expressions.
 */
public class LambdaBlobstore {

  private final Set<Long> blobIdsToDelete = new ConcurrentSkipListSet<>();

  private final TransactionHelper transactionHelper;

  private final Blobstore wrapped;

  public LambdaBlobstore(final Blobstore wrapped, final TransactionHelper transactionHelper) {
    this.wrapped = wrapped;
    this.transactionHelper = transactionHelper;
  }

  /**
   * See {@link Blobstore#createBlob()}.
   */
  public long createBlob(final Consumer<BlobAccessor> action) {
    long blobId = transactionHelper.required(() -> {
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
    transactionHelper.required(() -> {
      wrapped.deleteBlob(blobId);
      return null;
    });
    blobIdsToDelete.remove(blobId);
  }

  /**
   * See {@link Blobstore#readBlob(long)}.
   */
  public void readBlob(final long blobId, final Consumer<BlobReader> action) {
    transactionHelper.required(() -> {
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
    transactionHelper.required(() -> {
      try (BlobAccessor blobAccessor = wrapped.updateBlob(blobId)) {
        if (action != null) {
          action.accept(blobAccessor);
        }
      }
      return null;
    });
  }
}
