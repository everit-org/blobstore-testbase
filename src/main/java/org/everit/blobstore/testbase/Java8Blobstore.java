package org.everit.blobstore.testbase;

import java.util.function.Consumer;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.osgi.transaction.helper.api.TransactionHelper;

public class Java8Blobstore {

  private final TransactionHelper transactionHelper;

  private final Blobstore wrapped;

  public Java8Blobstore(final Blobstore wrapped, final TransactionHelper transactionHelper) {
    this.wrapped = wrapped;
    this.transactionHelper = transactionHelper;
  }

  public long createBlob(final Consumer<BlobAccessor> action) {
    return transactionHelper.required(() -> {
      try (BlobAccessor blobAccessor = wrapped.createBlob()) {
        if (action != null) {
          action.accept(blobAccessor);
        }
        return blobAccessor.getBlobId();
      }
    });
  }

  public void deleteBlob(final long blobId) {
    transactionHelper.required(() -> {
      wrapped.deleteBlob(blobId);
      return null;
    });
  }

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
