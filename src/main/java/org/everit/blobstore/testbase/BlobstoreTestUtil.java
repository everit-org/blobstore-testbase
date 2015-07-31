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

/**
 * Util functions to run tests for blobstore.
 *
 */
public final class BlobstoreTestUtil {

  /**
   * Notifies the waiter about happening of the expected event.
   *
   * @param expectedEventHappened
   *          The holder class that will become true.
   */
  public static void notifyAboutEvent(final BooleanHolder expectedEventHappened) {
    synchronized (expectedEventHappened) {
      expectedEventHappened.value = true;
      expectedEventHappened.notifyAll();
    }
  }

  private static boolean threadWaitingOnStreamRead(final Thread thread) {
    StackTraceElement[] stackTrace = thread.getStackTrace();
    if (stackTrace.length == 0) {
      return false;
    }
    StackTraceElement stackTraceElement = stackTrace[0];
    return stackTraceElement.isNativeMethod()
        && stackTraceElement.getClassName().contains("InputStream");
  }

  /**
   * Waits until <code>shouldWait</code> becomes false or timeout happens.
   *
   * @param expectedEventHappened
   *          The holder that tells if the expected event has happened.
   * @param timeout
   *          The function will wait until the timeout if shouldWait does not become
   *          <code>false</code>.
   * @return true if shouldWait is false.
   */
  public static boolean waitForEvent(final BooleanHolder expectedEventHappened,
      final long timeout) {
    long startTime = System.currentTimeMillis();
    long timeElapsed = 0;
    while (!expectedEventHappened.value && timeElapsed < timeout) {
      synchronized (expectedEventHappened) {
        timeElapsed = System.currentTimeMillis() - startTime;
        if (!expectedEventHappened.value && timeElapsed < timeout) {
          try {
            expectedEventHappened.wait(timeout - timeElapsed);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return expectedEventHappened.value;
  }

  /**
   * Wait until the thread gets the expected state or the socket read stops the thread.
   *
   * @param thread
   *          The thread.
   * @param state
   *          The expected state.
   * @param maxWaitTime
   *          The maximum waiting time.
   */
  public static void waitForThreadStateOrSocketRead(final Thread thread, final State state,
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

  private BlobstoreTestUtil() {
  }
}
