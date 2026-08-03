/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.iceberg;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Runs tasks on an {@link ExecutorService} with bounded in-flight submissions.
 */
final class BackpressureParallelExecutor {

  @FunctionalInterface
  interface ThrowingFunction<T, R> {
    R apply(T item) throws Exception;
  }

  @FunctionalInterface
  interface ResultHandler<R> {
    void accept(R result) throws ExecutionException, InterruptedException;
  }

  private BackpressureParallelExecutor() {
    // utility class
  }

  static <T, R> void run(
      ExecutorService executor,
      int maxInFlight,
      Iterable<T> items,
      ThrowingFunction<T, R> task,
      ResultHandler<R> onResult,
      String interruptedMessage,
      String executionMessage) {

    Semaphore semaphore = new Semaphore(maxInFlight);
    ExecutorCompletionService<R> completionService = new ExecutorCompletionService<>(executor);
    int submittedTasks = 0;
    int completedTasks = 0;

    try {
      for (T item : items) {
        semaphore.acquire();

        boolean taskSubmitted = false;
        try {
          completionService.submit(() -> {
            try {
              return task.apply(item);
            } finally {
              semaphore.release();
            }
          });
          taskSubmitted = true;
          submittedTasks++;
        } finally {
          if (!taskSubmitted) {
            semaphore.release();
          }
        }

        Future<R> done;
        while ((done = completionService.poll()) != null) {
          onResult.accept(done.get());
          completedTasks++;
        }
      }

      while (completedTasks < submittedTasks) {
        onResult.accept(completionService.take().get());
        completedTasks++;
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
      throw new RuntimeException(interruptedMessage, e);

    } catch (ExecutionException e) {
      executor.shutdownNow();
      throw new RuntimeException(executionMessage, e.getCause());
    }
  }
}
