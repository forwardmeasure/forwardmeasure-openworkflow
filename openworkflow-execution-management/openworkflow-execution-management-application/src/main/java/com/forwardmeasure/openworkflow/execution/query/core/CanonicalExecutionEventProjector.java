/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.openworkflow.execution.query.core;

import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Shared idempotent projection boundary used unchanged by both real engines. */
public final class CanonicalExecutionEventProjector implements ExecutionEventSink {
  private final ExecutionProjectionStore store;

  public CanonicalExecutionEventProjector(ExecutionProjectionStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  @Override
  public CompletionStage<Void> project(ExecutionEvent event) {
    Objects.requireNonNull(event, "event");
    ExecutionProjectionStore.ProjectionApplyResult result = store.apply(event);
    return switch (result) {
      case APPLIED, DUPLICATE, STALE -> CompletableFuture.completedFuture(null);
      case OUT_OF_ORDER ->
          CompletableFuture.failedFuture(
              new ProjectionException("event sequence has a gap: " + event.sequence()));
      case NOT_FOUND ->
          CompletableFuture.failedFuture(new ProjectionException("execution is not admitted"));
      case ENGINE_MISMATCH ->
          CompletableFuture.failedFuture(
              new ProjectionException("event engine differs from the pinned engine"));
    };
  }

  @Override
  public CompletionStage<Void> projectNext(ExecutionEvent event) {
    Objects.requireNonNull(event, "event");
    return result(store.applyNext(event), event.sequence());
  }

  private CompletionStage<Void> result(
      ExecutionProjectionStore.ProjectionApplyResult result, long sequence) {
    return switch (result) {
      case APPLIED, DUPLICATE, STALE -> CompletableFuture.completedFuture(null);
      case OUT_OF_ORDER ->
          CompletableFuture.failedFuture(
              new ProjectionException("event sequence has a gap: " + sequence));
      case NOT_FOUND ->
          CompletableFuture.failedFuture(new ProjectionException("execution is not admitted"));
      case ENGINE_MISMATCH ->
          CompletableFuture.failedFuture(
              new ProjectionException("event engine differs from the pinned engine"));
    };
  }

  public static final class ProjectionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ProjectionException(String message) {
      super(message);
    }
  }
}
