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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CanonicalExecutionEventProjectorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(
          new TenantId(UUID.fromString("10000000-0000-0000-0000-000000000001")),
          UUID.fromString("20000000-0000-0000-0000-000000000001"));

  @Test
  void appliesEachEventExactlyOnceAndIgnoresAStaleReplay() {
    var store = new OrderedStore();
    var projector = new CanonicalExecutionEventProjector(store);
    ExecutionEvent first = event("30000000-0000-0000-0000-000000000001", 0);
    ExecutionEvent second = event("30000000-0000-0000-0000-000000000002", 1);

    projector.project(first).toCompletableFuture().join();
    projector.project(first).toCompletableFuture().join();
    projector.project(second).toCompletableFuture().join();
    projector.project(first).toCompletableFuture().join();

    assertEquals(2, store.applied);
    assertEquals(1, store.lastSequence);
  }

  @Test
  void rejectsAnOrderingGapWithoutAdvancingTheProjection() {
    var store = new OrderedStore();
    var projector = new CanonicalExecutionEventProjector(store);

    var failure =
        assertThrows(
            CompletionException.class,
            () ->
                projector
                    .project(event("30000000-0000-0000-0000-000000000003", 1))
                    .toCompletableFuture()
                    .join());

    assertEquals(
        CanonicalExecutionEventProjector.ProjectionException.class, failure.getCause().getClass());
    assertEquals(-1, store.lastSequence);
  }

  private static ExecutionEvent event(String id, long sequence) {
    return new ExecutionEvent(
        UUID.fromString(id),
        UUID.fromString("40000000-0000-0000-0000-000000000001"),
        EXECUTION,
        EngineId.PEKKO,
        sequence,
        ExecutionEvent.EventType.STARTED,
        ExecutionLifecycleState.RUNNING,
        Instant.parse("2026-08-17T12:00:00Z"),
        JsonNodeFactory.instance.objectNode());
  }

  private static final class OrderedStore implements ExecutionProjectionStore {
    private final Set<UUID> events = new HashSet<>();
    private long lastSequence = -1;
    private int applied;

    @Override
    public ProjectionApplyResult apply(ExecutionEvent event) {
      if (events.contains(event.eventId())) {
        return ProjectionApplyResult.DUPLICATE;
      }
      if (event.sequence() <= lastSequence) {
        return ProjectionApplyResult.STALE;
      }
      if (event.sequence() != lastSequence + 1) {
        return ProjectionApplyResult.OUT_OF_ORDER;
      }
      events.add(event.eventId());
      lastSequence = event.sequence();
      applied++;
      return ProjectionApplyResult.APPLIED;
    }
  }
}
