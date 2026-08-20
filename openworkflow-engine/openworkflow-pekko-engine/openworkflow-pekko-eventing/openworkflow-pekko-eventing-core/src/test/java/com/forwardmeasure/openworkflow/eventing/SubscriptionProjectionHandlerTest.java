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
package com.forwardmeasure.openworkflow.eventing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.ScheduleEvent;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.persistence.query.Sequence;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.junit.jupiter.api.Test;

final class SubscriptionProjectionHandlerTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:subscription-projection");

  @Test
  void indexesAndTombstonesWorkflowListenByJournalRevision() throws Exception {
    var executionId = new ExecutionId(TENANT, UUID.randomUUID());
    var repository = new MemoryRepository();
    var handler = new WorkflowSubscriptionProjectionHandler(repository);
    String persistenceId = "workflow-execution|" + executionId.entityId();
    var data = JsonNodeFactory.instance.objectNode();

    handler
        .process(
            envelope(
                persistenceId,
                3,
                new EngineEvent.ListenStarted(
                    UUID.randomUUID(),
                    "/do/0/listen",
                    data,
                    data,
                    0,
                    "listen-1",
                    Set.of("orders.created.v1"),
                    AT)))
        .toCompletableFuture()
        .get();
    assertEquals(1, repository.values.size());
    assertTrue(repository.values.getFirst().active());
    assertEquals(Set.of("orders.created.v1"), repository.values.getFirst().eventTypes());

    handler
        .process(
            envelope(
                persistenceId,
                4,
                new EngineEvent.ListenEventAccepted(
                    UUID.randomUUID(),
                    "/do/0/listen",
                    "listen-1",
                    event("orders.created.v1"),
                    List.of(event("orders.created.v1")),
                    Map.of(),
                    Set.of(0),
                    true,
                    data,
                    data,
                    1,
                    AT)))
        .toCompletableFuture()
        .get();
    assertFalse(repository.values.getFirst().active());
    assertEquals(4, repository.values.getFirst().revision());
  }

  @Test
  void keepsForkSubscriptionUntilTheLastParallelListenerCompletes() throws Exception {
    var executionId = new ExecutionId(TENANT, UUID.randomUUID());
    var repository = new MemoryRepository();
    var handler = new WorkflowSubscriptionProjectionHandler(repository);
    String persistenceId = "workflow-execution|" + executionId.entityId();
    var data = JsonNodeFactory.instance.objectNode();
    var cloudEvent = event("orders.created.v1");

    handler
        .process(
            envelope(
                persistenceId,
                3,
                new EngineEvent.ForkBranchListenStarted(
                    UUID.randomUUID(),
                    "/parallel",
                    List.of(0),
                    "/parallel/receive",
                    data,
                    data,
                    "listen-1",
                    Set.of("orders.created.v1"),
                    true,
                    AT)))
        .toCompletableFuture()
        .get();
    assertTrue(repository.values.getFirst().active());
    assertEquals(Set.of(), repository.values.getFirst().eventTypes());

    var partial =
        new EngineEvent.ForkListenUpdate(
            List.of(0),
            "/parallel/receive",
            "listen-1",
            List.of(cloudEvent),
            Map.of(),
            Set.of(0),
            null,
            EngineEvent.ForkListenDisposition.COMPLETE,
            data,
            data,
            1,
            null,
            null,
            null);
    handler
        .process(
            envelope(
                persistenceId,
                4,
                new EngineEvent.ForkBranchListenAccepted(
                    UUID.randomUUID(), "/parallel", cloudEvent, List.of(partial), true, true, AT)))
        .toCompletableFuture()
        .get();
    assertTrue(repository.values.getFirst().active());

    handler
        .process(
            envelope(
                persistenceId,
                5,
                new EngineEvent.ForkBranchListenAccepted(
                    UUID.randomUUID(),
                    "/parallel",
                    cloudEvent,
                    List.of(partial),
                    false,
                    false,
                    AT)))
        .toCompletableFuture()
        .get();
    assertFalse(repository.values.getFirst().active());
    assertEquals(5, repository.values.getFirst().revision());
  }

  @Test
  void indexesEventScheduleUsingTheSharedLiteralTypeSelector() throws Exception {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: event-schedule-index
                  version: '1.0.0'
                schedule:
                  on:
                    any:
                      - with: { type: orders.created.v1 }
                      - with: { type: orders.cancelled.v1 }
                do:
                  - work: { set: { accepted: true } }
                """
                    .getBytes(StandardCharsets.UTF_8));
    var scheduleId = new ScheduleId(TENANT, plan.coordinates());
    var repository = new MemoryRepository();
    var handler = new ScheduleSubscriptionProjectionHandler(repository);
    handler
        .process(
            EventEnvelope.create(
                new Sequence(1),
                "workflow-schedule|" + scheduleId.entityId(),
                1,
                new ScheduleEvent.Registered(
                    UUID.randomUUID(),
                    scheduleId,
                    new ActorIdentity(TENANT, "did:web:actor:scheduler"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    null,
                    null,
                    AT),
                0L))
        .toCompletableFuture()
        .get();

    assertEquals(
        Set.of("orders.created.v1", "orders.cancelled.v1"),
        repository.values.getFirst().eventTypes());
    assertEquals(
        CloudEventSubscription.TargetKind.SCHEDULE, repository.values.getFirst().targetKind());
  }

  private static EventEnvelope<EngineEvent> envelope(
      String persistenceId, long sequence, EngineEvent event) {
    return EventEnvelope.create(new Sequence(sequence), persistenceId, sequence, event, 0L);
  }

  private static com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event(String type) {
    return new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
        "1.0",
        UUID.randomUUID().toString(),
        java.net.URI.create("urn:test"),
        type,
        null,
        AT,
        "application/json",
        JsonNodeFactory.instance.objectNode(),
        Map.of());
  }

  private static final class MemoryRepository implements CloudEventSubscriptionRepository {
    private final List<CloudEventSubscription> values = new ArrayList<>();

    @Override
    public CompletionStage<Void> store(CloudEventSubscription value) {
      values.removeIf(
          existing ->
              existing.tenantId().equals(value.tenantId())
                  && existing.targetKind() == value.targetKind()
                  && existing.targetEntityId().equals(value.targetEntityId())
                  && existing.revision() < value.revision());
      if (values.stream()
          .noneMatch(
              existing ->
                  existing.targetEntityId().equals(value.targetEntityId())
                      && existing.revision() >= value.revision())) {
        values.add(value);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<CloudEventSubscription>> candidates(
        TenantId tenantId, String eventType, int limit) {
      return CompletableFuture.completedFuture(
          values.stream()
              .filter(CloudEventSubscription::active)
              .filter(value -> value.tenantId().equals(tenantId))
              .filter(value -> value.acceptsType(eventType))
              .limit(limit)
              .toList());
    }
  }
}
