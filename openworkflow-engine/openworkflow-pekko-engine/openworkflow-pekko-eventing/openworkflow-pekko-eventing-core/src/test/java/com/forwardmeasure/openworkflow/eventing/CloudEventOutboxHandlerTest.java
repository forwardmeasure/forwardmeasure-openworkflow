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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.execution.query.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pekko.persistence.query.Sequence;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.junit.jupiter.api.Test;

final class CloudEventOutboxHandlerTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void publishesBeforePersistConfirmedAcknowledgement() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:outbox");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var calls = new ArrayList<String>();
    var event =
        new WorkflowCloudEvent(
            "1.0",
            "event-1",
            URI.create("urn:test"),
            "test.v1",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("value", 1),
            Map.of());
    var requested =
        new EngineEvent.EmitRequested(
            UUID.randomUUID(),
            "/do/0/emit",
            JsonNodeFactory.instance.objectNode(),
            JsonNodeFactory.instance.objectNode(),
            1,
            "operation-1",
            event,
            AT);
    var handler =
        new CloudEventOutboxHandler(
            (operationId, emitted) -> {
              calls.add("publish:" + operationId);
              assertEquals(event.id(), emitted.id());
              assertEquals(tenant.toString(), emitted.extensions().get("tenant").asText());
              return CompletableFuture.completedFuture(null);
            },
            (routedId, operationId) -> {
              calls.add("ack:" + operationId);
              assertEquals(executionId, routedId);
              return CompletableFuture.completedFuture(
                  new WorkflowReply.Accepted(
                      UUID.randomUUID(), executionId, 3, ExecutionStatus.COMPLETED));
            });

    handler
        .process(
            EventEnvelope.<EngineEvent>create(
                Sequence.apply(1L),
                CloudEventOutboxHandler.PERSISTENCE_ID_PREFIX + executionId.entityId(),
                2,
                requested,
                AT.toEpochMilli()))
        .toCompletableFuture()
        .join();
    assertEquals(java.util.List.of("publish:operation-1", "ack:operation-1"), calls);
  }

  @Test
  void publishesForkLaneIntentBeforePersistConfirmedAcknowledgement() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:fork-outbox");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var calls = new ArrayList<String>();
    var data = JsonNodeFactory.instance.objectNode().put("value", 1);
    var event =
        new WorkflowCloudEvent(
            "1.0",
            "fork-event",
            URI.create("urn:test"),
            "fork.v1",
            null,
            AT,
            "application/json",
            data,
            Map.of());
    var requested =
        new EngineEvent.ForkBranchEmitRequested(
            UUID.randomUUID(),
            "/parallel",
            java.util.List.of(1, 0),
            "/parallel/nested/publish",
            data,
            data,
            "fork-operation",
            event,
            true,
            AT);
    var handler =
        new CloudEventOutboxHandler(
            (operationId, emitted) -> {
              calls.add("publish:" + operationId);
              assertEquals(event.id(), emitted.id());
              assertEquals(tenant.toString(), emitted.extensions().get("tenant").asText());
              return CompletableFuture.completedFuture(null);
            },
            (routedId, operationId) -> {
              calls.add("ack:" + operationId);
              assertEquals(executionId, routedId);
              return CompletableFuture.completedFuture(
                  new WorkflowReply.Accepted(
                      UUID.randomUUID(), executionId, 3, ExecutionStatus.WAITING));
            });

    handler
        .process(
            EventEnvelope.<EngineEvent>create(
                Sequence.apply(1L),
                CloudEventOutboxHandler.PERSISTENCE_ID_PREFIX + executionId.entityId(),
                2,
                requested,
                AT.toEpochMilli()))
        .toCompletableFuture()
        .join();
    assertEquals(java.util.List.of("publish:fork-operation", "ack:fork-operation"), calls);
  }

  @Test
  void publicationFailureDoesNotAcknowledgeOrAdvance() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:outbox-failure");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var acknowledged = new boolean[] {false};
    var event =
        new WorkflowCloudEvent(
            "1.0",
            "event-2",
            URI.create("urn:test"),
            "test.v1",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.nullNode(),
            Map.of());
    var handler =
        new CloudEventOutboxHandler(
            (operationId, ignored) ->
                CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")),
            (ignored, operationId) -> {
              acknowledged[0] = true;
              return CompletableFuture.completedFuture(null);
            });
    var envelope =
        EventEnvelope.<EngineEvent>create(
            Sequence.apply(1L),
            CloudEventOutboxHandler.PERSISTENCE_ID_PREFIX + executionId.entityId(),
            1,
            new EngineEvent.EmitRequested(
                UUID.randomUUID(),
                "/emit",
                JsonNodeFactory.instance.objectNode(),
                JsonNodeFactory.instance.objectNode(),
                1,
                "operation-2",
                event,
                AT),
            AT.toEpochMilli());

    assertThrows(
        CompletionException.class, () -> handler.process(envelope).toCompletableFuture().join());
    assertEquals(false, acknowledged[0]);
  }

  @Test
  void publishesWorkflowPauseBeforeItsActiveTaskNotifications() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:lifecycle-pause");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var published = new ArrayList<WorkflowCloudEvent>();
    var handler = lifecycleHandler(executionId, published);

    handler
        .process(
            envelope(
                executionId,
                4,
                new EngineEvent.Paused(UUID.randomUUID(), List.of("/do/0/await-approval"), AT)))
        .toCompletableFuture()
        .join();

    assertEquals(
        List.of(
            "io.serverlessworkflow.workflow.suspended.v1",
            "io.serverlessworkflow.task.suspended.v1"),
        published.stream().map(WorkflowCloudEvent::type).toList());
    assertEquals("/do/0/await-approval", published.get(1).data().required("task").asText());
  }

  @Test
  void reusesStartedCoordinatesWithoutRacingExecutionProjection() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:lifecycle-start");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var published = new ArrayList<WorkflowCloudEvent>();
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: orders
                  name: delayed
                  version: '1.0.0'
                do:
                  - hold: { wait: PT5M }
                """
                    .getBytes(StandardCharsets.UTF_8),
                List.of());
    ExecutionQueryRepository laggingProjection =
        new ExecutionQueryRepository() {
          @Override
          public Optional<ExecutionProjection> find(
              TenantId ignoredTenant, ExecutionId ignoredExecution) {
            throw new AssertionError("ordered journal events must reuse Started coordinates");
          }

          @Override
          public ExecutionPage search(ExecutionSearch ignored) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry> history(
              TenantId ignoredTenant, ExecutionId ignoredExecution, long afterSequence, int limit) {
            throw new UnsupportedOperationException();
          }
        };
    var handler =
        new CloudEventOutboxHandler(
            (operationId, event) -> {
              published.add(event);
              return CompletableFuture.completedFuture(null);
            },
            (ignoredExecution, ignoredOperation) ->
                CompletableFuture.completedFuture(
                    new WorkflowReply.Accepted(
                        UUID.randomUUID(), executionId, 2, ExecutionStatus.WAITING)),
            laggingProjection);
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:lifecycle-start");
    var empty = JsonNodeFactory.instance.objectNode();

    handler
        .process(
            envelope(
                executionId,
                1,
                new EngineEvent.Started(UUID.randomUUID(), executionId, actor, plan, empty, AT)))
        .toCompletableFuture()
        .join();
    handler
        .process(
            envelope(
                executionId,
                2,
                new EngineEvent.WaitScheduled(
                    UUID.randomUUID(), "/do/0/hold", empty, empty, 1, AT.plusSeconds(300), AT)))
        .toCompletableFuture()
        .join();

    assertEquals(
        List.of(
            "io.serverlessworkflow.workflow.started.v1",
            "io.serverlessworkflow.task.created.v1",
            "io.serverlessworkflow.task.started.v1"),
        published.stream().map(WorkflowCloudEvent::type).toList());
  }

  @Test
  void publishesWorkflowCancellationBeforeItsActiveTaskNotificationsAfterRestart() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:lifecycle-cancel");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var published = new ArrayList<WorkflowCloudEvent>();

    // A fresh handler has no in-memory workflow history. The query supplies only
    // immutable definition identity; the journal event owns the exact task snapshot.
    var restarted = lifecycleHandler(executionId, published);
    restarted
        .process(
            envelope(
                executionId,
                7,
                new EngineEvent.Cancelled(UUID.randomUUID(), List.of("/do/0/call-provider"), AT)))
        .toCompletableFuture()
        .join();

    assertEquals(
        List.of(
            "io.serverlessworkflow.workflow.cancelled.v1",
            "io.serverlessworkflow.task.cancelled.v1"),
        published.stream().map(WorkflowCloudEvent::type).toList());
    assertEquals(tenant.toString(), published.getFirst().extensions().get("tenant").asText());
  }

  @Test
  void publishesEmitTaskLifecycleBeforeTheUserEvent() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:lifecycle-emit");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var published = new ArrayList<WorkflowCloudEvent>();
    var handler = lifecycleHandler(executionId, published);
    var userEvent =
        new WorkflowCloudEvent(
            "1.0",
            "emit-operation",
            URI.create("urn:orders"),
            "orders.submitted",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("order", 42),
            Map.of());

    handler
        .process(
            envelope(
                executionId,
                3,
                new EngineEvent.EmitRequested(
                    UUID.randomUUID(),
                    "/do/0/submit",
                    JsonNodeFactory.instance.objectNode(),
                    JsonNodeFactory.instance.objectNode(),
                    1,
                    "emit-operation",
                    userEvent,
                    AT)))
        .toCompletableFuture()
        .join();

    assertEquals(
        List.of(
            "io.serverlessworkflow.task.created.v1",
            "io.serverlessworkflow.task.started.v1",
            "orders.submitted"),
        published.stream().map(WorkflowCloudEvent::type).toList());
    assertEquals(tenant.toString(), published.getLast().extensions().get("tenant").asText());
  }

  private static CloudEventOutboxHandler lifecycleHandler(
      ExecutionId executionId, List<WorkflowCloudEvent> published) {
    var definition = new WorkflowCoordinates("orders", "fulfil", "1.0.0", "1.0.3");
    var revision =
        new DefinitionRevision(
            UUID.randomUUID(), UUID.randomUUID(), definition, "1".repeat(64), "2".repeat(64));
    var view =
        new ExecutionProjection(
            executionId,
            EngineId.PEKKO,
            revision,
            ExecutionLifecycleState.RUNNING,
            7,
            "correlation-1",
            7,
            AT,
            AT,
            null,
            JsonNodeFactory.instance.objectNode(),
            JsonNodeFactory.instance.objectNode(),
            null,
            List.of(),
            List.of());
    ExecutionQueryRepository repository =
        new ExecutionQueryRepository() {
          @Override
          public Optional<ExecutionProjection> find(
              TenantId requestedTenant, ExecutionId requestedExecution) {
            return requestedTenant.equals(executionId.tenantId())
                    && requestedExecution.equals(executionId)
                ? Optional.of(view)
                : Optional.empty();
          }

          @Override
          public ExecutionPage search(ExecutionSearch ignored) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry> history(
              TenantId ignoredTenant, ExecutionId ignoredExecution, long afterSequence, int limit) {
            throw new UnsupportedOperationException();
          }
        };
    return new CloudEventOutboxHandler(
        (operationId, event) -> {
          assertEquals(event.id(), operationId);
          published.add(event);
          return CompletableFuture.completedFuture(null);
        },
        (ignoredExecution, ignoredOperation) ->
            CompletableFuture.completedFuture(
                new WorkflowReply.Accepted(
                    UUID.randomUUID(), executionId, 8, ExecutionStatus.WAITING)),
        repository);
  }

  private static EventEnvelope<EngineEvent> envelope(
      ExecutionId executionId, long sequence, EngineEvent event) {
    return EventEnvelope.create(
        Sequence.apply(sequence),
        CloudEventOutboxHandler.PERSISTENCE_ID_PREFIX + executionId.entityId(),
        sequence,
        event,
        event.occurredAt().toEpochMilli());
  }
}
