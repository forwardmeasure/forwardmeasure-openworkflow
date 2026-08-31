package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorReply;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pekko.persistence.query.Sequence;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.junit.jupiter.api.Test;

final class ProtocolOperationOutboxHandlerTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void advancesAfterCoordinatorAcceptsDurableHandoffWithoutWaitingForTransport() {
    var executionId = execution();
    var calls = new ArrayList<String>();
    var handler =
        new ProtocolOperationOutboxHandler(
            (routed, operationId) -> {
              calls.add(operationId);
              return CompletableFuture.completedFuture(
                  new ProtocolOperationCoordinatorReply(routed, operationId, true));
            });

    handler
        .process(
            envelope(
                executionId,
                new EngineEvent.ProtocolCallRequested(
                    UUID.randomUUID(),
                    "/receive",
                    JsonNodeFactory.instance.objectNode(),
                    JsonNodeFactory.instance.objectNode(),
                    0,
                    descriptor("operation-1"),
                    AT)))
        .toCompletableFuture()
        .join();

    assertEquals(List.of("operation-1"), calls);
  }

  @Test
  void rejectedHandoffKeepsProjectionOffsetRetryable() {
    var executionId = execution();
    var handler =
        new ProtocolOperationOutboxHandler(
            (routed, operationId) ->
                CompletableFuture.completedFuture(
                    new ProtocolOperationCoordinatorReply(routed, operationId, false)));

    assertThrows(
        CompletionException.class,
        () ->
            handler
                .process(
                    envelope(
                        executionId,
                        new EngineEvent.ForkBranchProtocolCallRequested(
                            UUID.randomUUID(),
                            "/parallel",
                            List.of(0),
                            "/parallel/receive",
                            JsonNodeFactory.instance.objectNode(),
                            JsonNodeFactory.instance.objectNode(),
                            descriptor("operation-2"),
                            true,
                            AT)))
                .toCompletableFuture()
                .join());
  }

  @Test
  void correlatedWorkerRequestedStartsBothItsCommandAndEventsCoordinators() {
    var executionId = execution();
    var calls = new ArrayList<String>();
    var handler =
        new ProtocolOperationOutboxHandler(
            (routed, operationId) -> {
              calls.add(operationId);
              return CompletableFuture.completedFuture(
                  new ProtocolOperationCoordinatorReply(routed, operationId, true));
            });

    handler
        .process(
            envelope(
                executionId,
                new EngineEvent.CorrelatedWorkerRequested(
                    UUID.randomUUID(),
                    "/execute",
                    JsonNodeFactory.instance.objectNode(),
                    JsonNodeFactory.instance.objectNode(),
                    0,
                    "worker-1",
                    descriptor("worker-1"),
                    descriptor("worker-1:events"),
                    descriptor("worker-1:cancel"),
                    AT)))
        .toCompletableFuture()
        .join();

    assertEquals(List.of("worker-1", "worker-1:events"), calls);
  }

  @Test
  void correlatedWorkerCancellationDispatchedStartsOnlyTheCancellationCoordinator() {
    var executionId = execution();
    var calls = new ArrayList<String>();
    var handler =
        new ProtocolOperationOutboxHandler(
            (routed, operationId) -> {
              calls.add(operationId);
              return CompletableFuture.completedFuture(
                  new ProtocolOperationCoordinatorReply(routed, operationId, true));
            });

    handler
        .process(
            envelope(
                executionId,
                new EngineEvent.CorrelatedWorkerCancellationDispatched(
                    UUID.randomUUID(), "/execute", "worker-1", descriptor("worker-1:cancel"), AT)))
        .toCompletableFuture()
        .join();

    assertEquals(List.of("worker-1:cancel"), calls);
  }

  @Test
  void ignoresEventsThatDoNotRequestAProtocolOperation() {
    var handler =
        new ProtocolOperationOutboxHandler(
            (routed, operationId) ->
                CompletableFuture.failedFuture(new AssertionError("not called")));
    var executionId = execution();
    handler
        .process(envelope(executionId, new EngineEvent.Paused(UUID.randomUUID(), AT)))
        .toCompletableFuture()
        .join();
  }

  private static ExecutionId execution() {
    return new ExecutionId(
        new TenantId("did:web:forwardmeasure.com:tenant:protocol"), UUID.randomUUID());
  }

  private static ProtocolOperationDescriptor descriptor(String id) {
    var subscription =
        new AsyncApiSubscriptionPlan(
            null,
            new AsyncApiSubscriptionPlan.Consumption(
                AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT, 2, null, null),
            null,
            null,
            null);
    return new ProtocolOperationDescriptor(
        id,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        ProtocolOperationDescriptor.Mode.SUBSCRIBE,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "a".repeat(64)),
        "mqtt",
        URI.create("mqtt://broker.example.test:1883"),
        "receiveEvidence",
        JsonNodeFactory.instance.objectNode(),
        subscription,
        null,
        null);
  }

  private static EventEnvelope<EngineEvent> envelope(ExecutionId executionId, EngineEvent event) {
    return EventEnvelope.create(
        Sequence.apply(1L),
        HttpOperationOutboxHandler.PERSISTENCE_ID_PREFIX + executionId.entityId(),
        2,
        event,
        AT.toEpochMilli());
  }
}
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
