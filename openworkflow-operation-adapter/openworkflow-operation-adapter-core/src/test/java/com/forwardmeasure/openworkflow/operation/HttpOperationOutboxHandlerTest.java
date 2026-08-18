package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pekko.persistence.query.Sequence;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.junit.jupiter.api.Test;

final class HttpOperationOutboxHandlerTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void dispatchesThenWaitsForPersistConfirmedActorObservation() {
    var executionId =
        new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:a"), UUID.randomUUID());
    var calls = new ArrayList<String>();
    var descriptor = descriptor("operation-1");
    var handler =
        new HttpOperationOutboxHandler(
            (routed, operation) -> {
              calls.add("execute:" + operation.operationId());
              assertEquals(executionId, routed);
              return CompletableFuture.completedFuture(
                  HttpOperationResult.success(
                      JsonNodeFactory.instance.objectNode().put("accepted", true)));
            },
            (routed, operationId, result) -> {
              calls.add("observe:" + operationId);
              assertEquals(executionId, routed);
              return CompletableFuture.completedFuture(
                  new WorkflowReply.Accepted(
                      UUID.randomUUID(), executionId, 3, ExecutionStatus.COMPLETED));
            });
    handler
        .process(
            envelope(
                executionId,
                new EngineEvent.HttpCallRequested(
                    UUID.randomUUID(),
                    "/invoke",
                    JsonNodeFactory.instance.objectNode(),
                    JsonNodeFactory.instance.objectNode(),
                    0,
                    descriptor,
                    AT)))
        .toCompletableFuture()
        .join();
    assertEquals(java.util.List.of("execute:operation-1", "observe:operation-1"), calls);
  }

  @Test
  void rejectedObservationKeepsProjectionOffsetRetryable() {
    var executionId =
        new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:a"), UUID.randomUUID());
    var descriptor = descriptor("operation-2");
    var handler =
        new HttpOperationOutboxHandler(
            (ignored, operation) ->
                CompletableFuture.completedFuture(
                    HttpOperationResult.success(JsonNodeFactory.instance.nullNode())),
            (ignored, operationId, result) ->
                CompletableFuture.completedFuture(
                    new WorkflowReply.Rejected(
                        null,
                        executionId,
                        2,
                        ExecutionStatus.PAUSED,
                        "execution_paused",
                        "retry")));
    assertThrows(
        CompletionException.class,
        () ->
            handler
                .process(
                    envelope(
                        executionId,
                        new EngineEvent.ForkBranchHttpCallRequested(
                            UUID.randomUUID(),
                            "/parallel",
                            java.util.List.of(0),
                            "/parallel/invoke",
                            JsonNodeFactory.instance.objectNode(),
                            JsonNodeFactory.instance.objectNode(),
                            descriptor,
                            true,
                            AT)))
                .toCompletableFuture()
                .join());
  }

  private static HttpOperationDescriptor descriptor(String id) {
    return new HttpOperationDescriptor(
        id,
        HttpOperationDescriptor.Kind.HTTP,
        "GET",
        URI.create("https://api.example.test/items"),
        Map.of(),
        JsonNodeFactory.instance.nullNode(),
        HttpOperationDescriptor.Output.CONTENT,
        false,
        null,
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
