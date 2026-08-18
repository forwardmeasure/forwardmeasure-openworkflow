package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

final class AsyncApiHttpOperationExecutorTest {
  @Test
  void publishesTheEvaluatedMessageThroughTheSecuredHttpEdge() {
    var observations = new ArrayList<ProtocolOperationObservation>();
    var execution =
        new ExecutionId(
            new TenantId("did:web:forwardmeasure.com:tenant:async-http"), UUID.randomUUID());
    var driver =
        new AsyncApiHttpOperationExecutor(
            (routed, request) -> {
              assertEquals(execution, routed);
              assertEquals("POST", request.method());
              assertEquals("https://events.example.test/evidence", request.uri().toString());
              assertEquals("trace-1", request.headers().get("X-Trace"));
              assertEquals(42, request.body().required("value").asInt());
              return CompletableFuture.completedFuture(
                  HttpOperationResult.success(
                      JsonNodeFactory.instance.objectNode().put("accepted", true)));
            },
            Clock.fixed(Instant.parse("2026-08-15T12:00:00Z"), ZoneOffset.UTC));

    driver
        .execute(
            execution,
            operation(),
            (observationId, value, failed, terminal, observedAt) -> {
              observations.add(
                  new ProtocolOperationObservation(
                      observationId, value, failed, terminal, observedAt));
              return CompletableFuture.completedFuture(
                  ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(1, observations.size());
    assertEquals("response", observations.getFirst().observationId());
    assertTrue(observations.getFirst().terminal());
    assertTrue(observations.getFirst().value().required("accepted").asBoolean());
  }

  private static ProtocolOperationDescriptor operation() {
    var message = JsonNodeFactory.instance.objectNode();
    message.putObject("headers").put("X-Trace", "trace-1");
    message.putObject("payload").put("value", 42);
    return new ProtocolOperationDescriptor(
        "async-http-1",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        ProtocolOperationDescriptor.Mode.PUBLISH,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "d".repeat(64)),
        "https",
        URI.create("https://events.example.test/evidence"),
        "publishEvidence",
        message,
        null,
        null,
        null);
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
