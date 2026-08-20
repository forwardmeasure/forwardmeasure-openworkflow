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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class LifecycleCloudEventMapperTest {
  private static final Instant AT = Instant.parse("2026-08-16T12:00:00Z");
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:lifecycle");
  private static final ExecutionId EXECUTION =
      new ExecutionId(TENANT, UUID.fromString("11111111-2222-3333-4444-555555555555"));
  private static final WorkflowCoordinates COORDINATES =
      new WorkflowCoordinates("orders", "fulfil", "1.2.3", "1.0.3");

  @Test
  void mapsExactRequiredWorkflowAndTaskLifecycleShapes() {
    var mapper = new LifecycleCloudEventMapper();
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: orders
                  name: fulfil
                  version: '1.2.3'
                do:
                  - initialize: { set: { accepted: true } }
                """
                    .getBytes(StandardCharsets.UTF_8),
                List.of());
    UUID command = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String name = "fulfil-11111111-2222-3333-4444-555555555555.orders";

    var started =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.Started(
                command,
                EXECUTION,
                new ActorIdentity(TENANT, "did:web:forwardmeasure.com:actor:test"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                AT));
    assertEquals("io.serverlessworkflow.workflow.started.v1", started.getFirst().type());
    assertEquals(name, started.getFirst().data().required("name").asText());
    assertEquals(
        "1.2.3", started.getFirst().data().required("definition").required("version").asText());

    var entered =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.TaskEntered(
                command,
                "/do/0/initialize",
                JsonNodeFactory.instance.objectNode(),
                JsonNodeFactory.instance.objectNode(),
                0,
                AT));
    assertEquals(
        List.of("io.serverlessworkflow.task.created.v1", "io.serverlessworkflow.task.started.v1"),
        entered.stream().map(event -> event.type()).toList());
    assertEquals("/do/0/initialize", entered.getFirst().data().required("task").asText());

    var paused =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.Paused(command, List.of("/do/0/initialize"), AT));
    assertEquals("io.serverlessworkflow.workflow.suspended.v1", paused.getFirst().type());
    assertEquals("io.serverlessworkflow.task.suspended.v1", paused.get(1).type());

    var cancelled =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.Cancelled(command, List.of("/do/0/initialize"), AT));
    assertEquals("io.serverlessworkflow.workflow.cancelled.v1", cancelled.getFirst().type());
    assertEquals("io.serverlessworkflow.task.cancelled.v1", cancelled.get(1).type());
    assertEquals(TENANT.value(), cancelled.getFirst().extensions().get("tenant").asText());
  }

  @Test
  void mapsCorrelationBoundariesAndNeverEmbedsCompletionOutput() {
    var mapper = new LifecycleCloudEventMapper();
    UUID command = UUID.fromString("bbbbbbbb-cccc-dddd-eeee-ffffffffffff");
    var input = JsonNodeFactory.instance.objectNode().put("order", 42);
    var started =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.ListenStarted(
                command,
                "/do/0/await-order",
                input,
                input,
                1,
                "listen-1",
                java.util.Set.of("orders.accepted"),
                AT));
    assertEquals(
        "io.serverlessworkflow.workflow.correlation-started.v1", started.getFirst().type());
    assertEquals(AT.toString(), started.getFirst().data().required("startedAt").asText());

    var acceptedEvent =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "accepted-1",
            java.net.URI.create("urn:orders"),
            "orders.accepted",
            null,
            AT,
            "application/json",
            input,
            Map.of());
    var completed =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.ListenEventAccepted(
                command,
                "/do/0/await-order",
                "listen-1",
                acceptedEvent,
                List.of(acceptedEvent),
                Map.of("orderId", JsonNodeFactory.instance.textNode("order-42")),
                java.util.Set.of(0),
                true,
                input,
                input,
                2,
                AT));
    assertEquals(
        "io.serverlessworkflow.workflow.correlation-completed.v1", completed.getFirst().type());
    assertEquals(
        "order-42",
        completed.getFirst().data().required("correlationKeys").required("orderId").asText());

    var workflowCompleted =
        mapper.map(EXECUTION, COORDINATES, new EngineEvent.Completed(command, input, AT));
    assertEquals(false, workflowCompleted.getFirst().data().has("output"));
    assertEquals(false, workflowCompleted.getFirst().data().has("outputUri"));

    var taskCompleted =
        mapper.map(
            EXECUTION,
            COORDINATES,
            new EngineEvent.TaskCompleted(command, "/do/1/finalize", 2, input, input, AT));
    assertEquals(false, taskCompleted.getFirst().data().has("output"));
    assertEquals(false, taskCompleted.getFirst().data().has("outputUri"));
  }
}
