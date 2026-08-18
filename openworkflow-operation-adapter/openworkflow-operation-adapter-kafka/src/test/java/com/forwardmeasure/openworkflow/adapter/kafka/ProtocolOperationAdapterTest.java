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
package com.forwardmeasure.openworkflow.adapter.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.Done;
import org.junit.jupiter.api.Test;

class ProtocolOperationAdapterTest {
  private static final String TENANT = "134b09a7-1c36-4b89-86e7-a28c88bc5cef";

  @Test
  void routesDurableProtocolIntentAndBridgesProgressAndTerminalObservation() throws Exception {
    ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    var durable =
        new ProtocolOperationDescriptor(
            "operation-1",
            ProtocolOperationDescriptor.Kind.MCP,
            ProtocolOperationDescriptor.Mode.RPC_UNARY,
            null,
            "mcp-http",
            URI.create("https://mcp.example.test/rpc"),
            "tools/list",
            JsonNodeFactory.instance.objectNode(),
            null,
            null,
            null);
    OperationRequest request = request(json, durable);
    var progress = new ArrayList<OperationObservation>();
    var adapter =
        new ProtocolOperationAdapter(
            json,
            OperationDataReferenceFactory.boundedInline(),
            (execution, operation, sink) -> {
              assertNotNull(operation.requestedBy());
              assertEquals("organization-1", operation.requestedBy().organizationId());
              sink.observe(
                  "progress-1",
                  JsonNodeFactory.instance.objectNode().put("phase", "working"),
                  false,
                  false,
                  Instant.parse("2026-01-01T00:00:00Z"));
              sink.observe(
                  "terminal-1",
                  JsonNodeFactory.instance.objectNode().put("tools", 3),
                  false,
                  true,
                  Instant.parse("2026-01-01T00:00:01Z"));
              return CompletableFuture.completedFuture(Done.getInstance());
            });

    OperationObservation terminal =
        adapter.execute(request, progress::add).toCompletableFuture().join();

    assertEquals(OperationObservationStatus.PROGRESS, progress.getFirst().status());
    assertEquals(
        "working", progress.getFirst().metadata().inlineValue().required("phase").textValue());
    assertEquals(OperationObservationStatus.SUCCEEDED, terminal.status());
    assertEquals(3, terminal.output().inlineValue().required("tools").intValue());
  }

  private static OperationRequest request(ObjectMapper json, ProtocolOperationDescriptor protocol) {
    OksTenantId tenant = OksTenantId.parse("did:forwardmeasure:tenant:" + TENANT);
    ExecutionKey key = new ExecutionKey(tenant, new WorkflowExecutionId("execution-1"));
    var descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("executionKey", key.canonical());
    descriptor.put("taskPath", "invoke");
    descriptor.put("operationId", "operation-1");
    descriptor.put("operationKind", "call");
    descriptor.put("definitionReference", "definition-1");
    descriptor.put("callKind", "MCP");
    descriptor.set("protocolOperation", json.valueToTree(protocol));
    ActorContext actor =
        new ActorContext(
            tenant,
            ActorId.parse("did:forwardmeasure:actor:actor-1"),
            ActorType.HUMAN,
            "Operator",
            null,
            new BusinessCorrelationId("correlation-1"),
            Set.of("workflow-operator"),
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            "organization-1");
    return new OperationRequest(
        "operation-1",
        "call",
        "definition-1",
        descriptor,
        null,
        actor,
        "effect-1",
        actor.authenticatedAt(),
        null,
        Map.of(),
        List.of());
  }
}
