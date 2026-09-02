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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationMaterializer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class ProtocolOperationMaterializerTest {
  @Test
  void materializesShellScriptAndContainerAsCredentialFreeDurableIntents() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: durable-runs
          version: '1.0.0'
        do:
          - shell:
              run:
                await: true
                return: all
                shell:
                  command: printf
                  arguments: ['%s', '${ .message }']
          - script:
              run:
                script:
                  language: python
                  source: {endpoint: https://scripts.example.test/job.py}
          - container:
              run:
                await: false
                return: none
                container:
                  image: registry.example.test/jobs/evidence@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
                  command: process
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(request.uri(), "text/x-python", "print('pinned')"));
    var plan = new OpenWorkflowCompiler().compile(source, resources);
    var shellArguments = plan.steps().get(0).runPlan().configuration();
    ((com.fasterxml.jackson.databind.node.ArrayNode) shellArguments.path("arguments"))
        .set(1, JsonNodeFactory.instance.textNode("hello"));
    assertEquals(
        "${ .message }",
        plan.steps().get(0).runPlan().configuration().required("arguments").get(1).asText());

    ProtocolOperationDescriptor shell =
        ProtocolOperationMaterializer.materialize(
            plan, plan.steps().get(0), shellArguments, "run-shell-1", null);
    ProtocolOperationDescriptor script =
        ProtocolOperationMaterializer.materialize(
            plan,
            plan.steps().get(1),
            plan.steps().get(1).runPlan().configuration(),
            "run-script-1",
            null);
    ProtocolOperationDescriptor container =
        ProtocolOperationMaterializer.materialize(
            plan,
            plan.steps().get(2),
            plan.steps().get(2).runPlan().configuration(),
            "run-container-1",
            null);

    assertEquals(ProtocolOperationDescriptor.Kind.RUN, shell.kind());
    assertEquals(ProtocolOperationDescriptor.Mode.RUN_AWAIT, shell.mode());
    assertEquals("run-shell", shell.protocol());
    assertEquals(
        "hello", shell.request().required("configuration").required("arguments").get(1).asText());
    assertEquals("all", shell.request().required("return").asText());
    assertEquals(resources.getFirst().sha256(), script.document().sha256());
    assertEquals("print('pinned')", script.protocolSchema());
    assertEquals(ProtocolOperationDescriptor.Mode.RUN_DETACHED, container.mode());
    assertEquals("run-container", container.protocol());
  }

  @Test
  void materializesA2aStreamingJsonRpcIntent() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: a2a-stream
          version: '1.0.0'
        do:
          - delegate:
              call: a2a
              with:
                server: https://agent.example.test/rpc
                method: message/stream
                parameters:
                  message:
                    messageId: ev-42
        """
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source, java.util.List.of());
    var step = plan.steps().getFirst();

    ProtocolOperationDescriptor descriptor =
        ProtocolOperationMaterializer.materialize(
            plan, step, step.callPlan().arguments(), "a2a-1", null);

    assertEquals(ProtocolOperationDescriptor.Kind.A2A, descriptor.kind());
    assertEquals(ProtocolOperationDescriptor.Mode.RPC_STREAM, descriptor.mode());
    assertEquals("a2a-jsonrpc", descriptor.protocol());
    assertEquals("https://agent.example.test/rpc", descriptor.endpoint().toString());
    assertEquals("message/stream", descriptor.operation());
    assertEquals("ev-42", descriptor.request().required("message").required("messageId").asText());
    assertEquals("user", descriptor.request().required("message").required("role").asText());

    var defaultArguments =
        JsonNodeFactory.instance
            .objectNode()
            .put("server", "https://agent.example.test/rpc")
            .put("method", "message/stream");
    defaultArguments.putObject("parameters").putObject("message");
    var second =
        ProtocolOperationMaterializer.materialize(
            plan, step, defaultArguments, "a2a-defaults", null);
    assertEquals("user", second.request().required("message").required("role").asText());
    assertEquals(
        java.util
            .UUID
            .nameUUIDFromBytes("a2a-defaults:a2a-message".getBytes(StandardCharsets.UTF_8))
            .toString(),
        second.request().required("message").required("messageId").asText());
  }

  @Test
  void pinsAgentCardContentIntoTheDurableA2aIntent() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: pinned-agent
          version: '1.0.0'
        do:
          - delegate:
              call: a2a
              with:
                agentCard:
                  endpoint: https://agent.example.test/.well-known/agent-card.json
                method: tasks/get
                parameters: { id: task-42 }
        """
            .getBytes(StandardCharsets.UTF_8);
    String card =
        """
        {"name":"agent","url":"https://agent.example.test/rpc",
         "securitySchemes":{"bearer":{"httpAuthSecurityScheme":{"scheme":"Bearer"}}},
         "security":[{"bearer":[]}]}
        """;
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request -> ResolvedWorkflowResource.of(request.uri(), "application/json", card));
    var plan = new OpenWorkflowCompiler().compile(source, resources);

    ProtocolOperationDescriptor descriptor =
        ProtocolOperationMaterializer.materialize(
            plan,
            plan.steps().getFirst(),
            plan.steps().getFirst().callPlan().arguments(),
            "a2a-card-1",
            null);

    assertEquals("https://agent.example.test/rpc", descriptor.endpoint().toString());
    assertEquals(resources.getFirst().sha256(), descriptor.document().sha256());
    assertEquals(card, descriptor.protocolSchema());
  }

  @Test
  void materializesMcpHttpAndStdioIntentsWithoutCredentials() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: mcp-transports
          version: '1.0.0'
        do:
          - remote:
              call: mcp
              with:
                protocolVersion: '2025-06-18'
                method: tools/call
                parameters: {name: extract, arguments: {id: ev-42}}
                transport:
                  http: {endpoint: https://mcp.example.test/rpc}
                client: {name: openworkflow, version: 1.0.0}
          - local:
              call: mcp
              with:
                method: resources/list
                transport:
                  stdio: {command: approved-mcp, arguments: ['--safe']}
        """
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source, java.util.List.of());

    ProtocolOperationDescriptor http =
        ProtocolOperationMaterializer.materialize(
            plan,
            plan.steps().get(0),
            plan.steps().get(0).callPlan().arguments(),
            "mcp-http-1",
            null);
    ProtocolOperationDescriptor stdio =
        ProtocolOperationMaterializer.materialize(
            plan,
            plan.steps().get(1),
            plan.steps().get(1).callPlan().arguments(),
            "mcp-stdio-1",
            null);

    assertEquals(ProtocolOperationDescriptor.Kind.MCP, http.kind());
    assertEquals("mcp-http", http.protocol());
    assertEquals("https://mcp.example.test/rpc", http.endpoint().toString());
    assertEquals("tools/call", http.operation());
    assertEquals("extract", http.request().required("parameters").required("name").asText());
    assertEquals("mcp-stdio", stdio.protocol());
    assertEquals(
        "approved-mcp",
        stdio.request().required("transport").required("stdio").required("command").asText());
  }

  @Test
  void materializesPinnedGrpcEndpointRequestAndStreamingMode() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: grpc-stream
          version: '1.0.0'
        do:
          - invoke:
              call: grpc
              with:
                proto:
                  endpoint: https://contracts.example.test/evidence.proto
                service:
                  name: evidence.Classifier
                  host: classifier.example.test
                  port: 443
                method: Watch
                arguments:
                  evidenceId: '${ .id }'
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "text/x-protobuf",
                        request.uri().getPath().endsWith("shared.proto")
                            ? """
                            syntax = "proto3";
                            package evidence;
                            message Evidence { string evidence_id = 1; }
                            message Result { string classification = 1; }
                            """
                            : """
                            syntax = "proto3";
                            package evidence;
                            import "shared.proto";
                            service Classifier {
                              rpc Watch (Evidence) returns (stream Result);
                            }
                            """));
    var plan = new OpenWorkflowCompiler().compile(source, resources);
    var step = plan.steps().getFirst();
    var evaluated = step.callPlan().arguments();
    ((com.fasterxml.jackson.databind.node.ObjectNode) evaluated.path("arguments"))
        .put("evidenceId", "ev-42");

    ProtocolOperationDescriptor descriptor =
        ProtocolOperationMaterializer.materialize(plan, step, evaluated, "operation-1", null);

    assertEquals(ProtocolOperationDescriptor.Mode.GRPC_SERVER_STREAM, descriptor.mode());
    assertEquals("grpcs://classifier.example.test:443", descriptor.endpoint().toString());
    assertEquals("evidence.Classifier/Watch", descriptor.operation());
    assertEquals("ev-42", descriptor.request().required("evidenceId").asText());
    assertEquals(resources.getFirst().sha256(), descriptor.document().sha256());
    assertEquals(resources.get(1).content(), descriptor.protocolDependencies().get("shared.proto"));
  }

  @Test
  void materializesCorrelatedWorkerCommandEventsAndCancellationAsIndependentDurableIntents() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: correlated-worker
          version: '1.0.0'
        do:
          - execute:
              call: com.forwardmeasure.openworkflow.correlated-worker
              with:
                document:
                  endpoint: https://contracts.example.test/workers.yaml
                command:
                  channel: workers.commands
                  message:
                    payload:
                      request: hello
                events:
                  channel: workers.events
                  subscription:
                    consume:
                      until: '${ .payload.status == "SUCCEEDED" }'
                      for: PT30M
                cancellation:
                  channel: workers.cancellations
                  message:
                    payload:
                      reason: abandoned
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 2.6.0
                        info: {title: Workers, version: 1.0.0}
                        servers:
                          test: {url: kafka.test:9092, protocol: kafka}
                        channels:
                          workers.commands:
                            servers: [test]
                            publish: {message: {name: WorkerCommand}}
                          workers.events:
                            servers: [test]
                            subscribe: {message: {name: WorkerEvent}}
                          workers.cancellations:
                            servers: [test]
                            publish: {message: {name: WorkerCancellation}}
                        """));
    var plan = new OpenWorkflowCompiler().compile(source, resources);
    var step = plan.steps().getFirst();
    var deadline = java.time.Instant.parse("2026-01-01T00:30:00Z");

    ProtocolOperationMaterializer.CorrelatedWorkerOperations operations =
        ProtocolOperationMaterializer.materializeCorrelatedWorker(
            plan, step, step.callPlan().arguments(), "worker-1", null, deadline);

    assertEquals("worker-1", operations.command().operationId());
    assertEquals(ProtocolOperationDescriptor.Kind.ASYNC_API, operations.command().kind());
    assertEquals(ProtocolOperationDescriptor.Mode.PUBLISH, operations.command().mode());
    assertEquals("kafka", operations.command().protocol());
    assertEquals(
        "kafka://kafka.test:9092/workers.commands", operations.command().endpoint().toString());
    assertEquals(
        "hello", operations.command().request().required("payload").required("request").asText());
    assertEquals(
        "worker-1",
        operations.command().request().required("payload").required("operationId").asText());

    assertEquals("worker-1:events", operations.events().operationId());
    assertEquals(ProtocolOperationDescriptor.Mode.SUBSCRIBE, operations.events().mode());
    assertEquals(deadline, operations.events().subscriptionDeadline());

    assertEquals("worker-1:cancel", operations.cancellation().operationId());
    assertEquals(ProtocolOperationDescriptor.Mode.PUBLISH, operations.cancellation().mode());
    assertEquals(
        "abandoned",
        operations.cancellation().request().required("payload").required("reason").asText());
    assertEquals(
        "worker-1",
        operations.cancellation().request().required("payload").required("operationId").asText());
  }

  @Test
  void materializedCorrelatedWorkerHasNoCancellationOperationWhenTheCallDeclaresNone() {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: correlated-worker-no-cancel
          version: '1.0.0'
        do:
          - execute:
              call: com.forwardmeasure.openworkflow.correlated-worker
              with:
                document:
                  endpoint: https://contracts.example.test/workers.yaml
                command:
                  channel: workers.commands
                  message:
                    payload: {}
                events:
                  channel: workers.events
                  subscription:
                    consume:
                      until: '${ .payload.status == "SUCCEEDED" }'
                      for: PT30M
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 2.6.0
                        info: {title: Workers, version: 1.0.0}
                        servers:
                          test: {url: kafka.test:9092, protocol: kafka}
                        channels:
                          workers.commands:
                            servers: [test]
                            publish: {message: {name: WorkerCommand}}
                          workers.events:
                            servers: [test]
                            subscribe: {message: {name: WorkerEvent}}
                        """));
    var plan = new OpenWorkflowCompiler().compile(source, resources);
    var step = plan.steps().getFirst();

    ProtocolOperationMaterializer.CorrelatedWorkerOperations operations =
        ProtocolOperationMaterializer.materializeCorrelatedWorker(
            plan, step, step.callPlan().arguments(), "worker-2", null, null);

    assertEquals(null, operations.cancellation());
  }
}
