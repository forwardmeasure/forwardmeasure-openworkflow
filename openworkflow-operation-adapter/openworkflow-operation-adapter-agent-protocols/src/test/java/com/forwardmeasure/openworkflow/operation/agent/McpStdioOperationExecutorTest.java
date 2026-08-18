package com.forwardmeasure.openworkflow.operation.agent;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationObservation;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class McpStdioOperationExecutorTest {
  @Test
  void commandPolicyIsTenantQualifiedAndDenyByDefault() {
    var allowed = new TenantId("did:web:forwardmeasure.com:tenant:allowed");
    var other = new TenantId("did:web:forwardmeasure.com:tenant:other");
    var policy = McpStdioCommandPolicy.configured(allowed.value() + "=/opt/mcp-safe|mcp-readonly");

    assertDoesNotThrow(() -> policy.authorize(allowed, "/opt/mcp-safe"));
    assertThrows(SecurityException.class, () -> policy.authorize(other, "/opt/mcp-safe"));
    assertThrows(
        SecurityException.class,
        () -> McpStdioCommandPolicy.configured("").authorize(allowed, "/opt/mcp-safe"));
  }

  @Test
  void initializesCallsAndResolvesTenantEnvironmentOnlyAtProcessEdge() {
    String script =
        """
        read initialize
        printf '%s\\n' '{"jsonrpc":"2.0","id":"mcp-stdio-1-initialize","result":{"protocolVersion":"2025-06-18","capabilities":{}}}'
        read initialized
        read call
        printf '{"jsonrpc":"2.0","id":"mcp-stdio-1","result":{"token":"%s"}}\\n' "$MCP_TOKEN"
        """;
    var observations = new ArrayList<ProtocolOperationObservation>();
    var executor =
        new McpStdioOperationExecutor(
            Duration.ofSeconds(3),
            McpStdioCommandPolicy.allowlisted(Set.of("/bin/sh")),
            (tenant, name) -> "{\"MCP_TOKEN\":\"edge-only\"}".toCharArray());

    executor
        .execute(
            execution(),
            descriptor("mcp-stdio-1", script, true),
            (id, value, failed, terminal, at) -> {
              observations.add(new ProtocolOperationObservation(id, value, failed, terminal, at));
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                      .ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(1, observations.size());
    assertEquals("edge-only", observations.getFirst().value().required("token").asText());
    assertTrue(observations.getFirst().terminal());
    assertTrue(!descriptor("mcp-stdio-1", script, true).toString().contains("edge-only"));
  }

  @Test
  void cancellingOwnedTransportTerminatesTheChildProcess() throws Exception {
    var directory = Files.createTempDirectory("openworkflow-mcp-cancel-");
    var started = directory.resolve("started");
    var terminated = directory.resolve("terminated");
    String script =
        "trap 'echo yes > \""
            + terminated
            + "\"; exit 0' TERM; echo yes > \""
            + started
            + "\"; while true; do sleep 1; done";
    var executor =
        new McpStdioOperationExecutor(
            Duration.ofSeconds(30),
            McpStdioCommandPolicy.allowlisted(Set.of("/bin/sh")),
            (tenant, name) -> {
              throw new AssertionError("secret not expected");
            });
    CompletableFuture<org.apache.pekko.Done> transport =
        executor
            .execute(
                execution(),
                descriptor("mcp-cancel-1", script, false),
                (id, value, failed, terminal, at) ->
                    CompletableFuture.completedFuture(
                        com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                            .ObservationDisposition.CONTINUE))
            .toCompletableFuture();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!Files.exists(started) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(started));

    assertTrue(transport.cancel(true));
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!Files.exists(terminated) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(terminated));
  }

  private static ProtocolOperationDescriptor descriptor(
      String operationId, String script, boolean environmentSecret) {
    var request = JsonNodeFactory.instance.objectNode().put("protocolVersion", "2025-06-18");
    request.putObject("client").put("name", "openworkflow").put("version", "1.0.0");
    var transport = request.putObject("transport");
    var stdio = transport.putObject("stdio").put("command", "/bin/sh");
    stdio.putArray("arguments").add("-c").add(script);
    if (environmentSecret)
      transport.putObject("options").put("environmentSecret", "mcp-environment");
    request.putObject("parameters").put("name", "extract");
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.MCP,
        ProtocolOperationDescriptor.Mode.RPC_UNARY,
        null,
        "mcp-stdio",
        URI.create("stdio://local"),
        "tools/call",
        request,
        null,
        null,
        null);
  }

  private static ExecutionId execution() {
    return new ExecutionId(
        new TenantId("did:web:forwardmeasure.com:tenant:mcp-stdio"), UUID.randomUUID());
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
