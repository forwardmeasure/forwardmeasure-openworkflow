package com.forwardmeasure.openworkflow.operation.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.OperationTimeouts;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.pekko.Done;

/** MCP newline-delimited JSON-RPC stdio transport with owned-process cancellation. */
public final class McpStdioOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern ENVIRONMENT_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private final Duration timeout;
  private final Clock clock;
  private final McpStdioCommandPolicy commands;
  private final SecretProvider secrets;

  public McpStdioOperationExecutor(
      Duration timeout, McpStdioCommandPolicy commands, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), commands, secrets);
  }

  McpStdioOperationExecutor(
      Duration timeout, Clock clock, McpStdioCommandPolicy commands, SecretProvider secrets) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.commands = Objects.requireNonNull(commands, "commands");
    this.secrets = Objects.requireNonNull(secrets, "secrets");
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException("MCP stdio timeout must be positive");
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.MCP
        || !operation.protocol().equals("mcp-stdio")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("MCP stdio driver received an incompatible operation"));
    }
    JsonNode stdio = operation.request().path("transport").path("stdio");
    String command = stdio.path("command").asText();
    try {
      commands.authorize(executionId.tenantId(), command);
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var result = new CompletableFuture<Done>();
    Duration operationTimeout =
        OperationTimeouts.configuredOrMaximum(operation.request().path("timeout"), timeout);
    var active = new AtomicReference<Process>();
    result.whenComplete(
        (done, failure) -> {
          if (result.isCancelled()) terminate(active.get());
        });
    Thread.ofVirtual()
        .name("openworkflow-mcp-stdio-" + operation.operationId())
        .start(() -> run(executionId, operation, sink, stdio, active, result));
    CompletableFuture.runAsync(
        () -> {
          if (!result.isDone()) terminate(active.get());
        },
        CompletableFuture.delayedExecutor(operationTimeout.toMillis(), TimeUnit.MILLISECONDS));
    return result;
  }

  private void run(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      JsonNode stdio,
      AtomicReference<Process> active,
      CompletableFuture<Done> result) {
    Process process = null;
    var stderr = new StringBuilder();
    try {
      var invocation = new ArrayList<String>();
      invocation.add(stdio.required("command").asText());
      stdio.path("arguments").forEach(argument -> invocation.add(argument.asText()));
      ProcessBuilder builder = new ProcessBuilder(invocation);
      builder.environment().putAll(environment(executionId, operation, stdio));
      process = builder.start();
      active.set(process);
      Process ownedProcess = process;
      Thread.ofVirtual()
          .name("openworkflow-mcp-stderr-" + operation.operationId())
          .start(() -> drainError(ownedProcess, stderr));
      try (var writer =
              new BufferedWriter(
                  new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
          var reader =
              new BufferedReader(
                  new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        write(
            writer,
            rpc(
                "initialize",
                initializeParameters(operation),
                operation.operationId() + "-initialize"));
        Duration operationTimeout =
            OperationTimeouts.configuredOrMaximum(operation.request().path("timeout"), timeout);
        JsonNode initialized =
            response(reader, operation.operationId() + "-initialize", operationTimeout);
        if (initialized.has("error"))
          throw new IllegalStateException("MCP initialize failed: " + initialized.path("error"));
        write(writer, notification("notifications/initialized"));
        write(
            writer,
            rpc(
                operation.operation(),
                operation.request().path("parameters"),
                operation.operationId()));
        JsonNode envelope = response(reader, operation.operationId(), operationTimeout);
        boolean failed = envelope.has("error");
        JsonNode value = failed ? problem(envelope.path("error")) : envelope.path("result");
        sink.observe("response-0", value, failed, true, clock.instant())
            .toCompletableFuture()
            .join();
        result.complete(Done.getInstance());
      }
    } catch (Exception failure) {
      if (!result.isDone()) {
        ObjectNode problem = problem(failure, stderr.toString());
        sink.observe("transport-failure", problem, true, true, clock.instant())
            .whenComplete(
                (ignored, observationFailure) -> {
                  if (observationFailure == null) result.complete(Done.getInstance());
                  else result.completeExceptionally(observationFailure);
                });
      }
    } finally {
      terminate(process);
      active.compareAndSet(process, null);
    }
  }

  private Map<String, String> environment(
      ExecutionId executionId, ProtocolOperationDescriptor operation, JsonNode stdio)
      throws Exception {
    Map<String, String> values = new HashMap<>();
    stdio
        .path("environment")
        .properties()
        .forEach(entry -> putEnvironment(values, entry.getKey(), entry.getValue().asText()));
    JsonNode options = operation.request().path("transport").path("options");
    String secretName = options.path("environmentSecret").asText();
    if (!secretName.isBlank()) {
      char[] secret = secrets.resolve(executionId.tenantId(), secretName);
      try {
        JsonNode configured = JSON.readTree(new String(secret));
        if (!configured.isObject())
          throw new IllegalArgumentException("MCP environment secret must contain a JSON object");
        configured
            .properties()
            .forEach(
                entry -> {
                  if (!entry.getValue().isTextual())
                    throw new IllegalArgumentException(
                        "MCP environment secret values must be strings");
                  putEnvironment(values, entry.getKey(), entry.getValue().asText());
                });
      } finally {
        java.util.Arrays.fill(secret, '\0');
      }
    }
    return Map.copyOf(values);
  }

  private static void putEnvironment(Map<String, String> values, String name, String value) {
    if (!ENVIRONMENT_NAME.matcher(name).matches())
      throw new IllegalArgumentException("Invalid MCP environment variable name: " + name);
    values.put(name, value);
  }

  private JsonNode response(BufferedReader reader, String expectedId, Duration operationTimeout)
      throws Exception {
    long deadline = System.nanoTime() + operationTimeout.toNanos();
    while (System.nanoTime() < deadline) {
      String line = reader.readLine();
      if (line == null)
        throw new IllegalStateException("MCP stdio server closed before response " + expectedId);
      if (line.isBlank()) continue;
      JsonNode message = JSON.readTree(line);
      if (expectedId.equals(message.path("id").asText())) return message;
    }
    throw new java.util.concurrent.TimeoutException("MCP stdio response timed out: " + expectedId);
  }

  private static ObjectNode initializeParameters(ProtocolOperationDescriptor operation) {
    ObjectNode parameters =
        JsonNodeFactory.instance
            .objectNode()
            .put(
                "protocolVersion",
                operation.request().path("protocolVersion").asText("2025-06-18"));
    parameters.set("capabilities", JsonNodeFactory.instance.objectNode());
    JsonNode client = operation.request().path("client");
    parameters.set(
        "clientInfo",
        client.isObject()
            ? client.deepCopy()
            : JsonNodeFactory.instance
                .objectNode()
                .put("name", "openworkflow-actor-engine")
                .put("version", "1.0.0"));
    return parameters;
  }

  private static ObjectNode rpc(String method, JsonNode parameters, String id) {
    ObjectNode request =
        JsonNodeFactory.instance
            .objectNode()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("method", method);
    request.set(
        "params",
        parameters.isMissingNode() ? JsonNodeFactory.instance.objectNode() : parameters.deepCopy());
    return request;
  }

  private static ObjectNode notification(String method) {
    return JsonNodeFactory.instance.objectNode().put("jsonrpc", "2.0").put("method", method);
  }

  private static void write(BufferedWriter writer, JsonNode value) throws Exception {
    writer.write(value.toString());
    writer.newLine();
    writer.flush();
  }

  private static ObjectNode problem(JsonNode error) {
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:mcp:error")
        .put("status", 502)
        .put("title", "MCP operation failed")
        .put("detail", error.path("message").asText("Remote MCP error"))
        .set("extension", error.deepCopy());
  }

  private static ObjectNode problem(Throwable failure, String stderr) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    ObjectNode problem =
        JsonNodeFactory.instance
            .objectNode()
            .put("type", "urn:openworkflow:mcp:stdio-transport")
            .put("status", 502)
            .put("title", "MCP stdio transport failed")
            .put(
                "detail",
                root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage());
    if (!stderr.isBlank()) problem.put("stderr", stderr);
    return problem;
  }

  private static void drainError(Process process, StringBuilder output) {
    try (var reader =
        new BufferedReader(
            new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
      int character;
      while (output.length() < 8192 && (character = reader.read()) >= 0) {
        output.append((char) character);
      }
    } catch (Exception ignored) {
    }
  }

  private static void terminate(Process process) {
    if (process == null || !process.isAlive()) return;
    process.destroy();
    try {
      if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
    }
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
