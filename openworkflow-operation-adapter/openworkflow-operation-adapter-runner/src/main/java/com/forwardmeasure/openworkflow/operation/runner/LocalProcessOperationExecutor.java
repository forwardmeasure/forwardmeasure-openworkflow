package com.forwardmeasure.openworkflow.operation.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.pekko.Done;

/** Exact-argv shell and pinned-source script runner; never invokes a command shell implicitly. */
public final class LocalProcessOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern ENVIRONMENT_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private final Duration timeout;
  private final int maximumOutputBytes;
  private final Clock clock;
  private final RunExecutionPolicy policy;

  public LocalProcessOperationExecutor(
      Duration timeout, int maximumOutputBytes, RunExecutionPolicy policy) {
    this(timeout, maximumOutputBytes, Clock.systemUTC(), policy);
  }

  LocalProcessOperationExecutor(
      Duration timeout, int maximumOutputBytes, Clock clock, RunExecutionPolicy policy) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.policy = Objects.requireNonNull(policy, "policy");
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException("Run timeout must be positive");
    if (maximumOutputBytes < 1)
      throw new IllegalArgumentException("maximumOutputBytes must be positive");
    this.maximumOutputBytes = maximumOutputBytes;
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.RUN
        || !(operation.protocol().equals("run-shell")
            || operation.protocol().equals("run-script"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Local runner received an incompatible operation"));
    }
    JsonNode configuration = operation.request().required("configuration");
    try {
      if (operation.protocol().equals("run-shell")) {
        policy.authorizeCommand(executionId.tenantId(), configuration.required("command").asText());
      } else {
        policy.interpreter(executionId.tenantId(), configuration.required("language").asText());
      }
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var result = new CompletableFuture<Done>();
    var process = new AtomicReference<Process>();
    result.whenComplete(
        (done, failure) -> {
          if (result.isCancelled()) terminate(process.get());
        });
    Thread.ofVirtual()
        .name("openworkflow-run-" + operation.operationId())
        .start(() -> run(executionId, operation, configuration, sink, process, result));
    return result;
  }

  private void run(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      JsonNode configuration,
      ObservationSink sink,
      AtomicReference<Process> active,
      CompletableFuture<Done> result) {
    Process process = null;
    Path scriptDirectory = null;
    try {
      var invocation = new ArrayList<String>();
      if (operation.protocol().equals("run-shell")) {
        invocation.add(configuration.required("command").asText());
        configuration.path("arguments").forEach(argument -> invocation.add(argument.asText()));
      } else {
        invocation.add(
            policy.interpreter(
                executionId.tenantId(), configuration.required("language").asText()));
        scriptDirectory = Files.createTempDirectory("openworkflow-script-");
        Path script = scriptDirectory.resolve("source");
        String source =
            operation.protocolSchema() != null
                ? operation.protocolSchema()
                : configuration.required("code").asText();
        Files.writeString(script, source, StandardCharsets.UTF_8);
        invocation.add(script.toString());
      }
      ProcessBuilder builder = new ProcessBuilder(invocation);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.RUN_DETACHED) {
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
      }
      Map<String, String> environment = builder.environment();
      environment.put("OPENWORKFLOW_TENANT", executionId.tenantId().value().toString());
      environment.put("OPENWORKFLOW_EXECUTION_ID", executionId.value().toString());
      environment.put("OPENWORKFLOW_OPERATION_ID", operation.operationId());
      JsonNode configuredEnvironment = configuration.path("environment");
      configuredEnvironment
          .properties()
          .forEach(
              entry -> {
                if (!ENVIRONMENT_NAME.matcher(entry.getKey()).matches()) {
                  throw new IllegalArgumentException(
                      "Invalid run environment variable name: " + entry.getKey());
                }
                environment.put(
                    entry.getKey(),
                    entry.getValue().isTextual()
                        ? entry.getValue().asText()
                        : entry.getValue().toString());
              });
      if (operation.protocol().equals("run-script") && configuration.path("arguments").isObject()) {
        environment.put(
            "OPENWORKFLOW_SCRIPT_ARGUMENTS", configuration.path("arguments").toString());
      }
      process = builder.start();
      active.set(process);
      if (configuration.has("stdin")) {
        process
            .getOutputStream()
            .write(configuration.path("stdin").asText().getBytes(StandardCharsets.UTF_8));
      }
      process.getOutputStream().close();
      if (operation.mode() == ProtocolOperationDescriptor.Mode.RUN_DETACHED) {
        Process detached = process;
        sink.observe("launched", result(operation, "", "", null), false, true, clock.instant())
            .whenComplete((ignored, failure) -> complete(result, failure));
        process = null;
        active.compareAndSet(detached, null);
        return;
      }
      Process owned = process;
      var stdout = readAsync(owned.getInputStream());
      var stderr = readAsync(owned.getErrorStream());
      boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!exited) {
        terminate(process);
        throw new java.util.concurrent.TimeoutException("Run timed out");
      }
      JsonNode output = result(operation, stdout.join(), stderr.join(), process.exitValue());
      sink.observe("completed", output, false, true, clock.instant())
          .whenComplete((ignored, failure) -> complete(result, failure));
    } catch (Exception failure) {
      if (!result.isDone())
        sink.observe("run-failure", problem(failure), true, true, clock.instant())
            .whenComplete((ignored, observationFailure) -> complete(result, observationFailure));
    } finally {
      terminate(process);
      active.compareAndSet(process, null);
      delete(scriptDirectory);
    }
  }

  private JsonNode result(
      ProtocolOperationDescriptor operation, String stdout, String stderr, Integer code) {
    return switch (operation.request().path("return").asText("stdout")) {
      case "stderr" -> JsonNodeFactory.instance.textNode(stderr);
      case "code" ->
          code == null
              ? JsonNodeFactory.instance.nullNode()
              : JsonNodeFactory.instance.numberNode(code);
      case "all" ->
          JsonNodeFactory.instance
              .objectNode()
              .put("stdout", stdout)
              .put("stderr", stderr)
              .set(
                  "code",
                  code == null
                      ? JsonNodeFactory.instance.nullNode()
                      : JsonNodeFactory.instance.numberNode(code));
      case "none" -> JsonNodeFactory.instance.nullNode();
      default -> JsonNodeFactory.instance.textNode(stdout);
    };
  }

  private String readBounded(InputStream input) {
    try (input;
        var output = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[4096];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        if (output.size() + count > maximumOutputBytes)
          throw new IllegalStateException("Run output exceeded " + maximumOutputBytes + " bytes");
        output.write(buffer, 0, count);
      }
      return output.toString(StandardCharsets.UTF_8);
    } catch (java.io.IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private CompletableFuture<String> readAsync(InputStream input) {
    var result = new CompletableFuture<String>();
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                result.complete(readBounded(input));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              }
            });
    return result;
  }

  private static ObjectNode problem(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:run:process")
        .put("status", 500)
        .put("title", "Run process failed")
        .put(
            "detail",
            root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage());
  }

  private static void complete(CompletableFuture<Done> result, Throwable failure) {
    if (failure == null) result.complete(Done.getInstance());
    else result.completeExceptionally(failure);
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

  private static void delete(Path directory) {
    if (directory == null) return;
    try (var paths = Files.walk(directory)) {
      paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
              });
    } catch (Exception ignored) {
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
