package com.forwardmeasure.openworkflow.operation.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.pekko.Done;

/** OCI CLI adapter with exact image policy, bounded output, and owned-container cancellation. */
public final class OciContainerOperationExecutor implements ProtocolOperationExecutor {
  private static final Pattern ENVIRONMENT_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private final String runtime;
  private final Duration timeout;
  private final int maximumOutputBytes;
  private final Clock clock;
  private final RunExecutionPolicy policy;

  public OciContainerOperationExecutor(
      String runtime, Duration timeout, int maximumOutputBytes, RunExecutionPolicy policy) {
    this(runtime, timeout, maximumOutputBytes, Clock.systemUTC(), policy);
  }

  OciContainerOperationExecutor(
      String runtime,
      Duration timeout,
      int maximumOutputBytes,
      Clock clock,
      RunExecutionPolicy policy) {
    this.runtime = requireText(runtime, "runtime");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.policy = Objects.requireNonNull(policy, "policy");
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException("Container timeout must be positive");
    if (maximumOutputBytes < 1)
      throw new IllegalArgumentException("maximumOutputBytes must be positive");
    this.maximumOutputBytes = maximumOutputBytes;
    Thread.ofVirtual()
        .name("openworkflow-container-cleanup-recovery")
        .start(this::reconcileExpired);
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    if (operation.kind() != ProtocolOperationDescriptor.Kind.RUN
        || !operation.protocol().equals("run-container")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("OCI runner received an incompatible operation"));
    }
    JsonNode configuration = operation.request().required("configuration");
    String image = configuration.required("image").asText();
    try {
      policy.authorizeImage(executionId.tenantId(), image);
      if (!image.contains("@sha256:"))
        throw new SecurityException("Container run image must be pinned by sha256 digest");
      configuration
          .path("volumes")
          .properties()
          .forEach(
              entry ->
                  policy.authorizeVolume(
                      executionId.tenantId(), entry.getKey(), entry.getValue().asText()));
      configuration
          .path("ports")
          .properties()
          .forEach(
              entry ->
                  policy.authorizePort(
                      executionId.tenantId(), entry.getKey(), entry.getValue().asText()));
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var result = new CompletableFuture<Done>();
    var active = new AtomicReference<Process>();
    final String containerName;
    try {
      containerName = containerName(executionId, operation, configuration);
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    result.whenComplete(
        (done, failure) -> {
          if (result.isCancelled()) {
            terminate(active.get());
            stop(containerName);
          }
        });
    Thread.ofVirtual()
        .name("openworkflow-container-" + operation.operationId())
        .start(
            () -> run(executionId, operation, configuration, containerName, sink, active, result));
    return result;
  }

  private void run(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      JsonNode configuration,
      String containerName,
      ObservationSink sink,
      AtomicReference<Process> active,
      CompletableFuture<Done> result) {
    Process process = null;
    try {
      var invocation = new ArrayList<String>();
      invocation.add(runtime);
      invocation.add("run");
      if (operation.mode() == ProtocolOperationDescriptor.Mode.RUN_DETACHED) {
        invocation.add("--detach");
      }
      String cleanup = configuration.path("lifetime").path("cleanup").asText("never");
      Instant cleanupAt = null;
      if (cleanup.equals("always")) {
        invocation.add("--rm");
      } else if (cleanup.equals("eventually")) {
        cleanupAt =
            clock.instant().plus(cleanupAfter(configuration.path("lifetime").required("after")));
        invocation.add("--label");
        invocation.add("io.forwardmeasure.openworkflow.cleanup-at=" + cleanupAt.toEpochMilli());
      } else if (!cleanup.equals("never")) {
        throw new IllegalArgumentException("Unsupported container cleanup policy: " + cleanup);
      }
      invocation.add("--name");
      invocation.add(containerName);
      String pull = configuration.path("pullPolicy").asText("missing");
      invocation.add(
          "--pull="
              + switch (pull) {
                case "always" -> "always";
                case "never" -> "never";
                default -> "missing";
              });
      invocation.add("--env");
      invocation.add("OPENWORKFLOW_TENANT=" + executionId.tenantId().value());
      invocation.add("--env");
      invocation.add("OPENWORKFLOW_EXECUTION_ID=" + executionId.value());
      configuration
          .path("environment")
          .properties()
          .forEach(
              entry -> {
                if (!ENVIRONMENT_NAME.matcher(entry.getKey()).matches()) {
                  throw new IllegalArgumentException(
                      "Invalid container environment variable name: " + entry.getKey());
                }
                invocation.add("--env");
                invocation.add(
                    entry.getKey()
                        + "="
                        + (entry.getValue().isTextual()
                            ? entry.getValue().asText()
                            : entry.getValue().toString()));
              });
      configuration
          .path("volumes")
          .properties()
          .forEach(
              entry -> {
                invocation.add("--volume");
                invocation.add(entry.getKey() + ":" + entry.getValue().asText());
              });
      configuration
          .path("ports")
          .properties()
          .forEach(
              entry -> {
                invocation.add("--publish");
                invocation.add(entry.getKey() + ":" + entry.getValue().asText());
              });
      if (configuration.has("stdin")) invocation.add("--interactive");
      invocation.add(configuration.required("image").asText());
      if (configuration.has("command")) invocation.add(configuration.path("command").asText());
      configuration.path("arguments").forEach(argument -> invocation.add(argument.asText()));
      process = new ProcessBuilder(invocation).start();
      active.set(process);
      if (configuration.has("stdin"))
        process
            .getOutputStream()
            .write(configuration.path("stdin").asText().getBytes(StandardCharsets.UTF_8));
      process.getOutputStream().close();
      Process owned = process;
      var stdout = readAsync(owned.getInputStream());
      var stderr = readAsync(owned.getErrorStream());
      if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        terminate(process);
        stop(containerName);
        throw new java.util.concurrent.TimeoutException("Container run timed out");
      }
      int code = process.exitValue();
      JsonNode output = result(operation, stdout.join(), stderr.join(), code);
      sink.observe(
              operation.mode() == ProtocolOperationDescriptor.Mode.RUN_DETACHED
                  ? "launched"
                  : "completed",
              output,
              false,
              true,
              clock.instant())
          .whenComplete((ignored, failure) -> complete(result, failure));
      if (cleanupAt != null)
        scheduleCleanup(containerName, Duration.between(clock.instant(), cleanupAt));
    } catch (Exception failure) {
      if (!result.isDone())
        sink.observe("container-transport-failure", problem(failure), true, true, clock.instant())
            .whenComplete((ignored, observationFailure) -> complete(result, observationFailure));
    } finally {
      terminate(process);
      active.compareAndSet(process, null);
    }
  }

  private JsonNode result(
      ProtocolOperationDescriptor operation, String stdout, String stderr, int code) {
    return switch (operation.request().path("return").asText("stdout")) {
      case "stderr" -> JsonNodeFactory.instance.textNode(stderr);
      case "code" -> JsonNodeFactory.instance.numberNode(code);
      case "all" ->
          JsonNodeFactory.instance
              .objectNode()
              .put("stdout", stdout)
              .put("stderr", stderr)
              .put("code", code);
      case "none" -> JsonNodeFactory.instance.nullNode();
      default -> JsonNodeFactory.instance.textNode(stdout);
    };
  }

  private CompletableFuture<String> readAsync(InputStream input) {
    var result = new CompletableFuture<String>();
    Thread.ofVirtual()
        .start(
            () -> {
              try (input;
                  var output = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                  if (output.size() + count > maximumOutputBytes) {
                    throw new IllegalStateException(
                        "Container output exceeded " + maximumOutputBytes + " bytes");
                  }
                  output.write(buffer, 0, count);
                }
                result.complete(output.toString(StandardCharsets.UTF_8));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              }
            });
    return result;
  }

  private void stop(String containerName) {
    try {
      Process stop =
          new ProcessBuilder(runtime, "stop", "--time", "2", containerName)
              .redirectErrorStream(true)
              .start();
      stop.getInputStream().transferTo(java.io.OutputStream.nullOutputStream());
      if (!stop.waitFor(3, TimeUnit.SECONDS)) stop.destroyForcibly();
    } catch (Exception ignored) {
    }
  }

  private void scheduleCleanup(String containerName, Duration delay) {
    long milliseconds = Math.max(0, delay.toMillis());
    CompletableFuture.runAsync(
        () -> remove(containerName),
        CompletableFuture.delayedExecutor(milliseconds, TimeUnit.MILLISECONDS));
  }

  private void reconcileExpired() {
    try {
      Process listed =
          new ProcessBuilder(
                  runtime,
                  "ps",
                  "--all",
                  "--filter",
                  "label=io.forwardmeasure.openworkflow.cleanup-at",
                  "--format",
                  "{{.Names}} {{.Label \"io.forwardmeasure.openworkflow.cleanup-at\"}}")
              .redirectErrorStream(true)
              .start();
      String output = new String(listed.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!listed.waitFor(5, TimeUnit.SECONDS) || listed.exitValue() != 0) return;
      long now = clock.instant().toEpochMilli();
      for (String line : output.lines().toList()) {
        String[] fields = line.strip().split("\\s+", 2);
        if (fields.length != 2) continue;
        try {
          if (Long.parseLong(fields[1]) <= now) remove(fields[0]);
          else scheduleCleanup(fields[0], Duration.ofMillis(Long.parseLong(fields[1]) - now));
        } catch (NumberFormatException ignored) {
        }
      }
    } catch (Exception ignored) {
    }
  }

  private void remove(String containerName) {
    try {
      Process removal =
          new ProcessBuilder(runtime, "rm", "--force", containerName)
              .redirectErrorStream(true)
              .start();
      removal.getInputStream().transferTo(java.io.OutputStream.nullOutputStream());
      if (!removal.waitFor(5, TimeUnit.SECONDS)) removal.destroyForcibly();
    } catch (Exception ignored) {
    }
  }

  private static Duration cleanupAfter(JsonNode configured) {
    if (configured.isTextual()) return Duration.parse(configured.asText());
    if (!configured.isObject())
      throw new IllegalArgumentException(
          "Container cleanup duration must be ISO-8601 text or an object");
    Duration result = Duration.ZERO;
    result = result.plusDays(configured.path("days").asLong());
    result = result.plusHours(configured.path("hours").asLong());
    result = result.plusMinutes(configured.path("minutes").asLong());
    result = result.plusSeconds(configured.path("seconds").asLong());
    result = result.plusMillis(configured.path("milliseconds").asLong());
    if (result.isZero() || result.isNegative())
      throw new IllegalArgumentException("Container eventual cleanup duration must be positive");
    return result;
  }

  private static ObjectNode problem(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:run:container")
        .put("status", 500)
        .put("title", "Container run failed")
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

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }

  private static String containerName(
      ExecutionId executionId, ProtocolOperationDescriptor operation, JsonNode configuration) {
    String configured = configuration.path("name").asText();
    if (configured.isBlank()) return "ow-" + operation.operationId();
    if (!configured.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,47}")) {
      throw new IllegalArgumentException("Invalid container name");
    }
    String tenant =
        java.util
            .UUID
            .nameUUIDFromBytes(
                executionId.tenantId().value().toString().getBytes(StandardCharsets.UTF_8))
            .toString()
            .substring(0, 8);
    return "ow-" + tenant + "-" + configured;
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
