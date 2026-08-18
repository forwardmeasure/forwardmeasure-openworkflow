package com.forwardmeasure.openworkflow.operation.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class LocalProcessOperationExecutorTest {
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:local-runner");

  @Test
  void executesExactShellArgumentsAndReturnsBoundedProcessResult() {
    var policy =
        RunExecutionPolicy.configured(
            Map.of(TENANT, Set.of("/usr/bin/printf")), Map.of(), Map.of());
    var configuration = JsonNodeFactory.instance.objectNode().put("command", "/usr/bin/printf");
    configuration.putArray("arguments").add("%s:%s").add("hello world").add("safe");
    var observations = new ArrayList<ProtocolOperationObservation>();

    new LocalProcessOperationExecutor(Duration.ofSeconds(3), 4096, policy)
        .execute(
            execution(),
            descriptor("shell-1", "run-shell", "/usr/bin/printf", configuration, "all", null),
            (id, value, failed, terminal, at) -> {
              observations.add(new ProtocolOperationObservation(id, value, failed, terminal, at));
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                      .ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals("hello world:safe", observations.getFirst().value().required("stdout").asText());
    assertEquals(0, observations.getFirst().value().required("code").asInt());
    assertTrue(observations.getFirst().terminal());
  }

  @Test
  void executesPinnedScriptThroughTenantApprovedInterpreter() {
    var policy =
        RunExecutionPolicy.configured(
            Map.of(), Map.of(TENANT, Map.of("shell", "/bin/sh")), Map.of());
    var configuration =
        JsonNodeFactory.instance
            .objectNode()
            .put("language", "shell")
            .put("code", "printf '%s' \"$OPENWORKFLOW_SCRIPT_ARGUMENTS\"");
    configuration.putObject("arguments").put("evidence", "ev-42");
    var observation = new CompletableFuture<ProtocolOperationObservation>();

    new LocalProcessOperationExecutor(Duration.ofSeconds(3), 4096, policy)
        .execute(
            execution(),
            descriptor("script-1", "run-script", "shell", configuration, "stdout", null),
            (id, value, failed, terminal, at) -> {
              observation.complete(
                  new ProtocolOperationObservation(id, value, failed, terminal, at));
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                      .ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals("{\"evidence\":\"ev-42\"}", observation.join().value().asText());
  }

  @Test
  void cancellingAwaitedRunTerminatesTheOwnedProcess() throws Exception {
    var directory = Files.createTempDirectory("openworkflow-run-cancel-");
    var started = directory.resolve("started");
    var terminated = directory.resolve("terminated");
    String script =
        "trap 'echo yes > \""
            + terminated
            + "\"; exit 0' TERM; echo yes > \""
            + started
            + "\"; while true; do sleep 1; done";
    var configuration = JsonNodeFactory.instance.objectNode().put("command", "/bin/sh");
    configuration.putArray("arguments").add("-c").add(script);
    var policy =
        RunExecutionPolicy.configured(Map.of(TENANT, Set.of("/bin/sh")), Map.of(), Map.of());
    CompletableFuture<org.apache.pekko.Done> transport =
        new LocalProcessOperationExecutor(Duration.ofSeconds(30), 4096, policy)
            .execute(
                execution(),
                descriptor("shell-cancel", "run-shell", "/bin/sh", configuration, "none", null),
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
      String id,
      String protocol,
      String operation,
      com.fasterxml.jackson.databind.JsonNode configuration,
      String returnMode,
      String source) {
    var request = JsonNodeFactory.instance.objectNode().put("return", returnMode);
    request.set("configuration", configuration);
    return new ProtocolOperationDescriptor(
        id,
        ProtocolOperationDescriptor.Kind.RUN,
        ProtocolOperationDescriptor.Mode.RUN_AWAIT,
        null,
        protocol,
        URI.create("runner://local"),
        operation,
        request,
        null,
        null,
        null,
        source);
  }

  private static ExecutionId execution() {
    return new ExecutionId(TENANT, UUID.randomUUID());
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
