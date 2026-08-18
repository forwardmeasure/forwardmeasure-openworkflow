package com.forwardmeasure.openworkflow.operation.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class OciContainerOperationExecutorTest {
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:container-runner");
  private static final String IMAGE =
      "registry.example.test/jobs/evidence@sha256:" + "a".repeat(64);

  @Test
  void recoversExpiredEventuallyCleanupFromDurableContainerLabel() throws Exception {
    var directory = Files.createTempDirectory("openworkflow-fake-oci-cleanup-");
    var removed = directory.resolve("removed");
    var runtime = directory.resolve("runtime");
    Files.writeString(
        runtime,
        """
        #!/bin/sh
        if [ "$1" = "ps" ]; then
          echo 'ow-expired 1'
          exit 0
        fi
        if [ "$1" = "rm" ]; then
          echo "$@" > "%s"
          exit 0
        fi
        exit 0
        """
            .formatted(removed));
    Files.setPosixFilePermissions(runtime, PosixFilePermissions.fromString("rwx------"));

    executor(runtime.toString());

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!Files.exists(removed) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(removed));
    assertTrue(Files.readString(removed).contains("ow-expired"));
  }

  @Test
  void invokesPinnedAllowedImageWithExactOciArgumentsAndReturnsExitData() throws Exception {
    var directory = Files.createTempDirectory("openworkflow-fake-oci-");
    var arguments = directory.resolve("arguments");
    var runtime = directory.resolve("runtime");
    Files.writeString(
        runtime,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \""
            + arguments
            + "\"\nprintf 'container-output'\nprintf 'warning' >&2\nexit 7\n");
    Files.setPosixFilePermissions(runtime, PosixFilePermissions.fromString("rwx------"));
    var configuration =
        JsonNodeFactory.instance.objectNode().put("image", IMAGE).put("command", "process");
    configuration.putArray("arguments").add("--evidence").add("ev-42");
    configuration.putObject("environment").put("MODE", "safe");
    configuration.putObject("volumes").put("/safe/input", "/work/input");
    configuration.putObject("ports").put("127.0.0.1:18080", "8080");
    var observed = new AtomicReference<com.fasterxml.jackson.databind.JsonNode>();

    executor(runtime.toString())
        .execute(
            execution(),
            descriptor(
                "container-1", configuration, ProtocolOperationDescriptor.Mode.RUN_AWAIT, "all"),
            (id, value, failed, terminal, at) -> {
              observed.set(value);
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                      .ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals("container-output", observed.get().required("stdout").asText());
    assertEquals("warning", observed.get().required("stderr").asText());
    assertEquals(7, observed.get().required("code").asInt());
    String invoked = Files.readString(arguments);
    assertTrue(invoked.contains("--pull=missing"));
    assertTrue(invoked.contains("MODE=safe"));
    assertTrue(invoked.contains("/safe/input:/work/input"));
    assertTrue(invoked.contains("127.0.0.1:18080:8080"));
    assertTrue(invoked.contains(IMAGE));
    assertTrue(invoked.contains("ev-42"));
  }

  @Test
  void cancellationStopsTheOwnedContainer() throws Exception {
    var directory = Files.createTempDirectory("openworkflow-fake-oci-cancel-");
    var started = directory.resolve("started");
    var stopped = directory.resolve("stopped");
    var runtime = directory.resolve("runtime");
    Files.writeString(
        runtime,
        """
        #!/bin/sh
        if [ "$1" = "ps" ]; then
          exit 0
        fi
        if [ "$1" = "stop" ]; then
          echo yes > "%s"
          exit 0
        fi
        echo yes > "%s"
        trap 'exit 0' TERM
        while true; do sleep 1; done
        """
            .formatted(stopped, started));
    Files.setPosixFilePermissions(runtime, PosixFilePermissions.fromString("rwx------"));
    var configuration = JsonNodeFactory.instance.objectNode().put("image", IMAGE);
    CompletableFuture<org.apache.pekko.Done> transport =
        executor(runtime.toString())
            .execute(
                execution(),
                descriptor(
                    "container-cancel",
                    configuration,
                    ProtocolOperationDescriptor.Mode.RUN_AWAIT,
                    "none"),
                (id, value, failed, terminal, at) ->
                    CompletableFuture.completedFuture(
                        com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                            .ObservationDisposition.CONTINUE))
            .toCompletableFuture();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!Files.exists(started) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(started));
    assertTrue(transport.cancel(true));
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.exists(stopped) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(stopped));
  }

  private static OciContainerOperationExecutor executor(String runtime) {
    return new OciContainerOperationExecutor(
        runtime,
        Duration.ofSeconds(30),
        4096,
        RunExecutionPolicy.configured(
            Map.of(),
            Map.of(),
            Map.of(TENANT, Set.of(IMAGE)),
            Map.of(TENANT, Set.of("/safe/input>/work/input")),
            Map.of(TENANT, Set.of("127.0.0.1:18080>8080"))));
  }

  private static ProtocolOperationDescriptor descriptor(
      String id,
      com.fasterxml.jackson.databind.JsonNode configuration,
      ProtocolOperationDescriptor.Mode mode,
      String returnMode) {
    var request = JsonNodeFactory.instance.objectNode().put("return", returnMode);
    request.set("configuration", configuration);
    return new ProtocolOperationDescriptor(
        id,
        ProtocolOperationDescriptor.Kind.RUN,
        mode,
        null,
        "run-container",
        URI.create("runner://local"),
        IMAGE,
        request,
        null,
        null,
        null);
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
