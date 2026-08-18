package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pekko.Done;
import org.junit.jupiter.api.Test;

final class RoutingProtocolOperationExecutorTest {
  @Test
  void routesOnlyAnExplicitKindAndProtocolRegistration() {
    var calls = new java.util.ArrayList<String>();
    ProtocolOperationExecutor mqtt =
        (execution, operation, sink) -> {
          calls.add(operation.operationId());
          return CompletableFuture.completedFuture(Done.getInstance());
        };
    var router =
        new RoutingProtocolOperationExecutor(
            Map.of(
                new RoutingProtocolOperationExecutor.DriverKey(
                    ProtocolOperationDescriptor.Kind.ASYNC_API, "MQTT"),
                mqtt));
    var execution =
        new ExecutionId(
            new TenantId("did:web:forwardmeasure.com:tenant:routing"), UUID.randomUUID());

    router
        .execute(
            execution,
            descriptor("mqtt"),
            (id, value, failed, terminal, at) ->
                CompletableFuture.completedFuture(
                    ProtocolOperationExecutor.ObservationDisposition.CONTINUE))
        .toCompletableFuture()
        .join();
    assertEquals(java.util.List.of("operation-1"), calls);

    assertThrows(
        CompletionException.class,
        () ->
            router
                .execute(
                    execution,
                    descriptor("kafka"),
                    (id, value, failed, terminal, at) ->
                        CompletableFuture.completedFuture(
                            ProtocolOperationExecutor.ObservationDisposition.CONTINUE))
                .toCompletableFuture()
                .join());
  }

  private static ProtocolOperationDescriptor descriptor(String protocol) {
    return new ProtocolOperationDescriptor(
        "operation-1",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        ProtocolOperationDescriptor.Mode.PUBLISH,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "c".repeat(64)),
        protocol,
        URI.create(protocol + "://broker.example.test"),
        "publish",
        JsonNodeFactory.instance.objectNode(),
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
