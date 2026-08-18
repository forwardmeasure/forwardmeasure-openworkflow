package com.forwardmeasure.openworkflow.operation.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;
import org.junit.jupiter.api.Test;

final class AsyncApiRedisOperationExecutorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:redis"), UUID.randomUUID());

  @Test
  void publishCommitsToStreamBeforeTerminalObservation() {
    var calls = new ArrayList<String>();
    var executor = executor((server, password, timeout) -> stream(calls, new AtomicReference<>()));

    executor
        .execute(
            EXECUTION,
            operation(ProtocolOperationDescriptor.Mode.PUBLISH),
            (id, value, failed, terminal, at) -> {
              calls.add("observe:" + id);
              return CompletableFuture.completedFuture(
                  ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(List.of("publish:orders:42-0", "observe:42-0", "close"), calls);
  }

  @Test
  void consumerGroupAcknowledgesOnlyAfterObservation() {
    var calls = new ArrayList<String>();
    var handler = new AtomicReference<AsyncApiRedisOperationExecutor.DeliveryHandler>();
    var executor = executor((server, password, timeout) -> stream(calls, handler));
    var completion =
        executor
            .execute(
                EXECUTION,
                operation(ProtocolOperationDescriptor.Mode.SUBSCRIBE),
                (id, value, failed, terminal, at) -> {
                  calls.add("observe:" + value.required("order").asInt());
                  return CompletableFuture.completedFuture(
                      ProtocolOperationExecutor.ObservationDisposition.STOP);
                })
            .toCompletableFuture();
    assertFalse(completion.isDone());

    handler.get().delivered("43-0", "{\"order\":43}", () -> calls.add("ack"));
    completion.join();

    assertEquals(List.of("subscribe:orders:openworkflow", "observe:43", "ack", "close"), calls);
  }

  private static AsyncApiRedisOperationExecutor executor(
      AsyncApiRedisOperationExecutor.StreamFactory streams) {
    return new AsyncApiRedisOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(Instant.parse("2026-08-16T18:00:00Z"), ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        SecretProvider.rejecting(),
        streams);
  }

  private static AsyncApiRedisOperationExecutor.Stream stream(
      List<String> calls, AtomicReference<AsyncApiRedisOperationExecutor.DeliveryHandler> handler) {
    return new AsyncApiRedisOperationExecutor.Stream() {
      @Override
      public String publish(String stream, String value) {
        calls.add("publish:" + stream + ":42-0");
        return "42-0";
      }

      @Override
      public void subscribe(
          String stream,
          String group,
          String consumer,
          Duration timeout,
          AsyncApiRedisOperationExecutor.DeliveryHandler supplied,
          CompletableFuture<Done> completion) {
        calls.add("subscribe:" + stream + ":" + group);
        handler.set(supplied);
      }

      @Override
      public void close() {
        calls.add("close");
      }
    };
  }

  private static ProtocolOperationDescriptor operation(ProtocolOperationDescriptor.Mode mode) {
    var request = JsonNodeFactory.instance.objectNode();
    request.putObject("payload").put("order", 42);
    var subscription =
        mode == ProtocolOperationDescriptor.Mode.SUBSCRIBE
            ? new AsyncApiSubscriptionPlan(
                null,
                new AsyncApiSubscriptionPlan.Consumption(
                    AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT, 1, null, null),
                null,
                null,
                null)
            : null;
    return new ProtocolOperationDescriptor(
        "redis-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "d".repeat(64)),
        "redis",
        URI.create("redis://cache.example.test:6379/orders"),
        "orders",
        request,
        subscription,
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
