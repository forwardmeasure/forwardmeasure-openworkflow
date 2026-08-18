package com.forwardmeasure.openworkflow.operation.stomp;

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
import java.nio.charset.StandardCharsets;
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

final class AsyncApiStompOperationExecutorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:stomp"), UUID.randomUUID());

  @Test
  void publishWaitsForReceiptBeforeObservation() {
    var calls = new ArrayList<String>();
    var executor =
        executor(
            (host, port, tls, username, password, timeout) ->
                client(calls, new AtomicReference<>()));
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
    assertEquals(List.of("publish:/queue/orders", "observe:receipt-42", "close"), calls);
  }

  @Test
  void subscriptionAcknowledgesAfterObservationAndStops() {
    var calls = new ArrayList<String>();
    var handler = new AtomicReference<AsyncApiStompOperationExecutor.DeliveryHandler>();
    var executor =
        executor((host, port, tls, username, password, timeout) -> client(calls, handler));
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
    handler
        .get()
        .delivered(
            "m-43", "{\"order\":43}".getBytes(StandardCharsets.UTF_8), () -> calls.add("ack"));
    completion.join();
    assertEquals(List.of("subscribe:/queue/orders", "observe:43", "ack", "close"), calls);
  }

  private static AsyncApiStompOperationExecutor executor(
      AsyncApiStompOperationExecutor.ClientFactory clients) {
    return new AsyncApiStompOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(Instant.parse("2026-08-16T18:00:00Z"), ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        SecretProvider.rejecting(),
        clients);
  }

  private static AsyncApiStompOperationExecutor.Client client(
      List<String> calls, AtomicReference<AsyncApiStompOperationExecutor.DeliveryHandler> handler) {
    return new AsyncApiStompOperationExecutor.Client() {
      @Override
      public String publish(String destination, byte[] payload, Duration timeout) {
        calls.add("publish:" + destination);
        return "receipt-42";
      }

      @Override
      public void subscribe(
          String destination,
          AsyncApiStompOperationExecutor.DeliveryHandler supplied,
          CompletableFuture<Done> completion) {
        calls.add("subscribe:" + destination);
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
        "stomp-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "d".repeat(64)),
        "stomp",
        URI.create("stomp://broker.example.test:61613/queue/orders"),
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
