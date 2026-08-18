package com.forwardmeasure.openworkflow.operation.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.junit.jupiter.api.Test;

final class AsyncApiAmqpOperationExecutorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:amqp"), UUID.randomUUID());

  @Test
  void amqpAndAmqp1PublishBeforeTerminalObservation() {
    for (String protocol : List.of("amqp", "amqp1")) {
      var calls = new ArrayList<String>();
      var executor =
          executor(
              (selected, server, username, password, timeout) -> {
                assertEquals(protocol, selected);
                assertEquals(URI.create("amqp://broker.example.test:5672"), server);
                return broker(calls, new AtomicReference<>());
              });

      executor
          .execute(
              EXECUTION,
              operation(protocol, ProtocolOperationDescriptor.Mode.PUBLISH),
              (id, value, failed, terminal, at) -> {
                calls.add("observe:" + value.required("channel").asText());
                return CompletableFuture.completedFuture(
                    ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();

      assertEquals(List.of("publish:orders.created", "observe:orders.created", "close"), calls);
    }
  }

  @Test
  void subscriptionAcknowledgesAfterObservationAndClosesOnStop() {
    var calls = new ArrayList<String>();
    var handler = new AtomicReference<AsyncApiAmqpOperationExecutor.DeliveryHandler>();
    var executor =
        executor((protocol, server, username, password, timeout) -> broker(calls, handler));
    var completion =
        executor
            .execute(
                EXECUTION,
                operation("amqp", ProtocolOperationDescriptor.Mode.SUBSCRIBE),
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
            "delivery-1",
            "{\"order\":43}".getBytes(StandardCharsets.UTF_8),
            () -> calls.add("ack"));

    completion.join();
    assertEquals(List.of("subscribe:orders.created", "observe:43", "ack", "close"), calls);
  }

  private static AsyncApiAmqpOperationExecutor executor(
      AsyncApiAmqpOperationExecutor.BrokerFactory brokers) {
    return new AsyncApiAmqpOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(Instant.parse("2026-08-16T18:00:00Z"), ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        SecretProvider.rejecting(),
        brokers);
  }

  private static AsyncApiAmqpOperationExecutor.Broker broker(
      List<String> calls, AtomicReference<AsyncApiAmqpOperationExecutor.DeliveryHandler> handler) {
    return new AsyncApiAmqpOperationExecutor.Broker() {
      @Override
      public void publish(String channel, byte[] payload, JsonNode request) {
        calls.add("publish:" + channel);
      }

      @Override
      public void subscribe(
          String channel,
          JsonNode request,
          AsyncApiAmqpOperationExecutor.DeliveryHandler supplied) {
        calls.add("subscribe:" + channel);
        handler.set(supplied);
      }

      @Override
      public void close() {
        calls.add("close");
      }
    };
  }

  private static ProtocolOperationDescriptor operation(
      String protocol, ProtocolOperationDescriptor.Mode mode) {
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
        protocol + "-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "d".repeat(64)),
        protocol,
        URI.create(protocol + "://broker.example.test:5672/orders.created"),
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
