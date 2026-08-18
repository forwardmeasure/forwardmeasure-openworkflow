package com.forwardmeasure.openworkflow.operation.jms;

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
import org.junit.jupiter.api.Test;

final class AsyncApiJmsOperationExecutorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:jms"), UUID.randomUUID());

  @Test
  void everyJmsProviderPublishesBeforeObservation() {
    for (String protocol : List.of("jms", "ibmmq", "solace")) {
      var calls = new ArrayList<String>();
      var executor =
          executor(
              (selected, operation, username, password, timeout) -> {
                assertEquals(protocol, selected);
                return broker(calls, new AtomicReference<>());
              });
      executor
          .execute(
              EXECUTION,
              operation(protocol, ProtocolOperationDescriptor.Mode.PUBLISH),
              (id, value, failed, terminal, at) -> {
                calls.add("observe:" + id);
                return CompletableFuture.completedFuture(
                    ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();
      assertEquals(List.of("publish:orders", "observe:m-42", "close"), calls);
    }
  }

  @Test
  void subscriptionAcknowledgesOnlyAfterObservation() {
    var calls = new ArrayList<String>();
    var handler = new AtomicReference<AsyncApiJmsOperationExecutor.DeliveryHandler>();
    var executor =
        executor((protocol, operation, username, password, timeout) -> broker(calls, handler));
    var completion =
        executor
            .execute(
                EXECUTION,
                operation("ibmmq", ProtocolOperationDescriptor.Mode.SUBSCRIBE),
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
    assertEquals(List.of("subscribe:orders", "observe:43", "ack", "close"), calls);
  }

  private static AsyncApiJmsOperationExecutor executor(
      AsyncApiJmsOperationExecutor.BrokerFactory brokers) {
    return new AsyncApiJmsOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(Instant.parse("2026-08-16T18:00:00Z"), ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        SecretProvider.rejecting(),
        brokers);
  }

  private static AsyncApiJmsOperationExecutor.Broker broker(
      List<String> calls, AtomicReference<AsyncApiJmsOperationExecutor.DeliveryHandler> handler) {
    return new AsyncApiJmsOperationExecutor.Broker() {
      @Override
      public String publish(String destination, byte[] value) {
        calls.add("publish:" + destination);
        return "m-42";
      }

      @Override
      public void subscribe(
          String destination, AsyncApiJmsOperationExecutor.DeliveryHandler supplied) {
        calls.add("subscribe:" + destination);
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
        URI.create(protocol + "://broker.example.test:1414/orders"),
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
