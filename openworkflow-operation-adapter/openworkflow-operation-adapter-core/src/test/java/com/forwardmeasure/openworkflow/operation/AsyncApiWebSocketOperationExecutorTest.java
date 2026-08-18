package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
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

final class AsyncApiWebSocketOperationExecutorTest {
  private static final Instant AT = Instant.parse("2026-08-16T16:00:00Z");
  private static final ExecutionId EXECUTION =
      new ExecutionId(
          new TenantId("did:web:forwardmeasure.com:tenant:websocket"), UUID.randomUUID());

  @Test
  void publishesJsonAndReportsOneTerminalReceipt() {
    var sent = new ArrayList<String>();
    var observations = new ArrayList<ProtocolOperationObservation>();
    WebSocket socket = socket(sent);
    var executor =
        executor(
            (endpoint, headers, listener) -> {
              assertEquals(URI.create("wss://events.example.test/orders"), endpoint);
              assertEquals("ws-operation", headers.get("X-OpenWorkflow-Operation"));
              return CompletableFuture.completedFuture(socket);
            });

    executor
        .execute(
            EXECUTION,
            operation(ProtocolOperationDescriptor.Mode.PUBLISH),
            (id, value, failed, terminal, observedAt) -> {
              observations.add(
                  new ProtocolOperationObservation(id, value, failed, terminal, observedAt));
              return CompletableFuture.completedFuture(
                  ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(List.of("{\"order\":42}"), sent);
    assertEquals("sent", observations.getFirst().observationId());
    assertFalse(observations.getFirst().failed());
    assertEquals(true, observations.getFirst().terminal());
  }

  @Test
  void subscribesWithOneMessageAtATimeUntilTheCoordinatorStops() {
    var listener = new AtomicReference<WebSocket.Listener>();
    WebSocket socket = socket(new ArrayList<>());
    var executor =
        executor(
            (endpoint, headers, connected) -> {
              listener.set(connected);
              return CompletableFuture.completedFuture(socket);
            });
    var observed = new ArrayList<Integer>();

    var completion =
        executor
            .execute(
                EXECUTION,
                operation(ProtocolOperationDescriptor.Mode.SUBSCRIBE),
                (id, value, failed, terminal, observedAt) -> {
                  observed.add(value.required("order").asInt());
                  return CompletableFuture.completedFuture(
                      ProtocolOperationExecutor.ObservationDisposition.STOP);
                })
            .toCompletableFuture();
    assertFalse(completion.isDone());
    listener.get().onText(socket, "{\"order\":43}", true).toCompletableFuture().join();

    completion.join();
    assertEquals(List.of(43), observed);
  }

  private static AsyncApiWebSocketOperationExecutor executor(
      AsyncApiWebSocketOperationExecutor.Connector connector) {
    return new AsyncApiWebSocketOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(AT, ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        HttpClient.newHttpClient(),
        SecretProvider.rejecting(),
        connector);
  }

  private static ProtocolOperationDescriptor operation(ProtocolOperationDescriptor.Mode mode) {
    var message = JsonNodeFactory.instance.objectNode();
    message.putObject("payload").put("order", 42);
    AsyncApiSubscriptionPlan subscription =
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
        "ws-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "a".repeat(64)),
        "ws",
        URI.create("wss://events.example.test/orders"),
        "orders",
        message,
        subscription,
        null,
        null);
  }

  private static WebSocket socket(List<String> sent) {
    AtomicReference<WebSocket> reference = new AtomicReference<>();
    WebSocket socket =
        (WebSocket)
            Proxy.newProxyInstance(
                WebSocket.class.getClassLoader(),
                new Class<?>[] {WebSocket.class},
                (proxy, method, arguments) ->
                    switch (method.getName()) {
                      case "sendText" -> {
                        sent.add(arguments[0].toString());
                        yield CompletableFuture.completedFuture(reference.get());
                      }
                      case "sendBinary", "sendPing", "sendPong", "sendClose" ->
                          CompletableFuture.completedFuture(reference.get());
                      case "isOutputClosed", "isInputClosed" -> false;
                      case "getSubprotocol" -> "";
                      case "request", "abort" -> null;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    reference.set(socket);
    return socket;
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
