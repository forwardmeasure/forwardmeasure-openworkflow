package com.forwardmeasure.openworkflow.operation.nats;

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
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import java.lang.reflect.Proxy;
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
import org.junit.jupiter.api.Test;

final class AsyncApiNatsOperationExecutorTest {
  private static final Instant AT = Instant.parse("2026-08-16T17:00:00Z");
  private static final ExecutionId EXECUTION =
      new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:nats"), UUID.randomUUID());

  @Test
  void publishesAndFlushesBeforeReportingTheTerminalReceipt() {
    var calls = new ArrayList<String>();
    Connection connection = connection(calls, new AtomicReference<>());
    var executor = executor(options -> connection);

    executor
        .execute(
            EXECUTION,
            operation(ProtocolOperationDescriptor.Mode.PUBLISH),
            (id, value, failed, terminal, at) -> {
              calls.add("observe:" + value.required("subject").asText());
              return CompletableFuture.completedFuture(
                  ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(
        List.of(
            "publish:orders.created:{\"order\":42}", "flush", "observe:orders.created", "close"),
        calls);
  }

  @Test
  void subscribesUntilTheCoordinatorStopsThenClosesTheDispatcherAndConnection() throws Exception {
    var calls = new ArrayList<String>();
    var handler = new AtomicReference<MessageHandler>();
    Connection connection = connection(calls, handler);
    var executor = executor(options -> connection);
    var observed = new ArrayList<Integer>();

    var completion =
        executor
            .execute(
                EXECUTION,
                operation(ProtocolOperationDescriptor.Mode.SUBSCRIBE),
                (id, value, failed, terminal, at) -> {
                  observed.add(value.required("order").asInt());
                  return CompletableFuture.completedFuture(
                      ProtocolOperationExecutor.ObservationDisposition.STOP);
                })
            .toCompletableFuture();
    assertFalse(completion.isDone());
    handler.get().onMessage(message("orders.created", "1", "{\"order\":43}"));

    completion.join();
    assertEquals(List.of(43), observed);
    assertEquals(List.of("subscribe:orders.created", "close-dispatcher", "close"), calls);
  }

  private static AsyncApiNatsOperationExecutor executor(
      AsyncApiNatsOperationExecutor.ConnectionFactory factory) {
    return new AsyncApiNatsOperationExecutor(
        Duration.ofSeconds(2),
        Clock.fixed(AT, ZoneOffset.UTC),
        (tenant, endpoint) -> {},
        SecretProvider.rejecting(),
        factory);
  }

  private static Connection connection(
      List<String> calls, AtomicReference<MessageHandler> handler) {
    AtomicReference<Connection> connection = new AtomicReference<>();
    Dispatcher dispatcher =
        (Dispatcher)
            Proxy.newProxyInstance(
                Dispatcher.class.getClassLoader(),
                new Class<?>[] {Dispatcher.class},
                (proxy, method, arguments) ->
                    switch (method.getName()) {
                      case "subscribe" -> {
                        calls.add("subscribe:" + arguments[0]);
                        yield proxy;
                      }
                      default -> defaultValue(method.getReturnType(), proxy);
                    });
    Connection proxy =
        (Connection)
            Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (ignored, method, arguments) ->
                    switch (method.getName()) {
                      case "publish" -> {
                        calls.add(
                            "publish:"
                                + arguments[0]
                                + ":"
                                + new String(
                                    (byte[]) arguments[1],
                                    java.nio.charset.StandardCharsets.UTF_8));
                        yield null;
                      }
                      case "flush" -> {
                        calls.add("flush");
                        yield null;
                      }
                      case "createDispatcher" -> {
                        handler.set((MessageHandler) arguments[0]);
                        yield dispatcher;
                      }
                      case "closeDispatcher" -> {
                        calls.add("close-dispatcher");
                        yield null;
                      }
                      case "close" -> {
                        calls.add("close");
                        yield null;
                      }
                      default -> defaultValue(method.getReturnType(), connection.get());
                    });
    connection.set(proxy);
    return proxy;
  }

  private static Message message(String subject, String sid, String value) {
    return (Message)
        Proxy.newProxyInstance(
            Message.class.getClassLoader(),
            new Class<?>[] {Message.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "getSubject" -> subject;
                  case "getSID" -> sid;
                  case "getData" -> value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                  default -> defaultValue(method.getReturnType(), null);
                });
  }

  private static Object defaultValue(Class<?> type, Object self) {
    if (type == void.class) return null;
    if (type == boolean.class) return false;
    if (type == int.class) return 0;
    if (type == long.class) return 0L;
    if (type.isInstance(self)) return self;
    return null;
  }

  private static ProtocolOperationDescriptor operation(ProtocolOperationDescriptor.Mode mode) {
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
    var request = JsonNodeFactory.instance.objectNode();
    request.putObject("payload").put("order", 42);
    return new ProtocolOperationDescriptor(
        "nats-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "c".repeat(64)),
        "nats",
        URI.create("nats://broker.example.test:4222/orders.created"),
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
