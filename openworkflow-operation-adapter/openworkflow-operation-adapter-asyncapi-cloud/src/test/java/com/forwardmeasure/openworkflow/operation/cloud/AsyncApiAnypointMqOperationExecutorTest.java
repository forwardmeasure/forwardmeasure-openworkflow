package com.forwardmeasure.openworkflow.operation.cloud;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

final class AsyncApiAnypointMqOperationExecutorTest {
  private static final ExecutionId EXECUTION =
      new ExecutionId(
          new TenantId("did:web:forwardmeasure.com:tenant:anypoint"), UUID.randomUUID());

  @Test
  void publishUsesBrokerPutBeforeObservation() throws Exception {
    var calls = new ArrayList<String>();
    HttpServer server =
        server(
            exchange -> {
              calls.add(exchange.getRequestMethod() + ":" + exchange.getRequestURI().getPath());
              assertEquals(
                  "Bearer test-token", exchange.getRequestHeaders().getFirst("Authorization"));
              exchange.sendResponseHeaders(204, -1);
              exchange.close();
            });
    try {
      executor()
          .execute(
              EXECUTION,
              operation(server, ProtocolOperationDescriptor.Mode.PUBLISH),
              (id, value, failed, terminal, at) -> {
                calls.add("observe");
                return CompletableFuture.completedFuture(
                    ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();
      assertEquals("observe", calls.getLast());
      assertEquals(2, calls.size());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void consumeAcknowledgesOnlyAfterObservation() throws Exception {
    var calls = new ArrayList<String>();
    HttpServer server =
        server(
            exchange -> {
              if (exchange.getRequestMethod().equals("GET")) {
                byte[] body =
                    ("{\"headers\":{\"messageId\":\"m-43\","
                            + "\"lockId\":\"lock-43\"},"
                            + "\"body\":\"{\\\"order\\\":43}\"}")
                        .getBytes(StandardCharsets.UTF_8);
                calls.add("consume");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
              } else {
                calls.add("ack");
                exchange.sendResponseHeaders(204, -1);
              }
              exchange.close();
            });
    try {
      executor()
          .execute(
              EXECUTION,
              operation(server, ProtocolOperationDescriptor.Mode.SUBSCRIBE),
              (id, value, failed, terminal, at) -> {
                calls.add("observe:" + value.required("order").asInt());
                return CompletableFuture.completedFuture(
                    ProtocolOperationExecutor.ObservationDisposition.STOP);
              })
          .toCompletableFuture()
          .join();
      assertEquals(List.of("consume", "observe:43", "ack"), calls);
    } finally {
      server.stop(0);
    }
  }

  private static AsyncApiAnypointMqOperationExecutor executor() {
    return new AsyncApiAnypointMqOperationExecutor(
        Duration.ofSeconds(2),
        (tenant, endpoint) -> {},
        (tenant, name) -> "test-token".toCharArray());
  }

  private static ProtocolOperationDescriptor operation(
      HttpServer server, ProtocolOperationDescriptor.Mode mode) {
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
        "anypoint-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "d".repeat(64)),
        "anypointmq",
        URI.create(
            "http://127.0.0.1:"
                + server.getAddress().getPort()
                + "/api/v1/organizations/o/environments/e/destinations/orders"),
        "orders",
        request,
        subscription,
        new AuthenticationPlan(AuthenticationPlan.Kind.BEARER, null, "token"),
        null);
  }

  private static HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", handler);
    server.start();
    return server;
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
