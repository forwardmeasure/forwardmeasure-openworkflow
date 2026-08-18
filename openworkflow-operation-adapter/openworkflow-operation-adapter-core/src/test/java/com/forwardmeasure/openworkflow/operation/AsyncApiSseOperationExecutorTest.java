package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class AsyncApiSseOperationExecutorTest {
  @Test
  void consumesServerSentEventsInOrderUntilTheDurableCoordinatorStops() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/orders",
        exchange -> {
          byte[] body =
              ("id: order-1\n"
                      + "data: {\"order\":1}\n\n"
                      + "id: order-2\n"
                      + "data: {\"order\":2}\n\n")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      var execution =
          new ExecutionId(new TenantId("did:web:forwardmeasure.com:tenant:sse"), UUID.randomUUID());
      var observed = new ArrayList<String>();
      var executor =
          new AsyncApiSseOperationExecutor(
              Duration.ofSeconds(2), (tenant, uri) -> {}, SecretProvider.rejecting());

      executor
          .execute(
              execution,
              operation(server.getAddress().getPort()),
              (id, value, failed, terminal, observedAt) -> {
                observed.add(id + ":" + value.required("order").asInt());
                return java.util.concurrent.CompletableFuture.completedFuture(
                    observed.size() == 2
                        ? ProtocolOperationExecutor.ObservationDisposition.STOP
                        : ProtocolOperationExecutor.ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();

      assertEquals(List.of("order-1:1", "order-2:2"), observed);
    } finally {
      server.stop(0);
    }
  }

  private static ProtocolOperationDescriptor operation(int port) {
    var subscription =
        new AsyncApiSubscriptionPlan(
            null,
            new AsyncApiSubscriptionPlan.Consumption(
                AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT, 2, null, null),
            null,
            null,
            null);
    return new ProtocolOperationDescriptor(
        "sse-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        ProtocolOperationDescriptor.Mode.SUBSCRIBE,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "b".repeat(64)),
        "http",
        URI.create("http://127.0.0.1:" + port + "/orders"),
        "orders",
        JsonNodeFactory.instance.objectNode(),
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
