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
package com.forwardmeasure.openworkflow.eventing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class HttpCloudEventPublisherTest {
  private final ObjectMapper json = new ObjectMapper();

  @Test
  void publishesStructuredCloudEventWithIdempotencyKey() throws Exception {
    var contentType = new AtomicReference<String>();
    var idempotencyKey = new AtomicReference<String>();
    var body = new AtomicReference<byte[]>();
    HttpServer server =
        server(
            exchange -> {
              contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
              idempotencyKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
              body.set(exchange.getRequestBody().readAllBytes());
              exchange.sendResponseHeaders(202, -1);
              exchange.close();
            });
    try {
      var publisher = new HttpCloudEventPublisher(endpoint(server), json, Duration.ofSeconds(2));
      publisher.publish("operation-42", event()).toCompletableFuture().join();

      assertEquals("application/cloudevents+json", contentType.get());
      assertEquals("operation-42", idempotencyKey.get());
      var envelope = json.readTree(body.get());
      assertEquals("1.0", envelope.path("specversion").textValue());
      assertEquals("event-42", envelope.path("id").textValue());
      assertEquals("com.forwardmeasure.workflow.completed.v1", envelope.path("type").textValue());
      assertEquals("tenant-42", envelope.path("tenant").textValue());
      assertEquals(42, envelope.path("data").path("answer").intValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsNonSuccessfulHttpResponse() throws Exception {
    HttpServer server =
        server(
            exchange -> {
              exchange.getRequestBody().readAllBytes();
              exchange.sendResponseHeaders(503, -1);
              exchange.close();
            });
    try {
      var publisher = new HttpCloudEventPublisher(endpoint(server), json, Duration.ofSeconds(2));
      var failure =
          assertThrows(
              CompletionException.class,
              () -> publisher.publish("operation-42", event()).toCompletableFuture().join());
      assertEquals("CloudEvent endpoint returned HTTP 503", failure.getCause().getMessage());
    } finally {
      server.stop(0);
    }
  }

  private static HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws Exception {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/events", handler);
    server.start();
    return server;
  }

  private static URI endpoint(HttpServer server) {
    return URI.create(
        "http://"
            + server.getAddress().getHostString()
            + ":"
            + server.getAddress().getPort()
            + "/events");
  }

  private static WorkflowCloudEvent event() {
    return new WorkflowCloudEvent(
        "1.0",
        "event-42",
        URI.create("urn:forwardmeasure:workflow:test"),
        "com.forwardmeasure.workflow.completed.v1",
        "execution-42",
        null,
        "application/json",
        JsonNodeFactory.instance.objectNode().put("answer", 42),
        Map.of("tenant", JsonNodeFactory.instance.textNode("tenant-42")));
  }
}
