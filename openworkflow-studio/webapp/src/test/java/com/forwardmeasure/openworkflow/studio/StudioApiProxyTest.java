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
package com.forwardmeasure.openworkflow.studio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the real proxy against real HTTP servers - every prior test of Studio's proxying
 * behavior mocked the generated API clients, so nothing ever checked what {@link StudioApiProxy}
 * itself actually does to a request. Three real production bugs lived in this exact class
 * undetected: routing every request to one upstream regardless of path (execution calls always
 * 404'd), and two required headers ({@code If-Match}, then separately {@code X-Correlation-ID})
 * silently dropped by {@code REQUEST_HEADERS} despite the frontend sending them correctly.
 */
final class StudioApiProxyTest {
  private HttpServer defaultUpstream;
  private HttpServer executionUpstream;
  private final AtomicReference<HttpExchange> lastDefaultExchange = new AtomicReference<>();
  private final AtomicReference<HttpExchange> lastExecutionExchange = new AtomicReference<>();

  @BeforeEach
  void startUpstreams() throws IOException {
    defaultUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    defaultUpstream.createContext(
        "/",
        exchange -> {
          lastDefaultExchange.set(exchange);
          respondOk(exchange);
        });
    defaultUpstream.start();

    executionUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    executionUpstream.createContext(
        "/",
        exchange -> {
          lastExecutionExchange.set(exchange);
          respondOk(exchange);
        });
    executionUpstream.start();
  }

  @AfterEach
  void stopUpstreams() {
    defaultUpstream.stop(0);
    executionUpstream.stop(0);
  }

  private static void respondOk(HttpExchange exchange) throws IOException {
    byte[] body = {};
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().close();
  }

  private StudioApiProxy proxy() {
    return new StudioApiProxy(baseUrl(defaultUpstream), baseUrl(executionUpstream));
  }

  private static String baseUrl(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @Test
  void routesWorkflowPathsToTheDefaultUpstream() throws Exception {
    proxy().forward("GET", "v1/workflows", Map.of(), new byte[0]);

    assertTrue(lastDefaultExchange.get() != null, "definition-management upstream was not called");
    assertTrue(
        lastExecutionExchange.get() == null, "execution upstream should not have been called");
  }

  @Test
  void routesExecutionPathsToTheExecutionUpstream() throws Exception {
    proxy().forward("GET", "v1/executions", Map.of(), new byte[0]);

    assertTrue(lastExecutionExchange.get() != null, "execution-management upstream was not called");
    assertTrue(
        lastDefaultExchange.get() == null,
        "definition-management upstream should not have been called");
  }

  @Test
  void forwardsEveryHeaderTheApiContractRequires() throws Exception {
    Map<String, List<String>> headers =
        Map.of(
            "Authorization", List.of("Bearer token"),
            "If-Match", List.of("\"3\""),
            "X-Correlation-ID", List.of("11111111-1111-1111-1111-111111111111"),
            "Idempotency-Key", List.of("22222222-2222-2222-2222-222222222222"));

    proxy().forward("PUT", "v1/workflow-definitions/abc/definitions/def", headers, new byte[0]);

    var received = lastDefaultExchange.get().getRequestHeaders();
    assertEquals("Bearer token", received.getFirst("Authorization"));
    assertEquals("\"3\"", received.getFirst("If-Match"));
    assertEquals("11111111-1111-1111-1111-111111111111", received.getFirst("X-Correlation-ID"));
    assertEquals("22222222-2222-2222-2222-222222222222", received.getFirst("Idempotency-Key"));
  }

  @Test
  void dropsHeadersOutsideTheContract() throws Exception {
    proxy()
        .forward(
            "GET",
            "v1/workflows",
            Map.of("X-Not-Part-Of-The-Contract", List.of("should not cross")),
            new byte[0]);

    assertFalse(
        lastDefaultExchange.get().getRequestHeaders().containsKey("X-Not-Part-Of-The-Contract"));
  }
}
