package com.forwardmeasure.openworkflow.operation.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationObservation;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class JsonRpcHttpOperationExecutorTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void acquiresOAuthTokenAtTheTenantEdgeBeforeA2aDispatch() throws Exception {
    var tokenRequest = new AtomicReference<String>();
    var authorization = new AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/token",
        exchange -> {
          tokenRequest.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] body =
              "{\"access_token\":\"edge-token\",\"token_type\":\"Bearer\"}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/rpc",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body =
              "{\"jsonrpc\":\"2.0\",\"id\":\"oauth-a2a\",\"result\":{}}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      URI authority = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
      var configuration =
          JsonNodeFactory.instance
              .objectNode()
              .put("authority", authority.toString())
              .put("grant", "client_credentials");
      configuration.putObject("endpoints").put("token", "/token");
      configuration
          .putObject("client")
          .put("id", "workflow-client")
          .put("secret", "${ $secrets.clientSecret }")
          .put("authentication", "client_secret_post");
      var authentication =
          AuthenticationPlan.expressions(
              AuthenticationPlan.Kind.OAUTH2, null, configuration, List.of("clientSecret"));
      var context =
          new AuthenticationExpressionContext(
              null, JsonNodeFactory.instance.objectNode(), null, null, null, null, null, Map.of());
      var descriptor =
          new ProtocolOperationDescriptor(
              "oauth-a2a",
              ProtocolOperationDescriptor.Kind.A2A,
              ProtocolOperationDescriptor.Mode.RPC_UNARY,
              null,
              "a2a-jsonrpc",
              authority.resolve("/rpc"),
              "tasks/get",
              JsonNodeFactory.instance.objectNode().put("id", "task-1"),
              null,
              authentication,
              context);

      new JsonRpcHttpOperationExecutor(
              Duration.ofSeconds(3),
              HttpEgressPolicy.allowAllForTesting(),
              (tenant, name) -> {
                assertEquals("clientSecret", name);
                return "tenant-client-secret".toCharArray();
              })
          .execute(
              execution("oauth-a2a"),
              descriptor,
              (id, value, failed, terminal, at) ->
                  CompletableFuture.completedFuture(
                      com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                          .ObservationDisposition.CONTINUE))
          .toCompletableFuture()
          .join();

      assertTrue(tokenRequest.get().contains("grant_type=client_credentials"));
      assertTrue(tokenRequest.get().contains("client_id=workflow-client"));
      assertTrue(tokenRequest.get().contains("client_secret=tenant-client-secret"));
      assertEquals("Bearer edge-token", authorization.get());
      assertTrue(!descriptor.toString().contains("tenant-client-secret"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void answersDigestChallengeBeforeA2aJsonRpcDispatch() throws Exception {
    var attempts = new AtomicInteger();
    var authorization = new AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/rpc",
        exchange -> {
          int attempt = attempts.incrementAndGet();
          String header = exchange.getRequestHeaders().getFirst("Authorization");
          if (attempt == 1) {
            exchange
                .getResponseHeaders()
                .set(
                    "WWW-Authenticate",
                    "Digest realm=\"agents\", nonce=\"n-1\", algorithm=SHA-256, qop=\"auth\"");
            exchange.sendResponseHeaders(401, -1);
          } else {
            authorization.set(header);
            byte[] body =
                "{\"jsonrpc\":\"2.0\",\"id\":\"digest-a2a\",\"result\":{}}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
    try {
      URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/rpc");
      var descriptor =
          new ProtocolOperationDescriptor(
              "digest-a2a",
              ProtocolOperationDescriptor.Kind.A2A,
              ProtocolOperationDescriptor.Mode.RPC_UNARY,
              null,
              "a2a-jsonrpc",
              endpoint,
              "tasks/get",
              JsonNodeFactory.instance.objectNode().put("id", "task-1"),
              null,
              new AuthenticationPlan(AuthenticationPlan.Kind.DIGEST, null, "digest"),
              null);

      new JsonRpcHttpOperationExecutor(
              Duration.ofSeconds(3),
              HttpEgressPolicy.allowAllForTesting(),
              (tenant, name) -> "agent:password".toCharArray())
          .execute(
              execution("digest-a2a"),
              descriptor,
              (id, value, failed, terminal, at) ->
                  CompletableFuture.completedFuture(
                      com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                          .ObservationDisposition.CONTINUE))
          .toCompletableFuture()
          .join();

      assertEquals(2, attempts.get());
      assertTrue(authorization.get().startsWith("Digest username=\"agent\""));
      assertTrue(authorization.get().contains("algorithm=SHA-256"));
      assertTrue(authorization.get().contains("uri=\"/rpc\""));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void cancellingA2aStreamClosesTheOwnedHttpResponse() throws Exception {
    var streaming = new CountDownLatch(1);
    var disconnected = new CountDownLatch(1);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/stream",
        exchange -> {
          exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
          exchange.sendResponseHeaders(200, 0);
          try {
            exchange
                .getResponseBody()
                .write(
                    ("data: {\"jsonrpc\":\"2.0\","
                            + "\"id\":\"a2a-stream-1\",\"result\":{\"state\":\"working\"}}\\n\\n")
                        .getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().flush();
            streaming.countDown();
            while (true) {
              Thread.sleep(20);
              exchange
                  .getResponseBody()
                  .write(": keepalive\\n\\n".getBytes(StandardCharsets.UTF_8));
              exchange.getResponseBody().flush();
            }
          } catch (Exception closed) {
            disconnected.countDown();
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      var descriptor =
          new ProtocolOperationDescriptor(
              "a2a-stream-1",
              ProtocolOperationDescriptor.Kind.A2A,
              ProtocolOperationDescriptor.Mode.RPC_STREAM,
              null,
              "a2a-jsonrpc",
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/stream"),
              "message/stream",
              JsonNodeFactory.instance.objectNode(),
              null,
              null,
              null);
      CompletableFuture<org.apache.pekko.Done> transport =
          new JsonRpcHttpOperationExecutor(
                  Duration.ofSeconds(30),
                  HttpEgressPolicy.allowAllForTesting(),
                  SecretProvider.rejecting())
              .execute(
                  execution("a2a-stream"),
                  descriptor,
                  (id, value, failed, terminal, at) ->
                      CompletableFuture.completedFuture(
                          com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                              .ObservationDisposition.CONTINUE))
              .toCompletableFuture();
      assertTrue(streaming.await(3, TimeUnit.SECONDS));
      assertTrue(transport.cancel(true));
      assertTrue(disconnected.await(3, TimeUnit.SECONDS));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void initializesMcpSessionBeforeCallingTheWorkflowMethod() throws Exception {
    var requests = new ArrayList<com.fasterxml.jackson.databind.JsonNode>();
    var sessions = new ArrayList<String>();
    var count = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/mcp",
        exchange -> {
          int requestIndex = count.getAndIncrement();
          requests.add(JSON.readTree(exchange.getRequestBody()));
          sessions.add(exchange.getRequestHeaders().getFirst("Mcp-Session-Id"));
          if (requestIndex == 0) {
            byte[] body =
                """
                {"jsonrpc":"2.0","id":"mcp-1-initialize","result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"test","version":"1"}}}
                """
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Mcp-Session-Id", "session-42");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
          } else if (requestIndex == 1) {
            exchange.sendResponseHeaders(202, -1);
          } else {
            byte[] body =
                """
                {"jsonrpc":"2.0","id":"mcp-1","result":{"content":[{"type":"text","text":"done"}]}}
                """
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
    try {
      var endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/mcp");
      var request = JsonNodeFactory.instance.objectNode().put("protocolVersion", "2025-06-18");
      request.putObject("client").put("name", "openworkflow").put("version", "1.0.0");
      request.putObject("transport").putObject("http").put("endpoint", endpoint.toString());
      request.putObject("parameters").put("name", "extract");
      var descriptor =
          new ProtocolOperationDescriptor(
              "mcp-1",
              ProtocolOperationDescriptor.Kind.MCP,
              ProtocolOperationDescriptor.Mode.RPC_UNARY,
              null,
              "mcp-http",
              endpoint,
              "tools/call",
              request,
              null,
              null,
              null);
      var observations = new ArrayList<ProtocolOperationObservation>();

      new JsonRpcHttpOperationExecutor(
              Duration.ofSeconds(3),
              HttpEgressPolicy.allowAllForTesting(),
              SecretProvider.rejecting())
          .execute(
              execution("mcp"),
              descriptor,
              (id, value, failed, terminal, at) -> {
                observations.add(new ProtocolOperationObservation(id, value, failed, terminal, at));
                return CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                        .ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();

      assertEquals(
          List.of("initialize", "notifications/initialized", "tools/call"),
          requests.stream().map(value -> value.required("method").asText()).toList());
      assertEquals(java.util.Arrays.asList(null, "session-42", "session-42"), sessions);
      assertEquals("extract", requests.get(2).required("params").required("name").asText());
      assertEquals(
          "done",
          observations.getFirst().value().required("content").get(0).required("text").asText());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void executesA2aJsonRpcAndPersistsOnlyTheResult() throws Exception {
    var request = new AtomicReference<com.fasterxml.jackson.databind.JsonNode>();
    var authorization = new AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/rpc",
        exchange -> {
          request.set(JSON.readTree(exchange.getRequestBody()));
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body =
              """
              {"jsonrpc":"2.0","id":"a2a-1","result":{"task":{"id":"t-1"}}}
              """
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      var endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/rpc");
      String card =
          "{\"url\":\""
              + endpoint
              + "\","
              + "\"securitySchemes\":{\"agentBearer\":{\"type\":\"http\","
              + "\"scheme\":\"Bearer\"}},\"security\":[{\"agentBearer\":[]}]}";
      var descriptor =
          new ProtocolOperationDescriptor(
              "a2a-1",
              ProtocolOperationDescriptor.Kind.A2A,
              ProtocolOperationDescriptor.Mode.RPC_UNARY,
              new WorkflowResourceReference(
                  WorkflowResourceKind.A2A_AGENT_CARD,
                  URI.create("https://agent.example.test/.well-known/agent-card.json"),
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
              "a2a-jsonrpc",
              endpoint,
              "message/send",
              JsonNodeFactory.instance.objectNode().put("messageId", "m-1"),
              null,
              null,
              null,
              card);
      var observations = new ArrayList<ProtocolOperationObservation>();
      new JsonRpcHttpOperationExecutor(
              Duration.ofSeconds(3),
              HttpEgressPolicy.allowAllForTesting(),
              (tenant, name) -> "card-token".toCharArray())
          .execute(
              execution("a2a"),
              descriptor,
              (id, value, failed, terminal, at) -> {
                observations.add(new ProtocolOperationObservation(id, value, failed, terminal, at));
                return CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                        .ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();

      assertEquals("2.0", request.get().required("jsonrpc").asText());
      assertEquals("message/send", request.get().required("method").asText());
      assertEquals("m-1", request.get().required("params").required("messageId").asText());
      assertEquals("Bearer card-token", authorization.get());
      assertEquals(1, observations.size());
      assertTrue(observations.getFirst().terminal());
      assertEquals("t-1", observations.getFirst().value().required("task").required("id").asText());
    } finally {
      server.stop(0);
    }
  }

  private static ExecutionId execution(String suffix) {
    return new ExecutionId(
        new TenantId("did:web:forwardmeasure.com:tenant:" + suffix), UUID.randomUUID());
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
