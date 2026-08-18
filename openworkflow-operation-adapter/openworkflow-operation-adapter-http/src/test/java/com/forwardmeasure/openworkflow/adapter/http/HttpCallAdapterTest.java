package com.forwardmeasure.openworkflow.adapter.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpCallAdapterTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private HttpServer server;
  private String baseUri;
  private final AtomicInteger digestRequests = new AtomicInteger();
  private final CountDownLatch slowRequestStarted = new CountDownLatch(1);
  private final CountDownLatch releaseSlowRequest = new CountDownLatch(1);

  @BeforeEach
  void startRealHttpServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/pets", this::pet);
    server.createContext(
        "/redirect",
        exchange -> {
          exchange.getResponseHeaders().add("Location", baseUri + "/pets/redirected");
          respond(exchange, 302, "text/plain", "moved");
        });
    server.createContext(
        "/binary",
        exchange -> {
          byte[] body = new byte[] {0, 1, 2, (byte) 255};
          exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/digest",
        exchange -> {
          int attempt = digestRequests.incrementAndGet();
          String authorization = exchange.getRequestHeaders().getFirst("Authorization");
          if (attempt == 1) {
            assertEquals(null, authorization);
            exchange
                .getResponseHeaders()
                .add(
                    "WWW-Authenticate",
                    "Digest realm=\"evidence\", "
                        + "nonce=\"server-nonce\", "
                        + "opaque=\"server-opaque\", "
                        + "algorithm=SHA-256, qop=\"auth,auth-int\"");
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          assertTrue(authorization.startsWith("Digest "));
          assertTrue(authorization.contains("username=\"digest-user\""));
          assertTrue(authorization.contains("realm=\"evidence\""));
          assertTrue(authorization.contains("nonce=\"server-nonce\""));
          assertTrue(authorization.contains("uri=\"/digest\""));
          assertTrue(authorization.contains("algorithm=SHA-256"));
          assertTrue(authorization.contains("qop=auth"));
          assertTrue(authorization.contains("nc=00000001"));
          assertTrue(authorization.contains("response=\""));
          respond(exchange, 200, "application/json", "{\"ok\":true}");
        });
    server.createContext(
        "/slow",
        exchange -> {
          slowRequestStarted.countDown();
          try {
            if (!releaseSlowRequest.await(10, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Test did not release HTTP request");
            }
            respond(exchange, 200, "application/json", "{\"late\":true}");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            exchange.close();
          }
        });
    server.createContext(
        "/oversized",
        exchange ->
            respond(
                exchange,
                200,
                "application/json",
                "{\"value\":\"" + "x".repeat(DataReferences.MAX_INLINE_BYTES + 1) + "\"}"));
    server.start();
    baseUri = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopRealHttpServer() {
    releaseSlowRequest.countDown();
    server.stop(0);
  }

  @Test
  void executesInterpolatedRequestAndProducesNormativeResponseShape() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "post");
    arguments.put("endpoint", baseUri + "/pets/{petId}");
    arguments.putObject("query").put("status", "available now");
    arguments.putObject("headers").put("x-tenant", "ssb");
    arguments.set("body", JSON.readTree("{\"instruction\":\"extract persons\"}"));
    arguments.put("output", "response");
    OperationObservation observation =
        new HttpCallAdapter()
            .execute(request(arguments, JSON.readTree("{\"petId\":\"pet 17\"}")), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertEquals(OperationObservationStatus.SUCCEEDED, observation.status());
    var response = observation.output().inlineValue();
    assertEquals(200, response.required("statusCode").intValue());
    assertEquals("post", response.required("request").required("method").textValue());
    assertTrue(
        response
            .required("request")
            .required("uri")
            .textValue()
            .contains("/pets/pet%2017?status=available%20now"));
    assertEquals("ssb", response.required("content").required("tenant").textValue());
    assertEquals(
        "operation-1", response.required("content").required("idempotencyKey").textValue());
    assertEquals(
        "extract persons",
        response.required("content").required("body").required("instruction").textValue());
    assertTrue(response.required("content").required("upgrade").isNull());
    assertTrue(response.required("content").required("http2Settings").isNull());
  }

  @Test
  void supportsRawBinaryAndTheSpecifiedRedirectAcceptanceRule() throws Exception {
    ObjectNode raw = JSON.createObjectNode();
    raw.put("method", "GET");
    raw.put("endpoint", baseUri + "/binary");
    raw.put("output", "raw");
    OperationObservation binary =
        new HttpCallAdapter()
            .execute(request(raw, JSON.createObjectNode()), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);
    assertEquals(
        Base64.getEncoder().encodeToString(new byte[] {0, 1, 2, (byte) 255}),
        binary.output().inlineValue().textValue());

    ObjectNode redirect = JSON.createObjectNode();
    redirect.put("method", "GET");
    redirect.put("endpoint", baseUri + "/redirect");
    OperationObservation rejected =
        new HttpCallAdapter()
            .execute(request(redirect, JSON.createObjectNode()), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);
    assertEquals(OperationObservationStatus.FAILED, rejected.status());
    assertEquals(302, rejected.error().status());

    redirect.put("redirect", true);
    redirect.put("output", "response");
    OperationObservation accepted =
        new HttpCallAdapter()
            .execute(request(redirect, JSON.createObjectNode()), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);
    assertEquals(OperationObservationStatus.SUCCEEDED, accepted.status());
    assertEquals(302, accepted.output().inlineValue().required("statusCode").intValue());
  }

  @Test
  void failsClosedUntilAuthenticationIsResolvedAtTheAuthorisedEdge() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "GET");
    ObjectNode endpoint = arguments.putObject("endpoint");
    endpoint.put("uri", baseUri + "/pets/1");
    endpoint.putObject("authentication").putObject("basic").put("username", "must-not-be-ignored");

    OperationObservation observation =
        new HttpCallAdapter()
            .execute(request(arguments, JSON.createObjectNode()), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertEquals(OperationObservationStatus.FAILED, observation.status());
    assertEquals(HttpCallAdapter.COMMUNICATION_ERROR, observation.error().type());
    assertTrue(observation.error().detail().contains("authorised adapter edge"));
  }

  @Test
  void completesAnRfc7616DigestChallenge() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "GET");
    arguments.put("endpoint", baseUri + "/digest");
    OperationRequest request =
        request(arguments, JSON.createObjectNode())
            .withAuthentication(
                new ResolvedAuthentication(
                    ResolvedAuthentication.Kind.DIGEST,
                    Map.of(
                        "username",
                        "digest-user".toCharArray(),
                        "password",
                        "digest-password".toCharArray())));

    OperationObservation observation;
    try {
      observation =
          new HttpCallAdapter()
              .execute(request, ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);
    } finally {
      request.authentication().close();
    }

    assertEquals(2, digestRequests.get());
    assertEquals(OperationObservationStatus.SUCCEEDED, observation.status());
    assertTrue(observation.output().inlineValue().required("ok").booleanValue());
  }

  @Test
  void cancellationProducesExplicitTerminalOutcomeAgainstRealServer() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "GET");
    arguments.put("endpoint", baseUri + "/slow");
    OperationRequest request = request(arguments, JSON.createObjectNode());
    HttpCallAdapter adapter = new HttpCallAdapter();
    CompletableFuture<OperationObservation> result =
        adapter.execute(request, ignored -> {}).toCompletableFuture();

    assertTrue(slowRequestStarted.await(10, TimeUnit.SECONDS));
    adapter.cancel(request).toCompletableFuture().get(2, TimeUnit.SECONDS);
    OperationObservation observation = result.get(2, TimeUnit.SECONDS);

    assertEquals(OperationObservationStatus.CANCELLED, observation.status());
    assertEquals(499, observation.error().status());
    assertEquals(request.operationId(), observation.error().instance());
  }

  @Test
  void shutdownCancelsTransportAndPreservesStableExternalIdentity() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "POST");
    arguments.put("endpoint", baseUri + "/slow");
    OperationRequest request = request(arguments, JSON.createObjectNode());
    HttpCallAdapter adapter = new HttpCallAdapter();
    CompletableFuture<OperationObservation> result =
        adapter.execute(request, ignored -> {}).toCompletableFuture();

    assertTrue(slowRequestStarted.await(10, TimeUnit.SECONDS));
    adapter.close();
    OperationObservation observation = result.get(2, TimeUnit.SECONDS);

    assertEquals(OperationObservationStatus.CANCELLED, observation.status());
    assertEquals(499, observation.error().status());
    assertTrue(observation.error().detail().contains("shut down"));
  }

  @Test
  void oversizedResponseCompletesWithExplicitFailureInsteadOfHanging() throws Exception {
    ObjectNode arguments = JSON.createObjectNode();
    arguments.put("method", "GET");
    arguments.put("endpoint", baseUri + "/oversized");

    OperationObservation observation =
        new HttpCallAdapter()
            .execute(request(arguments, JSON.createObjectNode()), ignored -> {})
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertEquals(OperationObservationStatus.FAILED, observation.status());
    assertTrue(observation.error().detail().contains("maximum is"));
  }

  private OperationRequest request(
      ObjectNode arguments, com.fasterxml.jackson.databind.JsonNode input) {
    ObjectNode descriptor = JSON.createObjectNode();
    descriptor.put("operationId", "operation-1");
    descriptor.put("operationKind", "call");
    descriptor.put(
        "executionKey",
        new ExecutionKey(TENANT, new WorkflowExecutionId("execution-1")).canonical());
    descriptor.put("definitionReference", "definition-reference-1");
    descriptor.put("definitionSha256", "a".repeat(64));
    descriptor.put("taskPath", "/do/0/http");
    descriptor.put("callKind", "HTTP");
    descriptor.set("arguments", arguments);
    descriptor.set("taskInput", input);
    return new OperationRequest(
        "operation-1",
        "call",
        "definition-reference-1",
        descriptor,
        null,
        new ActorContext(
            TENANT,
            ActorId.parse("did:web:tenant.example.com:actors:user-1"),
            ActorType.HUMAN,
            "User One",
            "ssb-public",
            Set.of("evidence-control"),
            null,
            Instant.parse("2026-07-29T12:00:00Z")));
  }

  private void pet(HttpExchange exchange) throws java.io.IOException {
    byte[] requestBody = exchange.getRequestBody().readAllBytes();
    ObjectNode response = JSON.createObjectNode();
    response.put("path", exchange.getRequestURI().getPath());
    response.put("query", exchange.getRequestURI().getRawQuery());
    response.put("tenant", exchange.getRequestHeaders().getFirst("x-tenant"));
    response.put(
        "idempotencyKey",
        exchange.getRequestHeaders().getFirst(HttpCallAdapter.IDEMPOTENCY_KEY_HEADER));
    response.put("upgrade", exchange.getRequestHeaders().getFirst("Upgrade"));
    response.put("http2Settings", exchange.getRequestHeaders().getFirst("HTTP2-Settings"));
    if (requestBody.length > 0) {
      response.set("body", JSON.readTree(requestBody));
    }
    respond(exchange, 200, "application/json", JSON.writeValueAsString(response));
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body)
      throws java.io.IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
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
