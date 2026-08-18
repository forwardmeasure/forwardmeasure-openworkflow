package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JdkHttpOperationExecutorTest {
  @TempDir Path temporaryDirectory;

  @Test
  void sendsTenantScopedIdempotentAuthenticatedRequestAndDecodesResponse() throws Exception {
    var observed = new java.util.concurrent.atomic.AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/items",
        exchange -> {
          observed.set(
              exchange.getRequestMethod()
                  + "|"
                  + exchange.getRequestHeaders().getFirst("Idempotency-Key")
                  + "|"
                  + exchange.getRequestHeaders().getFirst("X-OpenWorkflow-Tenant")
                  + "|"
                  + exchange.getRequestHeaders().getFirst("Authorization")
                  + "|"
                  + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] response = "{\"accepted\":true}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      var tenant = new TenantId("did:web:forwardmeasure.com:tenant:a");
      var executionId = new ExecutionId(tenant, UUID.randomUUID());
      var operation =
          new HttpOperationDescriptor(
              "operation-1",
              HttpOperationDescriptor.Kind.HTTP,
              "POST",
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/items"),
              Map.of("Content-Type", "application/json"),
              JsonNodeFactory.instance.objectNode().put("name", "evidence"),
              HttpOperationDescriptor.Output.CONTENT,
              false,
              null,
              null,
              new AuthenticationPlan(AuthenticationPlan.Kind.BEARER, null, "api-token"));
      var executor =
          new JdkHttpOperationExecutor(
              new ObjectMapper(),
              Duration.ofSeconds(3),
              (routedTenant, name) -> {
                assertEquals(tenant, routedTenant);
                assertEquals("api-token", name);
                return "secret-token".toCharArray();
              },
              HttpEgressPolicy.allowAllForTesting());

      HttpOperationResult result =
          executor.execute(executionId, operation).toCompletableFuture().join();
      assertNull(result.error());
      assertTrue(result.output().required("accepted").booleanValue());
      assertEquals(
          "POST|operation-1|" + tenant + "|Bearer" + " secret-token|{\"name\":\"evidence\"}",
          observed.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void responseOutputIncludesCredentialFreeRequestAndResponseMetadata() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/pets/1",
        exchange -> {
          byte[] response = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      var tenant = new TenantId("did:web:forwardmeasure.com:tenant:response");
      URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/pets/1");
      var operation =
          new HttpOperationDescriptor(
              "operation-response",
              HttpOperationDescriptor.Kind.HTTP,
              "GET",
              uri,
              Map.of("Accept", "application/json"),
              JsonNodeFactory.instance.nullNode(),
              HttpOperationDescriptor.Output.RESPONSE,
              false,
              null,
              null,
              null);
      HttpOperationResult result =
          new JdkHttpOperationExecutor(
                  new ObjectMapper(),
                  Duration.ofSeconds(3),
                  SecretProvider.rejecting(),
                  HttpEgressPolicy.allowAllForTesting())
              .execute(new ExecutionId(tenant, UUID.randomUUID()), operation)
              .toCompletableFuture()
              .join();
      assertNull(result.error());
      assertEquals("GET", result.output().required("request").required("method").textValue());
      assertEquals(uri.toString(), result.output().required("request").required("uri").textValue());
      assertEquals(
          "application/json",
          result.output().required("request").required("headers").required("Accept").textValue());
      assertEquals(200, result.output().required("statusCode").intValue());
      assertEquals(1, result.output().required("content").required("id").intValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void deniedEgressBecomesStructuredFailureWithoutNetworkAccess() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:denied");
    var operation =
        new HttpOperationDescriptor(
            "operation-denied",
            HttpOperationDescriptor.Kind.HTTP,
            "GET",
            URI.create("https://denied.example.test/items"),
            Map.of(),
            JsonNodeFactory.instance.nullNode(),
            HttpOperationDescriptor.Output.CONTENT,
            false,
            null,
            null,
            null);
    var executor =
        new JdkHttpOperationExecutor(
            new ObjectMapper(),
            Duration.ofSeconds(1),
            SecretProvider.rejecting(),
            new AllowlistedHttpEgressPolicy(Map.of()));

    HttpOperationResult result =
        executor
            .execute(new ExecutionId(tenant, UUID.randomUUID()), operation)
            .toCompletableFuture()
            .join();
    assertNull(result.output());
    assertEquals(403, result.error().required("status").intValue());
    assertEquals(
        "urn:openworkflow:operation:operation-denied",
        result.error().required("instance").asText());
  }

  @Test
  void directorySecretsUseTheForwardMeasureTenantDidAsTheirBoundary() throws Exception {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:a");
    Path tenantDirectory = temporaryDirectory.resolve(tenant.value().toString());
    Files.createDirectories(tenantDirectory);
    Files.writeString(
        tenantDirectory.resolve("api-token"), "tenant-secret\n", StandardCharsets.UTF_8);

    char[] resolved = new DirectorySecretProvider(temporaryDirectory).resolve(tenant, "api-token");

    assertEquals("tenant-secret", new String(resolved));
  }

  @Test
  void evaluatesExpressionBackedAuthenticationOnlyAtTheTenantEgressEdge() throws Exception {
    var authorization = new java.util.concurrent.atomic.AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/secured",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });
    server.start();
    try {
      var tenant = new TenantId("did:web:forwardmeasure.com:tenant:dynamic-auth");
      var configuration =
          JsonNodeFactory.instance.objectNode().put("token", "${ $secrets[\"api-token\"] }");
      var context =
          new AuthenticationExpressionContext(
              null, JsonNodeFactory.instance.objectNode(), null, null, null, null, null, Map.of());
      var operation =
          new HttpOperationDescriptor(
              "operation-dynamic-auth",
              HttpOperationDescriptor.Kind.HTTP,
              "GET",
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/secured"),
              Map.of(),
              JsonNodeFactory.instance.nullNode(),
              HttpOperationDescriptor.Output.CONTENT,
              false,
              null,
              null,
              AuthenticationPlan.expressions(
                  AuthenticationPlan.Kind.BEARER,
                  null,
                  configuration,
                  java.util.List.of("api-token")),
              context);
      var executor =
          new JdkHttpOperationExecutor(
              new ObjectMapper(),
              Duration.ofSeconds(3),
              (routedTenant, name) -> {
                assertEquals(tenant, routedTenant);
                assertEquals("api-token", name);
                return "edge-only-token".toCharArray();
              },
              HttpEgressPolicy.allowAllForTesting());

      HttpOperationResult result =
          executor
              .execute(new ExecutionId(tenant, UUID.randomUUID()), operation)
              .toCompletableFuture()
              .join();

      assertNull(result.error());
      assertEquals("Bearer edge-only-token", authorization.get());
      assertTrue(!operation.toString().contains("edge-only-token"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void answersDigestChallengeAndRetriesTheSameIdempotentOperation() throws Exception {
    var attempts = new java.util.concurrent.atomic.AtomicInteger();
    var digestHeader = new java.util.concurrent.atomic.AtomicReference<String>();
    var idempotency = new java.util.concurrent.atomic.AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/digest",
        exchange -> {
          int attempt = attempts.incrementAndGet();
          idempotency.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
          if (attempt == 1) {
            exchange
                .getResponseHeaders()
                .add(
                    "WWW-Authenticate",
                    "Digest realm=\"evidence\", nonce=\"nonce-1\", "
                        + "algorithm=SHA-256, qop=\"auth\"");
            exchange.sendResponseHeaders(401, -1);
          } else {
            digestHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            exchange.sendResponseHeaders(204, -1);
          }
          exchange.close();
        });
    server.start();
    try {
      var tenant = new TenantId("did:web:forwardmeasure.com:tenant:digest");
      var operation =
          new HttpOperationDescriptor(
              "operation-digest",
              HttpOperationDescriptor.Kind.HTTP,
              "GET",
              URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/digest"),
              Map.of(),
              JsonNodeFactory.instance.nullNode(),
              HttpOperationDescriptor.Output.CONTENT,
              false,
              null,
              null,
              new AuthenticationPlan(AuthenticationPlan.Kind.DIGEST, null, "digest-user"));
      var executor =
          new JdkHttpOperationExecutor(
              new ObjectMapper(),
              Duration.ofSeconds(3),
              (routedTenant, name) -> "alice:secret".toCharArray(),
              HttpEgressPolicy.allowAllForTesting());

      HttpOperationResult result =
          executor
              .execute(new ExecutionId(tenant, UUID.randomUUID()), operation)
              .toCompletableFuture()
              .join();

      assertNull(result.error());
      assertEquals(2, attempts.get());
      assertEquals("operation-digest", idempotency.get());
      assertTrue(digestHeader.get().startsWith("Digest username=\"alice\""));
      assertTrue(digestHeader.get().contains("algorithm=SHA-256"));
      assertTrue(digestHeader.get().contains("qop=auth"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void obtainsOAuthClientCredentialsTokenAtTheTenantEdge() throws Exception {
    var tokenRequest = new java.util.concurrent.atomic.AtomicReference<String>();
    var resourceAuthorization = new java.util.concurrent.atomic.AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/oauth2/token",
        exchange -> {
          tokenRequest.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] response =
              "{\"access_token\":\"issued-token\",\"token_type\":\"Bearer\"}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.createContext(
        "/resource",
        exchange -> {
          resourceAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });
    server.start();
    try {
      var tenant = new TenantId("did:web:forwardmeasure.com:tenant:oauth");
      String authority = "http://127.0.0.1:" + server.getAddress().getPort();
      var configuration =
          JsonNodeFactory.instance
              .objectNode()
              .put("authority", authority)
              .put("grant", "client_credentials");
      configuration
          .putObject("client")
          .put("id", "workflow-client")
          .put("secret", "${ $secrets.clientSecret }");
      configuration.putArray("scopes").add("workflow.execute");
      var operation =
          authenticatedOperation(
              "operation-oauth",
              authority + "/resource",
              AuthenticationPlan.expressions(
                  AuthenticationPlan.Kind.OAUTH2,
                  null,
                  configuration,
                  java.util.List.of("clientSecret")));
      var executor =
          new JdkHttpOperationExecutor(
              new ObjectMapper(),
              Duration.ofSeconds(3),
              (routedTenant, name) -> "client-secret".toCharArray(),
              HttpEgressPolicy.allowAllForTesting());

      HttpOperationResult result =
          executor
              .execute(new ExecutionId(tenant, UUID.randomUUID()), operation)
              .toCompletableFuture()
              .join();

      assertNull(result.error());
      assertTrue(tokenRequest.get().contains("grant_type=client_credentials"));
      assertTrue(tokenRequest.get().contains("client_id=workflow-client"));
      assertTrue(tokenRequest.get().contains("client_secret=client-secret"));
      assertTrue(tokenRequest.get().contains("scope=workflow.execute"));
      assertEquals("Bearer issued-token", resourceAuthorization.get());
      assertTrue(!operation.toString().contains("client-secret"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void discoversOidcTokenEndpointBeforeDispatch() throws Exception {
    var discoverySeen = new java.util.concurrent.atomic.AtomicBoolean();
    var resourceAuthorization = new java.util.concurrent.atomic.AtomicReference<String>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
    try {
      String authority = "http://127.0.0.1:" + server.getAddress().getPort();
      server.createContext(
          "/.well-known/openid-configuration",
          exchange -> {
            discoverySeen.set(true);
            byte[] response =
                ("{\"token_endpoint\":\"" + authority + "/oidc/token\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
          });
      server.createContext(
          "/oidc/token",
          exchange -> {
            byte[] response = "{\"access_token\":\"oidc-token\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
          });
      server.createContext(
          "/oidc-resource",
          exchange -> {
            resourceAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
          });
      var configuration =
          JsonNodeFactory.instance
              .objectNode()
              .put("authority", authority)
              .put("grant", "client_credentials");
      configuration.putObject("client").put("id", "oidc-client").put("authentication", "none");
      var operation =
          authenticatedOperation(
              "operation-oidc",
              authority + "/oidc-resource",
              AuthenticationPlan.expressions(
                  AuthenticationPlan.Kind.OIDC, null, configuration, java.util.List.of()));
      var executor =
          new JdkHttpOperationExecutor(
              new ObjectMapper(),
              Duration.ofSeconds(3),
              SecretProvider.rejecting(),
              HttpEgressPolicy.allowAllForTesting());

      HttpOperationResult result =
          executor
              .execute(
                  new ExecutionId(
                      new TenantId("did:web:forwardmeasure.com:tenant:oidc"), UUID.randomUUID()),
                  operation)
              .toCompletableFuture()
              .join();

      assertNull(result.error());
      assertTrue(discoverySeen.get());
      assertEquals("Bearer oidc-token", resourceAuthorization.get());
    } finally {
      server.stop(0);
    }
  }

  private static HttpOperationDescriptor authenticatedOperation(
      String operationId, String endpoint, AuthenticationPlan authentication) {
    return new HttpOperationDescriptor(
        operationId,
        HttpOperationDescriptor.Kind.HTTP,
        "GET",
        URI.create(endpoint),
        Map.of(),
        JsonNodeFactory.instance.nullNode(),
        HttpOperationDescriptor.Output.CONTENT,
        false,
        null,
        null,
        authentication,
        new AuthenticationExpressionContext(
            null, JsonNodeFactory.instance.objectNode(), null, null, null, null, null, Map.of()));
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
