package com.forwardmeasure.openworkflow.operation.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.OperationTimeouts;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;

/** A2A JSON-RPC and MCP Streamable HTTP transport owned by the durable coordinator. */
public final class JsonRpcHttpOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Set<String> RESTRICTED =
      Set.of(
          "authorization",
          "content-length",
          "host",
          "connection",
          "transfer-encoding",
          "upgrade",
          "proxy-authorization");
  private final HttpClient client;
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final AgentCardSecurityResolver agentCardSecurity;

  public JsonRpcHttpOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, egress, secrets, AgentCardHttpClientProvider.rejecting());
  }

  public JsonRpcHttpOperationExecutor(
      Duration timeout,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      AgentCardHttpClientProvider mutualTls) {
    this(
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
        timeout,
        Clock.systemUTC(),
        egress,
        secrets,
        mutualTls);
  }

  JsonRpcHttpOperationExecutor(
      HttpClient client,
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets) {
    this(client, timeout, clock, egress, secrets, AgentCardHttpClientProvider.rejecting());
  }

  JsonRpcHttpOperationExecutor(
      HttpClient client,
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      AgentCardHttpClientProvider mutualTls) {
    this.client = Objects.requireNonNull(client, "client");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.authentication =
        new HttpAuthenticationSupport(
            client, JSON, timeout, egress, Objects.requireNonNull(secrets, "secrets"));
    this.agentCardSecurity = new AgentCardSecurityResolver(secrets, client, mutualTls);
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException("JSON-RPC timeout must be positive");
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.A2A
        && !(operation.kind() == ProtocolOperationDescriptor.Kind.MCP
            && operation.protocol().equals("mcp-http"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("HTTP JSON-RPC driver received an incompatible operation"));
    }
    try {
      egress.authorize(executionId.tenantId(), operation.endpoint());
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var owned = new CompletableFuture<Done>();
    var active = new AtomicReference<CompletableFuture<?>>();
    var activeBody = new AtomicReference<InputStream>();
    owned.whenComplete(
        (done, failure) -> {
          if (owned.isCancelled()) {
            CompletableFuture<?> request = active.get();
            if (request != null) request.cancel(true);
            InputStream body = activeBody.get();
            if (body != null)
              try {
                body.close();
              } catch (java.io.IOException ignored) {
              }
          }
        });
    JsonNode parameters =
        operation.kind() == ProtocolOperationDescriptor.Kind.MCP
            ? operation.request().path("parameters")
            : operation.request();
    CompletionStage<HttpResponse<InputStream>> dispatch =
        operation.kind() == ProtocolOperationDescriptor.Kind.MCP
            ? initializeMcp(executionId, operation, active)
                .thenCompose(
                    session ->
                        send(
                            executionId,
                            operation,
                            rpc(operation.operation(), parameters, operation.operationId()),
                            active,
                            session))
            : send(
                executionId,
                operation,
                rpc(operation.operation(), parameters, operation.operationId()),
                active,
                null);
    dispatch.whenComplete(
        (response, failure) -> {
          if (owned.isDone()) return;
          if (failure != null) {
            observeFailure(operation, sink, failure, owned);
            return;
          }
          Thread.ofVirtual()
              .name("openworkflow-jsonrpc-" + operation.operationId())
              .start(() -> consume(operation, sink, response, activeBody, owned));
        });
    return owned;
  }

  private CompletionStage<String> initializeMcp(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      AtomicReference<CompletableFuture<?>> active) {
    JsonNode configured = operation.request();
    ObjectNode parameters =
        JsonNodeFactory.instance
            .objectNode()
            .put("protocolVersion", configured.path("protocolVersion").asText("2025-06-18"));
    parameters.set("capabilities", JsonNodeFactory.instance.objectNode());
    JsonNode clientInfo = configured.path("client");
    parameters.set(
        "clientInfo",
        clientInfo.isObject()
            ? clientInfo.deepCopy()
            : JsonNodeFactory.instance
                .objectNode()
                .put("name", "openworkflow-actor-engine")
                .put("version", "1.0.0"));
    return send(
            executionId,
            operation,
            rpc("initialize", parameters, operation.operationId() + "-initialize"),
            active,
            null)
        .thenCompose(
            response -> {
              try (InputStream body = response.body()) {
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                  throw new IllegalStateException(
                      "MCP initialize HTTP status " + response.statusCode());
                }
                JsonNode envelope = JSON.readTree(body);
                if (envelope == null || envelope.has("error")) {
                  throw new IllegalStateException("MCP initialize failed: " + envelope);
                }
                String session = response.headers().firstValue("Mcp-Session-Id").orElse(null);
                return send(
                        executionId,
                        operation,
                        notification(
                            "notifications/initialized", JsonNodeFactory.instance.objectNode()),
                        active,
                        session)
                    .thenApply(
                        initialized -> {
                          try (InputStream ignored = initialized.body()) {
                            ignored.transferTo(java.io.OutputStream.nullOutputStream());
                            if (initialized.statusCode() < 200 || initialized.statusCode() >= 300) {
                              throw new IllegalStateException(
                                  "MCP initialized notification HTTP status "
                                      + initialized.statusCode());
                            }
                          } catch (java.io.IOException failure) {
                            throw new java.io.UncheckedIOException(failure);
                          }
                          return session;
                        });
              } catch (java.io.IOException failure) {
                throw new java.io.UncheckedIOException(failure);
              }
            });
  }

  private CompletionStage<HttpResponse<InputStream>> send(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      JsonNode body,
      AtomicReference<CompletableFuture<?>> active,
      String session) {
    byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
    return authentication
        .resolve(
            executionId,
            operation.authentication(),
            operation.authenticationContext(),
            operation.operationId())
        .thenCompose(
            credential -> {
              AgentCardSecurityResolver.Selection cardSecurity =
                  agentCardSecurity.select(executionId.tenantId(), operation);
              return sendAuthenticated(
                  executionId, operation, payload, active, session, credential, cardSecurity);
            });
  }

  private CompletionStage<HttpResponse<InputStream>> sendAuthenticated(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      byte[] payload,
      AtomicReference<CompletableFuture<?>> active,
      String session,
      HttpAuthenticationSupport.Credential credential,
      AgentCardSecurityResolver.Selection cardSecurity) {
    if (cardSecurity.digest() != null && credential != null) {
      throw new IllegalArgumentException(
          "Call authentication conflicts with AgentCard Digest security");
    }
    HttpAuthenticationSupport.Credential effectiveCredential =
        cardSecurity.digest() == null ? credential : cardSecurity.digest();
    if (effectiveCredential != null
        && effectiveCredential.kind()
            == com.forwardmeasure.openworkflow.definition.AuthenticationPlan.Kind.DIGEST) {
      return dispatch(executionId, operation, payload, active, session, null, cardSecurity)
          .thenCompose(
              response -> {
                if (response.statusCode() != 401) {
                  return CompletableFuture.completedFuture(response);
                }
                String challenge =
                    response.headers().allValues("www-authenticate").stream()
                        .filter(value -> value.regionMatches(true, 0, "Digest ", 0, 7))
                        .findFirst()
                        .orElseThrow(
                            () ->
                                new IllegalArgumentException(
                                    "Digest endpoint returned 401 without a Digest challenge"));
                try {
                  response.body().close();
                } catch (java.io.IOException ignored) {
                }
                String authorization =
                    authentication.digestAuthorization(
                        "POST",
                        cardSecurity.endpoint(),
                        payload,
                        effectiveCredential,
                        challenge,
                        operation.operationId());
                return dispatch(
                    executionId, operation, payload, active, session, authorization, cardSecurity);
              });
    }
    return dispatch(
        executionId,
        operation,
        payload,
        active,
        session,
        effectiveCredential == null ? null : effectiveCredential.authorization(),
        cardSecurity);
  }

  private CompletableFuture<HttpResponse<InputStream>> dispatch(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      byte[] payload,
      AtomicReference<CompletableFuture<?>> active,
      String session,
      String authorization,
      AgentCardSecurityResolver.Selection cardSecurity) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(cardSecurity.endpoint())
            .timeout(
                OperationTimeouts.configuredOrMaximum(
                    operation.kind() == ProtocolOperationDescriptor.Kind.MCP
                        ? operation.request().path("timeout")
                        : null,
                    timeout))
            .header("Content-Type", "application/json")
            .header(
                "Accept",
                operation.mode() == ProtocolOperationDescriptor.Mode.RPC_STREAM
                    ? "text/event-stream, application/json"
                    : "application/json")
            .header("X-OpenWorkflow-Tenant", executionId.tenantId().value().toString())
            .header("Idempotency-Key", operation.operationId());
    if (operation.kind() == ProtocolOperationDescriptor.Kind.MCP) {
      builder.header(
          "MCP-Protocol-Version", operation.request().path("protocolVersion").asText("2025-06-18"));
      if (session != null && !session.isBlank()) {
        builder.header("Mcp-Session-Id", session);
      }
    }
    JsonNode headers =
        operation.kind() == ProtocolOperationDescriptor.Kind.MCP
            ? operation.request().path("transport").path("http").path("headers")
            : JsonNodeFactory.instance.missingNode();
    headers
        .properties()
        .forEach(
            entry -> {
              if (RESTRICTED.contains(entry.getKey().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                    "Restricted JSON-RPC request header: " + entry.getKey());
              }
              builder.header(entry.getKey(), entry.getValue().asText());
            });
    cardSecurity
        .headers()
        .forEach(
            (name, value) -> {
              if (RESTRICTED.contains(name.toLowerCase(Locale.ROOT))
                  && !name.equalsIgnoreCase("authorization")) {
                throw new IllegalArgumentException("Restricted AgentCard security header: " + name);
              }
              if (authorization != null && name.equalsIgnoreCase("authorization")) {
                throw new IllegalArgumentException(
                    "Call authentication conflicts with AgentCard security");
              }
              builder.header(name, value);
            });
    if (authorization != null) builder.header("Authorization", authorization);
    CompletableFuture<HttpResponse<InputStream>> request =
        cardSecurity
            .client()
            .sendAsync(
                builder.POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(),
                HttpResponse.BodyHandlers.ofInputStream());
    active.set(request);
    return request;
  }

  private void consume(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpResponse<InputStream> response,
      AtomicReference<InputStream> activeBody,
      CompletableFuture<Done> owned) {
    activeBody.set(response.body());
    try (InputStream input = response.body()) {
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalStateException("JSON-RPC HTTP status " + response.statusCode());
      }
      String contentType =
          response
              .headers()
              .firstValue("content-type")
              .orElse("application/json")
              .toLowerCase(Locale.ROOT);
      if (contentType.contains("text/event-stream")) {
        consumeEvents(operation, sink, input, owned);
      } else {
        JsonNode envelope = JSON.readTree(input);
        observeEnvelope(operation, sink, envelope, 0, true, owned);
      }
    } catch (Exception failure) {
      if (!owned.isDone()) observeFailure(operation, sink, failure, owned);
    } finally {
      activeBody.compareAndSet(response.body(), null);
    }
  }

  private void consumeEvents(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      InputStream input,
      CompletableFuture<Done> owned)
      throws Exception {
    try (var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      int index = 0;
      while (!owned.isDone() && (line = reader.readLine()) != null) {
        if (!line.startsWith("data:")) continue;
        String data = line.substring(5).stripLeading();
        if (data.isBlank() || data.equals("[DONE]")) continue;
        JsonNode envelope = JSON.readTree(data);
        ObservationDisposition disposition =
            observeEnvelope(operation, sink, envelope, index++, false, owned);
        if (disposition == ObservationDisposition.STOP) {
          owned.complete(Done.getInstance());
          return;
        }
      }
      if (!owned.isDone())
        sink.observe(
                "terminal-" + index,
                JsonNodeFactory.instance.nullNode(),
                false,
                true,
                clock.instant())
            .whenComplete((ignored, failure) -> complete(owned, failure));
    }
  }

  private ObservationDisposition observeEnvelope(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      JsonNode envelope,
      int index,
      boolean terminal,
      CompletableFuture<Done> owned) {
    boolean failed = envelope.has("error");
    JsonNode value = failed ? problem(envelope.path("error")) : envelope.path("result");
    try {
      ObservationDisposition disposition =
          sink.observe("response-" + index, value, failed, terminal || failed, clock.instant())
              .toCompletableFuture()
              .join();
      if (terminal || failed || disposition == ObservationDisposition.STOP) {
        owned.complete(Done.getInstance());
      }
      return disposition;
    } catch (Exception failure) {
      owned.completeExceptionally(failure);
      return ObservationDisposition.STOP;
    }
  }

  private void observeFailure(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      Throwable failure,
      CompletableFuture<Done> owned) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    ObjectNode problem =
        JsonNodeFactory.instance
            .objectNode()
            .put("type", "urn:openworkflow:jsonrpc:transport")
            .put("status", 502)
            .put("title", "JSON-RPC transport failed")
            .put(
                "detail",
                root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage());
    sink.observe("transport-failure", problem, true, true, clock.instant())
        .whenComplete((ignored, observationFailure) -> complete(owned, observationFailure));
  }

  private static ObjectNode rpc(String method, JsonNode parameters, String id) {
    ObjectNode request =
        JsonNodeFactory.instance
            .objectNode()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("method", method);
    request.set(
        "params",
        parameters == null ? JsonNodeFactory.instance.objectNode() : parameters.deepCopy());
    return request;
  }

  private static ObjectNode notification(String method, JsonNode parameters) {
    ObjectNode request =
        JsonNodeFactory.instance.objectNode().put("jsonrpc", "2.0").put("method", method);
    request.set("params", parameters.deepCopy());
    return request;
  }

  private static ObjectNode problem(JsonNode error) {
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:jsonrpc:error")
        .put("status", 502)
        .put("title", "JSON-RPC operation failed")
        .put("detail", error.path("message").asText("Remote JSON-RPC error"))
        .set("extension", error.deepCopy());
  }

  private static void complete(CompletableFuture<Done> result, Throwable failure) {
    if (failure == null) result.complete(Done.getInstance());
    else result.completeExceptionally(failure);
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
