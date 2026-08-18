package com.forwardmeasure.openworkflow.operation.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;

/** MuleSoft Anypoint MQ Broker REST API publish/consume/ack driver. */
public final class AsyncApiAnypointMqOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final HttpClient client;

  public AsyncApiAnypointMqOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(
        timeout,
        Clock.systemUTC(),
        egress,
        secrets,
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(timeout)
            .build());
  }

  AsyncApiAnypointMqOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      HttpClient client) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.client = Objects.requireNonNull(client, "client");
    this.authentication =
        new HttpAuthenticationSupport(
            client, JSON, timeout, egress, Objects.requireNonNull(secrets, "secrets"));
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.ASYNC_API
        || !operation.protocol().equals("anypointmq")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Anypoint MQ driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && (operation.authentication().kind() == AuthenticationPlan.Kind.BASIC
            || operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Anypoint MQ requires bearer, OAuth, or OIDC authentication"));
    }
    try {
      egress.authorize(executionId.tenantId(), operation.endpoint());
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return authentication
        .resolve(
            executionId,
            operation.authentication(),
            operation.authenticationContext(),
            operation.operationId())
        .thenCompose(
            credential ->
                operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH
                    ? publish(operation, sink, token(credential))
                    : subscribe(operation, sink, token(credential)));
  }

  private CompletionStage<Done> publish(
      ProtocolOperationDescriptor operation, ObservationSink sink, String token) {
    try {
      String id =
          UUID.nameUUIDFromBytes(operation.operationId().getBytes(StandardCharsets.UTF_8))
              .toString();
      var body = JsonNodeFactory.instance.objectNode();
      body.put("body", JSON.writeValueAsString(payload(operation)));
      HttpResponse<byte[]> response =
          client.send(
              request(URI.create(messages(operation) + "/" + id), token)
                  .PUT(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());
      requireSuccess(response, "publish");
      return sink.observe(
              id,
              JsonNodeFactory.instance.objectNode().put("messageId", id),
              false,
              true,
              clock.instant())
          .thenApply(disposition -> Done.getInstance());
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> subscribe(
      ProtocolOperationDescriptor operation, ObservationSink sink, String token) {
    var completion = new CompletableFuture<Done>();
    Thread.ofVirtual()
        .name("openworkflow-anypoint-mq")
        .start(
            () -> {
              try {
                while (!completion.isDone()) {
                  URI uri =
                      URI.create(
                          messages(operation)
                              + "?pollingTime="
                              + Math.min(20000, timeout.toMillis())
                              + "&batchSize=1&lockTtl="
                              + Math.max(120000, timeout.toMillis()));
                  HttpResponse<byte[]> response =
                      client.send(
                          request(uri, token).GET().build(),
                          HttpResponse.BodyHandlers.ofByteArray());
                  requireSuccess(response, "consume");
                  JsonNode decoded = JSON.readTree(response.body());
                  if (decoded == null || decoded.isNull()) continue;
                  if (decoded.isArray()) {
                    for (JsonNode message : decoded) {
                      deliver(operation, sink, token, completion, message);
                      if (completion.isDone()) break;
                    }
                  } else if (decoded.isObject() && !decoded.isEmpty()) {
                    deliver(operation, sink, token, completion, decoded);
                  }
                }
              } catch (Exception failure) {
                if (!completion.isDone()) completion.completeExceptionally(failure);
              }
            });
    return completion;
  }

  private void deliver(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      String token,
      CompletableFuture<Done> completion,
      JsonNode message)
      throws Exception {
    JsonNode headers = message.path("headers");
    String id = headers.required("messageId").asText();
    String lockId = headers.required("lockId").asText();
    JsonNode body = message.path("body");
    JsonNode value = body.isTextual() ? parseBody(body.asText()) : body;
    ObservationDisposition disposition =
        sink.observe(id, value, false, false, clock.instant()).toCompletableFuture().join();
    acknowledge(operation, token, id, lockId);
    if (disposition == ObservationDisposition.STOP) {
      completion.complete(Done.getInstance());
    }
  }

  private void acknowledge(
      ProtocolOperationDescriptor operation, String token, String id, String lockId)
      throws Exception {
    var item = JsonNodeFactory.instance.objectNode().put("messageId", id).put("lockId", lockId);
    byte[] body = JSON.writeValueAsBytes(JsonNodeFactory.instance.arrayNode().add(item));
    HttpResponse<byte[]> response =
        client.send(
            request(URI.create(messages(operation)), token)
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(body))
                .build(),
            HttpResponse.BodyHandlers.ofByteArray());
    requireSuccess(response, "acknowledge");
  }

  private HttpRequest.Builder request(URI uri, String token) {
    return HttpRequest.newBuilder(uri)
        .timeout(timeout)
        .header("Authorization", "Bearer " + token)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-cache");
  }

  private static String messages(ProtocolOperationDescriptor operation) {
    String endpoint = operation.endpoint().toString().replaceFirst("/+$", "");
    return endpoint.endsWith("/messages") ? endpoint : endpoint + "/messages";
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    return operation.request().has("payload")
        ? operation.request().get("payload")
        : operation.request();
  }

  private static JsonNode parseBody(String body) {
    try {
      return JSON.readTree(body);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.textNode(body);
    }
  }

  private static String token(HttpAuthenticationSupport.Credential credential) {
    if (credential == null
        || credential.authorization() == null
        || !credential.authorization().regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("Anypoint MQ requires a resolved bearer token");
    }
    return credential.authorization().substring(7);
  }

  private static void requireSuccess(HttpResponse<byte[]> response, String action) {
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IllegalStateException(
          "Anypoint MQ " + action + " returned status " + response.statusCode());
    }
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
