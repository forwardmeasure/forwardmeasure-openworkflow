package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.pekko.Done;

/** HTTP Server-Sent Events and newline-delimited AsyncAPI subscription driver. */
public final class AsyncApiSseOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpClient client;
  private final HttpAuthenticationSupport authentication;

  public AsyncApiSseOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(
        timeout,
        Clock.systemUTC(),
        egress,
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(timeout)
            .build(),
        secrets);
  }

  AsyncApiSseOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      HttpClient client,
      SecretProvider secrets) {
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
        || operation.mode() != ProtocolOperationDescriptor.Mode.SUBSCRIBE
        || !(operation.protocol().equals("http")
            || operation.protocol().equals("https")
            || operation.protocol().equals("mercure"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI SSE driver received an incompatible operation"));
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
        .thenCompose(credential -> subscribe(executionId, operation, sink, credential));
  }

  private CompletionStage<Done> subscribe(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    return request(operation, credential, null)
        .thenCompose(
            response -> {
              if (response.statusCode() == 401
                  && credential != null
                  && credential.kind() == AuthenticationPlan.Kind.DIGEST) {
                response.body().close();
                String challenge =
                    response
                        .headers()
                        .firstValue("WWW-Authenticate")
                        .orElseThrow(
                            () ->
                                new IllegalArgumentException(
                                    "Digest subscription challenge is missing"));
                String authorization =
                    authentication.digestAuthorization(
                        "GET",
                        operation.endpoint(),
                        new byte[0],
                        credential,
                        challenge,
                        operation.operationId());
                return request(operation, credential, authorization)
                    .thenCompose(retried -> consume(operation, sink, retried));
              }
              return consume(operation, sink, response);
            });
  }

  private CompletionStage<HttpResponse<Stream<String>>> request(
      ProtocolOperationDescriptor operation,
      HttpAuthenticationSupport.Credential credential,
      String authorizationOverride) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(operation.endpoint())
            .GET()
            .header("Accept", "text/event-stream, application/x-ndjson")
            .header("Cache-Control", "no-cache")
            .header("X-OpenWorkflow-Operation", operation.operationId());
    if (operation.request().path("headers").isObject()) {
      operation
          .request()
          .path("headers")
          .properties()
          .forEach(
              entry ->
                  request.header(
                      entry.getKey(),
                      entry.getValue().isValueNode()
                          ? entry.getValue().asText()
                          : entry.getValue().toString()));
    }
    String authorization =
        authorizationOverride != null
            ? authorizationOverride
            : credential == null ? null : credential.authorization();
    if (authorization != null
        && (credential == null
            || credential.kind() != AuthenticationPlan.Kind.DIGEST
            || authorizationOverride != null)) {
      request.header("Authorization", authorization);
    }
    return client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofLines());
  }

  private CompletionStage<Done> consume(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpResponse<Stream<String>> response) {
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      response.body().close();
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "AsyncAPI HTTP subscription returned status " + response.statusCode()));
    }
    var completion = new CompletableFuture<Done>();
    var active = new AtomicReference<Stream<String>>(response.body());
    completion.whenComplete(
        (done, failure) -> {
          Stream<String> lines = active.getAndSet(null);
          if (lines != null) lines.close();
        });
    Thread.ofVirtual()
        .name("openworkflow-sse-" + operation.operationId())
        .start(
            () -> {
              try (Stream<String> lines = response.body()) {
                var event = new SseEvent();
                var iterator = lines.iterator();
                long sequence = 0;
                while (!completion.isDone() && iterator.hasNext()) {
                  String line = iterator.next();
                  if (line.isEmpty()) {
                    if (event.hasData()) {
                      if (observe(sink, event.id(sequence), event.value(), completion)) break;
                      sequence++;
                      event = new SseEvent();
                    }
                  } else if (line.startsWith("data:")) {
                    event.append(line.substring(5).stripLeading());
                  } else if (line.startsWith("id:")) {
                    event.id(line.substring(3).strip());
                  } else if (!line.startsWith(":")) {
                    // A non-SSE HTTP stream is treated as NDJSON.
                    if (observe(sink, Long.toString(sequence), decode(line), completion)) break;
                    sequence++;
                  }
                }
                if (!completion.isDone() && event.hasData()) {
                  observe(sink, event.id(sequence), event.value(), completion);
                }
                if (!completion.isDone()) {
                  completion.completeExceptionally(
                      new IllegalStateException(
                          "HTTP subscription ended before its consumption policy completed"));
                }
              } catch (Exception failure) {
                if (!completion.isDone()) completion.completeExceptionally(failure);
              } finally {
                active.set(null);
              }
            });
    return completion;
  }

  private boolean observe(
      ObservationSink sink, String id, JsonNode value, CompletableFuture<Done> completion) {
    ObservationDisposition disposition =
        sink.observe(id, value, false, false, clock.instant()).toCompletableFuture().join();
    if (disposition == ObservationDisposition.STOP) {
      completion.complete(Done.getInstance());
      return true;
    }
    return false;
  }

  private static JsonNode decode(String value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.textNode(value);
    }
  }

  private static final class SseEvent {
    private String id;
    private final StringBuilder data = new StringBuilder();

    void id(String value) {
      id = value;
    }

    void append(String value) {
      if (!data.isEmpty()) data.append('\n');
      data.append(value);
    }

    boolean hasData() {
      return !data.isEmpty();
    }

    String id(long fallback) {
      return id == null || id.isBlank() ? Long.toString(fallback) : id;
    }

    JsonNode value() {
      return decode(data.toString());
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
