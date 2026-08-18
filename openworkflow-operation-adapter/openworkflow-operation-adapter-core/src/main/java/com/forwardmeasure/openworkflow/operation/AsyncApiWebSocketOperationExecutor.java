package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;

/** JDK WebSocket AsyncAPI publish/subscribe driver with back-pressured observations. */
public final class AsyncApiWebSocketOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final Connector connector;

  public AsyncApiWebSocketOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(
        timeout,
        Clock.systemUTC(),
        egress,
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(timeout)
            .build(),
        secrets,
        null);
  }

  AsyncApiWebSocketOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      HttpClient client,
      SecretProvider secrets,
      Connector connector) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    Objects.requireNonNull(client, "client");
    this.authentication =
        new HttpAuthenticationSupport(
            client, JSON, timeout, egress, Objects.requireNonNull(secrets, "secrets"));
    this.connector =
        connector == null
            ? (uri, headers, listener) -> {
              WebSocket.Builder builder = client.newWebSocketBuilder().connectTimeout(timeout);
              headers.forEach(builder::header);
              return builder.buildAsync(uri, listener);
            }
            : connector;
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
        || !operation.protocol().equals("ws")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "AsyncAPI WebSocket driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "WebSocket authentication does not support HTTP Digest challenges"));
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
        .thenCompose(credential -> connect(operation, sink, credential));
  }

  private CompletionStage<Done> connect(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    Map<String, String> headers = new LinkedHashMap<>();
    if (credential != null && credential.authorization() != null) {
      headers.put("Authorization", credential.authorization());
    }
    headers.put("X-OpenWorkflow-Operation", operation.operationId());
    var completion = new CompletableFuture<Done>();
    var listener = new Listener(operation, sink, completion, clock);
    connector
        .connect(operation.endpoint(), headers, listener)
        .whenComplete(
            (socket, failure) -> {
              if (failure != null) {
                completion.completeExceptionally(failure);
                return;
              }
              listener.connected(socket);
              if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
                String message = encode(operation.request());
                socket
                    .sendText(message, true)
                    .thenCompose(
                        ignored ->
                            sink.observe(
                                "sent",
                                JsonNodeFactory.instance
                                    .objectNode()
                                    .put("endpoint", operation.endpoint().toString()),
                                false,
                                true,
                                clock.instant()))
                    .whenComplete(
                        (disposition, sendFailure) -> {
                          socket.sendClose(WebSocket.NORMAL_CLOSURE, "complete");
                          if (sendFailure == null) {
                            completion.complete(Done.getInstance());
                          } else {
                            completion.completeExceptionally(sendFailure);
                          }
                        });
              } else {
                socket.request(1);
              }
            });
    completion.whenComplete(
        (done, failure) -> {
          if (completion.isCancelled()) listener.abort();
        });
    return completion;
  }

  private static String encode(JsonNode request) {
    JsonNode payload = request.has("payload") ? request.get("payload") : request;
    try {
      return JSON.writeValueAsString(payload);
    } catch (Exception failure) {
      throw new IllegalArgumentException("AsyncAPI WebSocket message cannot be encoded", failure);
    }
  }

  @FunctionalInterface
  interface Connector {
    CompletionStage<WebSocket> connect(
        URI endpoint, Map<String, String> headers, WebSocket.Listener listener);
  }

  private static final class Listener implements WebSocket.Listener {
    private final ProtocolOperationDescriptor operation;
    private final ObservationSink sink;
    private final CompletableFuture<Done> completion;
    private final Clock clock;
    private final AtomicReference<WebSocket> socket = new AtomicReference<>();
    private final AtomicLong sequence = new AtomicLong();
    private final StringBuilder text = new StringBuilder();
    private java.io.ByteArrayOutputStream binary = new java.io.ByteArrayOutputStream();

    private Listener(
        ProtocolOperationDescriptor operation,
        ObservationSink sink,
        CompletableFuture<Done> completion,
        Clock clock) {
      this.operation = operation;
      this.sink = sink;
      this.completion = completion;
      this.clock = clock;
    }

    void connected(WebSocket connected) {
      socket.set(connected);
      if (completion.isCancelled()) connected.abort();
    }

    void abort() {
      WebSocket connected = socket.get();
      if (connected != null) connected.abort();
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (!last) {
        webSocket.request(1);
        return null;
      }
      String value = text.toString();
      text.setLength(0);
      return observe(webSocket, decode(value));
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
      byte[] chunk = new byte[data.remaining()];
      data.get(chunk);
      binary.writeBytes(chunk);
      if (!last) {
        webSocket.request(1);
        return null;
      }
      byte[] value = binary.toByteArray();
      binary = new java.io.ByteArrayOutputStream();
      return observe(webSocket, decode(value));
    }

    private CompletionStage<?> observe(WebSocket webSocket, JsonNode value) {
      return sink.observe(
              Long.toString(sequence.getAndIncrement()), value, false, false, clock.instant())
          .whenComplete(
              (disposition, failure) -> {
                if (failure != null) {
                  webSocket.abort();
                  completion.completeExceptionally(failure);
                } else if (disposition == ObservationDisposition.STOP) {
                  webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "complete");
                  completion.complete(Done.getInstance());
                } else {
                  webSocket.request(1);
                }
              });
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      if (!completion.isDone()) {
        if (operation.mode() == ProtocolOperationDescriptor.Mode.SUBSCRIBE) {
          completion.completeExceptionally(
              new IllegalStateException(
                  "WebSocket subscription closed before its consumption policy completed: "
                      + statusCode
                      + " "
                      + reason));
        } else {
          completion.complete(Done.getInstance());
        }
      }
      return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      completion.completeExceptionally(error);
    }

    private static JsonNode decode(String value) {
      try {
        return JSON.readTree(value);
      } catch (Exception ignored) {
        return JsonNodeFactory.instance.textNode(value);
      }
    }

    private static JsonNode decode(byte[] value) {
      try {
        return JSON.readTree(value);
      } catch (Exception ignored) {
        return JsonNodeFactory.instance.binaryNode(value);
      }
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
