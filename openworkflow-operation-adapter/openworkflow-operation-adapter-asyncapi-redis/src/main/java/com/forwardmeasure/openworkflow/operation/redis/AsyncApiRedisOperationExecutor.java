package com.forwardmeasure.openworkflow.operation.redis;

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
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;

/** Durable Redis Streams AsyncAPI publish/subscribe driver. */
public final class AsyncApiRedisOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final StreamFactory streams;

  public AsyncApiRedisOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, LettuceStream::new);
  }

  AsyncApiRedisOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      StreamFactory streams) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.authentication =
        new HttpAuthenticationSupport(
            HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(timeout)
                .build(),
            JSON,
            timeout,
            egress,
            Objects.requireNonNull(secrets, "secrets"));
    this.streams = Objects.requireNonNull(streams, "streams");
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
        || !operation.protocol().equals("redis")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI Redis driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Redis does not support HTTP Digest authentication"));
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
        .thenCompose(credential -> run(executionId, operation, sink, credential));
  }

  private CompletionStage<Done> run(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    Stream stream = null;
    try {
      stream = streams.open(server(operation), password(credential), timeout);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
        String id = stream.publish(name(operation), JSON.writeValueAsString(payload(operation)));
        Stream opened = stream;
        return sink.observe(
                id,
                JsonNodeFactory.instance.objectNode().put("stream", name(operation)).put("id", id),
                false,
                true,
                clock.instant())
            .thenApply(disposition -> Done.getInstance())
            .whenComplete((done, failure) -> close(opened));
      }
      String group = operation.request().path("headers").path("group").asText("openworkflow");
      String consumer =
          "ow-"
              + UUID.nameUUIDFromBytes(
                  (executionId.entityId() + "|" + operation.operationId()).getBytes());
      var completion = new CompletableFuture<Done>();
      Stream opened = stream;
      opened.subscribe(
          name(operation),
          group,
          consumer,
          timeout,
          (id, value, acknowledge) -> {
            if (completion.isDone()) return;
            synchronized (completion) {
              if (completion.isDone()) return;
              try {
                ObservationDisposition disposition =
                    sink.observe(id, decode(value), false, false, clock.instant())
                        .toCompletableFuture()
                        .join();
                acknowledge.run();
                if (disposition == ObservationDisposition.STOP) {
                  completion.complete(Done.getInstance());
                }
              } catch (Exception failure) {
                completion.completeExceptionally(failure);
              }
            }
          },
          completion);
      completion.whenComplete((done, failure) -> close(opened));
      return completion;
    } catch (Exception failure) {
      if (stream != null) close(stream);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    return operation.request().has("payload")
        ? operation.request().get("payload")
        : operation.request();
  }

  private static JsonNode decode(String value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.textNode(value);
    }
  }

  private static URI server(ProtocolOperationDescriptor operation) {
    URI endpoint = operation.endpoint();
    try {
      return new URI(
          endpoint.getScheme(),
          endpoint.getUserInfo(),
          endpoint.getHost(),
          endpoint.getPort(),
          null,
          null,
          null);
    } catch (Exception failure) {
      throw new IllegalArgumentException("Invalid Redis endpoint", failure);
    }
  }

  private static String name(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String value = path == null ? "" : path.replaceFirst("^/+", "");
    if (value.isBlank())
      throw new IllegalArgumentException(
          "AsyncAPI Redis stream must be present in the endpoint path");
    return value;
  }

  private static String password(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return null;
    if (credential.kind() == AuthenticationPlan.Kind.BASIC) {
      return credential.password();
    }
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("Redis bearer authentication requires a token");
    }
    return authorization.substring(7);
  }

  private static void close(Stream stream) {
    try {
      stream.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  interface StreamFactory {
    Stream open(URI server, String password, Duration timeout) throws Exception;
  }

  interface Stream {
    String publish(String stream, String value) throws Exception;

    void subscribe(
        String stream,
        String group,
        String consumer,
        Duration timeout,
        DeliveryHandler handler,
        CompletableFuture<Done> completion)
        throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String id, String value, Runnable acknowledge);
  }

  private static final class LettuceStream implements Stream {
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    private LettuceStream(URI server, String password, Duration timeout) {
      RedisURI.Builder uri =
          RedisURI.builder()
              .withHost(server.getHost())
              .withPort(server.getPort() < 0 ? 6379 : server.getPort())
              .withTimeout(timeout);
      if (server.getScheme().equals("rediss")) uri.withSsl(true);
      if (password != null) uri.withPassword(password.toCharArray());
      client = RedisClient.create(uri.build());
      connection = client.connect();
    }

    @Override
    public String publish(String stream, String value) {
      return connection.sync().xadd(stream, Map.of("payload", value));
    }

    @Override
    @SuppressWarnings("unchecked") // Lettuce exposes generic stream offsets only through varargs.
    public void subscribe(
        String stream,
        String group,
        String consumer,
        Duration timeout,
        DeliveryHandler handler,
        CompletableFuture<Done> completion) {
      try {
        connection
            .sync()
            .xgroupCreate(
                XReadArgs.StreamOffset.from(stream, "0-0"),
                group,
                XGroupCreateArgs.Builder.mkstream());
      } catch (io.lettuce.core.RedisBusyException ignored) {
      }
      Thread.ofVirtual()
          .name("openworkflow-redis-stream")
          .start(
              () -> {
                try {
                  while (!completion.isDone()) {
                    var messages =
                        connection
                            .sync()
                            .xreadgroup(
                                Consumer.from(group, consumer),
                                XReadArgs.Builder.block(timeout).count(1),
                                XReadArgs.StreamOffset.lastConsumed(stream));
                    for (var message : messages) {
                      handler.delivered(
                          message.getId(),
                          message.getBody().get("payload"),
                          () -> connection.sync().xack(stream, group, message.getId()));
                    }
                  }
                } catch (Exception failure) {
                  if (!completion.isDone()) completion.completeExceptionally(failure);
                }
              });
    }

    @Override
    public void close() {
      connection.close();
      client.shutdown();
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
