package com.forwardmeasure.openworkflow.operation.nats;

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
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;

/** Core NATS AsyncAPI publish/subscribe driver. */
public final class AsyncApiNatsOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final ConnectionFactory connections;

  public AsyncApiNatsOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, Nats::connect);
  }

  AsyncApiNatsOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      ConnectionFactory connections) {
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
    this.connections = Objects.requireNonNull(connections, "connections");
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
        || !operation.protocol().equals("nats")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI NATS driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("NATS does not support HTTP Digest authentication"));
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
        .thenCompose(credential -> connect(executionId, operation, sink, credential));
  }

  private CompletionStage<Done> connect(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    try {
      Options.Builder options =
          new Options.Builder()
              .server(server(operation))
              .connectionName(identity(executionId, operation))
              .connectionTimeout(timeout);
      if (credential != null) {
        if (credential.kind() == AuthenticationPlan.Kind.BASIC) {
          options.userInfo(credential.username(), credential.password());
        } else {
          options.token(bearerToken(credential.authorization()).toCharArray());
        }
      }
      Connection connection = connections.connect(options.build());
      return operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH
          ? publish(connection, operation, sink)
          : subscribe(connection, operation, sink);
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> publish(
      Connection connection, ProtocolOperationDescriptor operation, ObservationSink sink) {
    try {
      byte[] payload = JSON.writeValueAsBytes(payload(operation));
      connection.publish(subject(operation), payload);
      connection.flush(timeout);
      return sink.observe(
              operation.operationId(),
              JsonNodeFactory.instance.objectNode().put("subject", subject(operation)),
              false,
              true,
              clock.instant())
          .thenApply(disposition -> Done.getInstance())
          .whenComplete((done, failure) -> close(connection));
    } catch (Exception failure) {
      close(connection);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> subscribe(
      Connection connection, ProtocolOperationDescriptor operation, ObservationSink sink) {
    var completion = new CompletableFuture<Done>();
    var dispatcher = new AtomicReference<Dispatcher>();
    var sequence = new AtomicLong();
    Dispatcher created =
        connection.createDispatcher(
            message -> {
              if (completion.isDone()) return;
              synchronized (completion) {
                if (completion.isDone()) return;
                try {
                  ObservationDisposition disposition =
                      sink.observe(
                              observationId(message, sequence.getAndIncrement()),
                              decode(message.getData()),
                              false,
                              false,
                              clock.instant())
                          .toCompletableFuture()
                          .join();
                  if (disposition == ObservationDisposition.STOP) {
                    completion.complete(Done.getInstance());
                  }
                } catch (Exception failure) {
                  completion.completeExceptionally(failure);
                }
              }
            });
    dispatcher.set(created);
    created.subscribe(subject(operation));
    completion.whenComplete(
        (done, failure) -> {
          Dispatcher active = dispatcher.getAndSet(null);
          if (active != null) {
            try {
              connection.closeDispatcher(active);
            } catch (Exception ignored) {
            }
          }
          close(connection);
        });
    return completion;
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    JsonNode request = operation.request();
    return request.has("payload") ? request.get("payload") : request;
  }

  private static JsonNode decode(byte[] data) {
    try {
      return JSON.readTree(data);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.binaryNode(data);
    }
  }

  private static String server(ProtocolOperationDescriptor operation) {
    String value = operation.endpoint().toString();
    int path = value.indexOf('/', value.indexOf("//") + 2);
    return path < 0 ? value : value.substring(0, path);
  }

  private static String subject(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String subject = path == null ? "" : path.replaceFirst("^/+", "");
    if (subject.isBlank() || subject.contains("/")) {
      throw new IllegalArgumentException("AsyncAPI NATS channel must resolve to one subject");
    }
    return subject;
  }

  private static String identity(ExecutionId executionId, ProtocolOperationDescriptor operation) {
    return "ow-"
        + UUID.nameUUIDFromBytes(
            (executionId.entityId() + "|" + operation.operationId())
                .getBytes(StandardCharsets.UTF_8));
  }

  private static String bearerToken(String authorization) {
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("NATS bearer authentication requires a token");
    }
    return authorization.substring(7);
  }

  private static String observationId(Message message, long fallback) {
    return message.getSID() == null
        ? message.getSubject() + "-" + fallback
        : message.getSubject() + "-" + message.getSID();
  }

  private static void close(Connection connection) {
    try {
      connection.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  interface ConnectionFactory {
    Connection connect(Options options) throws Exception;
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
