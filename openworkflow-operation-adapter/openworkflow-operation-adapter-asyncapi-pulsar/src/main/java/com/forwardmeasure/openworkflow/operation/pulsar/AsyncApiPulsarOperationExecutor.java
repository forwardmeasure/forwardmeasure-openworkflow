package com.forwardmeasure.openworkflow.operation.pulsar;

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
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

/** Native Apache Pulsar AsyncAPI publish/subscribe driver. */
public final class AsyncApiPulsarOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final ClientFactory clients;

  public AsyncApiPulsarOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, AsyncApiPulsarOperationExecutor::openClient);
  }

  AsyncApiPulsarOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      ClientFactory clients) {
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
    this.clients = Objects.requireNonNull(clients, "clients");
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
        || !operation.protocol().equals("pulsar")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "AsyncAPI Pulsar driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && (operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST
            || operation.authentication().kind() == AuthenticationPlan.Kind.BASIC)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Pulsar driver requires token, OAuth, or OIDC authentication"));
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
        .thenCompose(credential -> run(executionId, operation, sink, token(credential)));
  }

  private CompletionStage<Done> run(
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      String token) {
    Client client = null;
    try {
      client = clients.open(server(operation), token, timeout);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
        String id =
            client.publish(topic(operation), JSON.writeValueAsBytes(payload(operation)), timeout);
        Client opened = client;
        return sink.observe(
                id,
                JsonNodeFactory.instance
                    .objectNode()
                    .put("topic", topic(operation))
                    .put("messageId", id),
                false,
                true,
                clock.instant())
            .thenApply(disposition -> Done.getInstance())
            .whenComplete((done, failure) -> close(opened));
      }
      var completion = new CompletableFuture<Done>();
      Client opened = client;
      String subscription =
          operation.request().path("headers").path("subscription").asText("openworkflow");
      String consumer =
          "ow-"
              + UUID.nameUUIDFromBytes(
                  (executionId.entityId() + "|" + operation.operationId()).getBytes());
      opened.subscribe(
          topic(operation),
          subscription,
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
      if (client != null) close(client);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static Client openClient(URI server, String token, Duration timeout) throws Exception {
    var builder =
        PulsarClient.builder()
            .serviceUrl(server.toString())
            .operationTimeout(
                Math.toIntExact(timeout.toMillis()), java.util.concurrent.TimeUnit.MILLISECONDS);
    if (token != null) builder.authentication(AuthenticationFactory.token(token));
    return new NativeClient(builder.build());
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
      throw new IllegalArgumentException("Invalid Pulsar endpoint", failure);
    }
  }

  private static String topic(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String value = path == null ? "" : path.replaceFirst("^/+", "");
    if (value.isBlank())
      throw new IllegalArgumentException(
          "AsyncAPI Pulsar topic must be present in the endpoint path");
    return value;
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    return operation.request().has("payload")
        ? operation.request().get("payload")
        : operation.request();
  }

  private static JsonNode decode(byte[] value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.binaryNode(value);
    }
  }

  private static String token(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return null;
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("Pulsar authentication requires a bearer token");
    }
    return authorization.substring(7);
  }

  private static void close(Client client) {
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  interface ClientFactory {
    Client open(URI server, String token, Duration timeout) throws Exception;
  }

  interface Client {
    String publish(String topic, byte[] value, Duration timeout) throws Exception;

    void subscribe(
        String topic,
        String subscription,
        String consumer,
        Duration timeout,
        DeliveryHandler handler,
        CompletableFuture<Done> completion)
        throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String id, byte[] value, Runnable acknowledge);
  }

  private static final class NativeClient implements Client {
    private final PulsarClient client;
    private Consumer<byte[]> consumer;

    private NativeClient(PulsarClient client) {
      this.client = client;
    }

    @Override
    public String publish(String topic, byte[] value, Duration timeout) throws Exception {
      try (var producer = client.newProducer().topic(topic).create()) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(producer.send(value).toByteArray());
      }
    }

    @Override
    public void subscribe(
        String topic,
        String subscription,
        String consumerName,
        Duration timeout,
        DeliveryHandler handler,
        CompletableFuture<Done> completion)
        throws Exception {
      consumer =
          client
              .newConsumer()
              .topic(topic)
              .subscriptionName(subscription)
              .consumerName(consumerName)
              .subscriptionType(SubscriptionType.Shared)
              .subscribe();
      Thread.ofVirtual()
          .name("openworkflow-pulsar")
          .start(
              () -> {
                try {
                  while (!completion.isDone()) {
                    Message<byte[]> message =
                        consumer.receive(
                            Math.toIntExact(timeout.toMillis()),
                            java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (message == null) continue;
                    String id =
                        Base64.getUrlEncoder()
                            .withoutPadding()
                            .encodeToString(message.getMessageId().toByteArray());
                    handler.delivered(id, message.getData(), () -> acknowledge(consumer, message));
                  }
                } catch (Exception failure) {
                  if (!completion.isDone()) completion.completeExceptionally(failure);
                }
              });
    }

    private static void acknowledge(Consumer<byte[]> consumer, Message<byte[]> message) {
      try {
        consumer.acknowledge(message);
      } catch (Exception failure) {
        throw new IllegalStateException("Pulsar acknowledgement failed", failure);
      }
    }

    @Override
    public void close() throws Exception {
      if (consumer != null) consumer.close();
      client.close();
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
