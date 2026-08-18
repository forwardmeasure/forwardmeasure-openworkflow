package com.forwardmeasure.openworkflow.operation.mqtt;

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
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pekko.Done;

/** MQTT 3.1.1 and MQTT 5 AsyncAPI publish/subscribe driver. */
public final class AsyncApiMqttOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final ClientFactory clients;

  public AsyncApiMqttOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, PahoClient::new);
  }

  AsyncApiMqttOperationExecutor(
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
        || !(operation.protocol().equals("mqtt") || operation.protocol().equals("mqtt5"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI MQTT driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("MQTT does not support HTTP Digest authentication"));
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
    Client client = null;
    try {
      client =
          clients.create(operation.protocol(), broker(operation), identity(executionId, operation));
      Credentials credentials = credentials(credential);
      client.connect(credentials.username(), credentials.password(), timeout);
      return operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH
          ? publish(client, operation, sink)
          : subscribe(client, operation, sink);
    } catch (Exception failure) {
      if (client != null) close(client);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> publish(
      Client client, ProtocolOperationDescriptor operation, ObservationSink sink) throws Exception {
    JsonNode request = operation.request();
    int qos = request.path("headers").path("qos").asInt(1);
    boolean retained = request.path("headers").path("retained").asBoolean(false);
    byte[] payload =
        JSON.writeValueAsBytes(request.has("payload") ? request.get("payload") : request);
    client.publish(topic(operation), payload, qos, retained, timeout);
    return sink.observe(
            operation.operationId(),
            JsonNodeFactory.instance.objectNode().put("topic", topic(operation)).put("qos", qos),
            false,
            true,
            clock.instant())
        .thenApply(disposition -> Done.getInstance())
        .whenComplete((done, failure) -> close(client));
  }

  private CompletionStage<Done> subscribe(
      Client client, ProtocolOperationDescriptor operation, ObservationSink sink) throws Exception {
    var completion = new CompletableFuture<Done>();
    var sequence = new AtomicLong();
    int qos = operation.request().path("headers").path("qos").asInt(1);
    client.subscribe(
        topic(operation),
        qos,
        (message, acknowledge) -> {
          if (completion.isDone()) return;
          synchronized (completion) {
            if (completion.isDone()) return;
            try {
              ObservationDisposition disposition =
                  sink.observe(
                          topic(operation) + "-" + sequence.getAndIncrement(),
                          decode(message),
                          false,
                          false,
                          clock.instant())
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
        });
    completion.whenComplete((done, failure) -> close(client));
    return completion;
  }

  private static Credentials credentials(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return new Credentials(null, null);
    if (credential.kind() == AuthenticationPlan.Kind.BASIC) {
      return new Credentials(
          credential.username(), credential.password().getBytes(StandardCharsets.UTF_8));
    }
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("MQTT bearer authentication requires a token");
    }
    return new Credentials("token", authorization.substring(7).getBytes(StandardCharsets.UTF_8));
  }

  private static JsonNode decode(byte[] value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.binaryNode(value);
    }
  }

  private static String broker(ProtocolOperationDescriptor operation) {
    String scheme =
        switch (operation.endpoint().getScheme()) {
          case "mqtt" -> "tcp";
          case "mqtts" -> "ssl";
          default -> operation.endpoint().getScheme();
        };
    int port = operation.endpoint().getPort();
    return scheme + "://" + operation.endpoint().getHost() + (port < 0 ? "" : ":" + port);
  }

  private static String topic(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String topic = path == null ? "" : path.replaceFirst("^/+", "");
    if (topic.isBlank())
      throw new IllegalArgumentException("AsyncAPI MQTT channel must resolve to a topic");
    return topic;
  }

  private static String identity(ExecutionId executionId, ProtocolOperationDescriptor operation) {
    return "ow-"
        + UUID.nameUUIDFromBytes(
            (executionId.entityId() + "|" + operation.operationId())
                .getBytes(StandardCharsets.UTF_8));
  }

  private static void close(Client client) {
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  private record Credentials(String username, byte[] password) {}

  @FunctionalInterface
  interface ClientFactory {
    Client create(String protocol, String broker, String clientId) throws Exception;
  }

  interface Client {
    void connect(String username, byte[] password, Duration timeout) throws Exception;

    void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception;

    void subscribe(String topic, int qos, MessageHandler handler) throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface MessageHandler {
    void message(byte[] payload, Runnable acknowledge);
  }

  private static final class PahoClient implements Client {
    private final org.eclipse.paho.client.mqttv3.MqttAsyncClient mqtt3;
    private final org.eclipse.paho.mqttv5.client.MqttAsyncClient mqtt5;

    private PahoClient(String protocol, String broker, String clientId) throws Exception {
      mqtt3 =
          protocol.equals("mqtt")
              ? new org.eclipse.paho.client.mqttv3.MqttAsyncClient(broker, clientId)
              : null;
      mqtt5 =
          protocol.equals("mqtt5")
              ? new org.eclipse.paho.mqttv5.client.MqttAsyncClient(broker, clientId)
              : null;
    }

    @Override
    public void connect(String username, byte[] password, Duration timeout) throws Exception {
      int seconds = Math.max(1, Math.toIntExact(timeout.toSeconds()));
      if (mqtt3 != null) {
        var options = new org.eclipse.paho.client.mqttv3.MqttConnectOptions();
        options.setMqttVersion(
            org.eclipse.paho.client.mqttv3.MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setConnectionTimeout(seconds);
        options.setAutomaticReconnect(true);
        if (username != null) options.setUserName(username);
        if (password != null)
          options.setPassword(new String(password, StandardCharsets.UTF_8).toCharArray());
        mqtt3.connect(options).waitForCompletion(timeout.toMillis());
      } else {
        var options = new org.eclipse.paho.mqttv5.client.MqttConnectionOptions();
        options.setConnectionTimeout(seconds);
        options.setAutomaticReconnect(true);
        if (username != null) options.setUserName(username);
        if (password != null) options.setPassword(password);
        mqtt5.connect(options).waitForCompletion(timeout.toMillis());
      }
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained, Duration timeout)
        throws Exception {
      if (mqtt3 != null) {
        mqtt3.publish(topic, payload, qos, retained).waitForCompletion(timeout.toMillis());
      } else {
        mqtt5.publish(topic, payload, qos, retained).waitForCompletion(timeout.toMillis());
      }
    }

    @Override
    public void subscribe(String topic, int qos, MessageHandler handler) throws Exception {
      if (mqtt3 != null) {
        mqtt3.setManualAcks(true);
        mqtt3.subscribe(
            topic,
            qos,
            (receivedTopic, message) ->
                handler.message(message.getPayload(), () -> acknowledge3(message)));
      } else {
        mqtt5.setManualAcks(true);
        mqtt5.subscribe(
            new org.eclipse.paho.mqttv5.common.MqttSubscription(topic, qos),
            (receivedTopic, message) ->
                handler.message(message.getPayload(), () -> acknowledge5(message)));
      }
    }

    private void acknowledge3(org.eclipse.paho.client.mqttv3.MqttMessage message) {
      try {
        mqtt3.messageArrivedComplete(message.getId(), message.getQos());
      } catch (Exception failure) {
        throw new IllegalStateException("MQTT acknowledgement failed", failure);
      }
    }

    private void acknowledge5(org.eclipse.paho.mqttv5.common.MqttMessage message) {
      try {
        mqtt5.messageArrivedComplete(message.getId(), message.getQos());
      } catch (Exception failure) {
        throw new IllegalStateException("MQTT5 acknowledgement failed", failure);
      }
    }

    @Override
    public void close() throws Exception {
      if (mqtt3 != null) {
        if (mqtt3.isConnected()) mqtt3.disconnectForcibly();
        mqtt3.close();
      } else {
        if (mqtt5.isConnected()) mqtt5.disconnectForcibly();
        mqtt5.close();
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
