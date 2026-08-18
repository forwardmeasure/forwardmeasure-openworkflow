package com.forwardmeasure.openworkflow.operation.amqp;

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
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pekko.Done;

/** Native AMQP 0-9-1 and AMQP 1.0 AsyncAPI publish/subscribe driver. */
public final class AsyncApiAmqpOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final BrokerFactory brokers;

  public AsyncApiAmqpOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, AsyncApiAmqpOperationExecutor::openBroker);
  }

  AsyncApiAmqpOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      BrokerFactory brokers) {
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
    this.brokers = Objects.requireNonNull(brokers, "brokers");
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
        || !(operation.protocol().equals("amqp") || operation.protocol().equals("amqp1"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI AMQP driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AMQP does not support HTTP Digest authentication"));
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
        .thenCompose(credential -> run(operation, sink, credential));
  }

  private CompletionStage<Done> run(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    Broker broker = null;
    try {
      Credentials resolved = credentials(credential);
      broker =
          brokers.open(
              operation.protocol(),
              server(operation),
              resolved.username(),
              resolved.password(),
              timeout);
      return operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH
          ? publish(broker, operation, sink)
          : subscribe(broker, operation, sink);
    } catch (Exception failure) {
      if (broker != null) close(broker);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> publish(
      Broker broker, ProtocolOperationDescriptor operation, ObservationSink sink) throws Exception {
    byte[] payload = JSON.writeValueAsBytes(payload(operation));
    broker.publish(channel(operation), payload, operation.request());
    return sink.observe(
            operation.operationId(),
            JsonNodeFactory.instance.objectNode().put("channel", channel(operation)),
            false,
            true,
            clock.instant())
        .thenApply(disposition -> Done.getInstance())
        .whenComplete((done, failure) -> close(broker));
  }

  private CompletionStage<Done> subscribe(
      Broker broker, ProtocolOperationDescriptor operation, ObservationSink sink) throws Exception {
    var completion = new CompletableFuture<Done>();
    var sequence = new AtomicLong();
    broker.subscribe(
        channel(operation),
        operation.request(),
        (identifier, body, acknowledge) -> {
          if (completion.isDone()) return;
          synchronized (completion) {
            if (completion.isDone()) return;
            try {
              ObservationDisposition disposition =
                  sink.observe(
                          identifier == null
                              ? channel(operation) + "-" + sequence.getAndIncrement()
                              : identifier,
                          decode(body),
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
    completion.whenComplete((done, failure) -> close(broker));
    return completion;
  }

  private static Broker openBroker(
      String protocol, URI server, String username, String password, Duration timeout)
      throws Exception {
    return protocol.equals("amqp")
        ? new RabbitBroker(server, username, password, timeout)
        : new Amqp1Broker(server, username, password);
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    JsonNode request = operation.request();
    return request.has("payload") ? request.get("payload") : request;
  }

  private static JsonNode decode(byte[] value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.binaryNode(value);
    }
  }

  private static URI server(ProtocolOperationDescriptor operation) {
    URI endpoint = operation.endpoint();
    String scheme = endpoint.getScheme().equals("amqp1") ? "amqp" : endpoint.getScheme();
    try {
      return new URI(
          scheme, endpoint.getUserInfo(), endpoint.getHost(), endpoint.getPort(), null, null, null);
    } catch (Exception failure) {
      throw new IllegalArgumentException("Invalid AMQP endpoint", failure);
    }
  }

  private static String channel(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String channel = path == null ? "" : path.replaceFirst("^/+", "");
    if (channel.isBlank()) {
      throw new IllegalArgumentException(
          "AsyncAPI AMQP channel must be present in the endpoint path");
    }
    return channel;
  }

  private static Credentials credentials(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return new Credentials(null, null);
    if (credential.kind() == AuthenticationPlan.Kind.BASIC) {
      return new Credentials(credential.username(), credential.password());
    }
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("AMQP bearer authentication requires a token");
    }
    return new Credentials("token", authorization.substring(7));
  }

  private static void close(Broker broker) {
    try {
      broker.close();
    } catch (Exception ignored) {
    }
  }

  private record Credentials(String username, String password) {}

  @FunctionalInterface
  interface BrokerFactory {
    Broker open(String protocol, URI server, String username, String password, Duration timeout)
        throws Exception;
  }

  interface Broker {
    void publish(String channel, byte[] payload, JsonNode request) throws Exception;

    void subscribe(String channel, JsonNode request, DeliveryHandler handler) throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String identifier, byte[] body, Runnable acknowledge);
  }

  private static final class RabbitBroker implements Broker {
    private final com.rabbitmq.client.Connection connection;
    private final com.rabbitmq.client.Channel channel;

    private RabbitBroker(URI server, String username, String password, Duration timeout)
        throws Exception {
      var factory = new com.rabbitmq.client.ConnectionFactory();
      factory.setUri(server);
      factory.setConnectionTimeout(Math.toIntExact(timeout.toMillis()));
      if (username != null) factory.setUsername(username);
      if (password != null) factory.setPassword(password);
      connection = factory.newConnection("openworkflow-asyncapi-amqp");
      channel = connection.createChannel();
    }

    @Override
    public void publish(String destination, byte[] payload, JsonNode request) throws Exception {
      JsonNode headers = request.path("headers");
      String exchange = headers.path("exchange").asText("");
      String routingKey = headers.path("routingKey").asText(destination);
      channel.confirmSelect();
      channel.basicPublish(exchange, routingKey, null, payload);
      channel.waitForConfirmsOrDie();
    }

    @Override
    public void subscribe(String destination, JsonNode request, DeliveryHandler handler)
        throws Exception {
      boolean durable = request.path("headers").path("durable").asBoolean(true);
      channel.queueDeclare(destination, durable, false, false, null);
      channel.basicQos(1);
      channel.basicConsume(
          destination,
          false,
          (consumerTag, delivery) ->
              handler.delivered(
                  destination + "-" + delivery.getEnvelope().getDeliveryTag(),
                  delivery.getBody(),
                  () -> acknowledge(delivery.getEnvelope().getDeliveryTag())),
          consumerTag -> {});
    }

    private void acknowledge(long deliveryTag) {
      try {
        channel.basicAck(deliveryTag, false);
      } catch (Exception failure) {
        throw new IllegalStateException("AMQP acknowledgement failed", failure);
      }
    }

    @Override
    public void close() throws Exception {
      if (channel.isOpen()) channel.close();
      if (connection.isOpen()) connection.close();
    }
  }

  private static final class Amqp1Broker implements Broker {
    private final JMSContext context;

    private Amqp1Broker(URI server, String username, String password) {
      var factory = new org.apache.qpid.jms.JmsConnectionFactory(server.toString());
      context =
          username == null
              ? factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE)
              : factory.createContext(username, password, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @Override
    public void publish(String destination, byte[] payload, JsonNode request) {
      context.createProducer().send(context.createQueue(destination), payload);
    }

    @Override
    public void subscribe(String destination, JsonNode request, DeliveryHandler handler) {
      context
          .createConsumer(context.createQueue(destination))
          .setMessageListener(
              message ->
                  handler.delivered(
                      identifier(destination, message), body(message), () -> acknowledge(message)));
      context.start();
    }

    private static byte[] body(Message message) {
      try {
        return message.getBody(byte[].class);
      } catch (Exception failure) {
        try {
          return message.getBody(String.class).getBytes(StandardCharsets.UTF_8);
        } catch (Exception nested) {
          throw new IllegalStateException("Cannot decode AMQP 1.0 message", nested);
        }
      }
    }

    private static String identifier(String destination, Message message) {
      try {
        String id = message.getJMSMessageID();
        return id == null ? destination : id;
      } catch (Exception failure) {
        throw new IllegalStateException("Cannot read AMQP 1.0 message id", failure);
      }
    }

    private static void acknowledge(Message message) {
      try {
        message.acknowledge();
      } catch (Exception failure) {
        throw new IllegalStateException("AMQP 1.0 acknowledgement failed", failure);
      }
    }

    @Override
    public void close() {
      context.close();
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
