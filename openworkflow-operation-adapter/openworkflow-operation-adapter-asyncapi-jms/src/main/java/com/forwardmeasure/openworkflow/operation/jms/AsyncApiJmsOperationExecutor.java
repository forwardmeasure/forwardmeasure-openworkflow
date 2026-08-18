package com.forwardmeasure.openworkflow.operation.jms;

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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pekko.Done;

/** Standard JMS, IBM MQ, and Solace native AsyncAPI publish/subscribe driver. */
public final class AsyncApiJmsOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final BrokerFactory brokers;

  public AsyncApiJmsOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, AsyncApiJmsOperationExecutor::openBroker);
  }

  AsyncApiJmsOperationExecutor(
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
        || !(operation.protocol().equals("jms")
            || operation.protocol().equals("ibmmq")
            || operation.protocol().equals("solace"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI JMS driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() != AuthenticationPlan.Kind.BASIC) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "JMS provider authentication requires username and password"));
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
      String username = credential == null ? null : credential.username();
      String password = credential == null ? null : credential.password();
      broker = brokers.open(operation.protocol(), operation, username, password, timeout);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
        String id =
            broker.publish(destination(operation), JSON.writeValueAsBytes(payload(operation)));
        Broker opened = broker;
        return sink.observe(
                id,
                JsonNodeFactory.instance
                    .objectNode()
                    .put("destination", destination(operation))
                    .put("messageId", id),
                false,
                true,
                clock.instant())
            .thenApply(disposition -> Done.getInstance())
            .whenComplete((done, failure) -> close(opened));
      }
      var completion = new CompletableFuture<Done>();
      var sequence = new AtomicLong();
      Broker opened = broker;
      opened.subscribe(
          destination(operation),
          (id, value, acknowledge) -> {
            if (completion.isDone()) return;
            synchronized (completion) {
              if (completion.isDone()) return;
              try {
                ObservationDisposition disposition =
                    sink.observe(
                            id == null
                                ? destination(operation) + "-" + sequence.getAndIncrement()
                                : id,
                            decode(value),
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
      completion.whenComplete((done, failure) -> close(opened));
      return completion;
    } catch (Exception failure) {
      if (broker != null) close(broker);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static Broker openBroker(
      String protocol,
      ProtocolOperationDescriptor operation,
      String username,
      String password,
      Duration timeout)
      throws Exception {
    return switch (protocol) {
      case "jms" -> new JakartaBroker(qpid(operation), username, password);
      case "solace" ->
          new JakartaBroker(solace(operation, username, password, timeout), null, null);
      case "ibmmq" -> new IbmBroker(operation, username, password);
      default -> throw new IllegalArgumentException("Unsupported JMS provider");
    };
  }

  private static jakarta.jms.ConnectionFactory qpid(ProtocolOperationDescriptor operation) {
    return new org.apache.qpid.jms.JmsConnectionFactory(origin(operation).toString());
  }

  private static jakarta.jms.ConnectionFactory solace(
      ProtocolOperationDescriptor operation, String username, String password, Duration timeout)
      throws Exception {
    var factory = com.solacesystems.jms.SolJmsUtility.createConnectionFactory();
    factory.setHost(origin(operation).toString());
    factory.setVPN(header(operation, "vpn", "default"));
    factory.setUsername(username);
    factory.setPassword(password);
    factory.setConnectTimeoutInMillis(Math.toIntExact(timeout.toMillis()));
    return factory;
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

  private static String destination(ProtocolOperationDescriptor operation) {
    String configured = header(operation, "destination", "");
    if (!configured.isBlank()) return configured;
    String path = operation.endpoint().getPath();
    String value = path == null ? "" : path.replaceFirst("^/+", "");
    if (value.isBlank())
      throw new IllegalArgumentException(
          "JMS destination must be present in the endpoint path or headers");
    return value;
  }

  private static String header(
      ProtocolOperationDescriptor operation, String name, String fallback) {
    String value = operation.request().path("headers").path(name).asText();
    return value.isBlank() ? fallback : value;
  }

  private static URI origin(ProtocolOperationDescriptor operation) {
    URI endpoint = operation.endpoint();
    String scheme =
        switch (operation.protocol()) {
          case "jms" -> endpoint.getScheme().equals("jms") ? "amqp" : endpoint.getScheme();
          case "solace" -> endpoint.getScheme().equals("solace") ? "tcp" : endpoint.getScheme();
          default -> endpoint.getScheme();
        };
    try {
      return new URI(
          scheme, endpoint.getUserInfo(), endpoint.getHost(), endpoint.getPort(), null, null, null);
    } catch (Exception failure) {
      throw new IllegalArgumentException("Invalid JMS endpoint", failure);
    }
  }

  private static void close(Broker broker) {
    try {
      broker.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  interface BrokerFactory {
    Broker open(
        String protocol,
        ProtocolOperationDescriptor operation,
        String username,
        String password,
        Duration timeout)
        throws Exception;
  }

  interface Broker {
    String publish(String destination, byte[] value) throws Exception;

    void subscribe(String destination, DeliveryHandler handler) throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String id, byte[] value, Runnable acknowledge);
  }

  private static final class JakartaBroker implements Broker {
    private final jakarta.jms.JMSContext context;

    private JakartaBroker(jakarta.jms.ConnectionFactory factory, String username, String password) {
      context =
          username == null
              ? factory.createContext(jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE)
              : factory.createContext(
                  username, password, jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @Override
    public String publish(String destination, byte[] value) throws Exception {
      jakarta.jms.BytesMessage message = context.createBytesMessage();
      message.writeBytes(value);
      context.createProducer().send(context.createQueue(destination), message);
      return messageId(message);
    }

    @Override
    public void subscribe(String destination, DeliveryHandler handler) {
      context
          .createConsumer(context.createQueue(destination))
          .setMessageListener(
              message ->
                  handler.delivered(messageId(message), body(message), () -> acknowledge(message)));
      context.start();
    }

    private static byte[] body(jakarta.jms.Message message) {
      try {
        return message.getBody(byte[].class);
      } catch (Exception failure) {
        try {
          return message.getBody(String.class).getBytes(StandardCharsets.UTF_8);
        } catch (Exception nested) {
          throw new IllegalStateException("Cannot decode JMS message", nested);
        }
      }
    }

    private static String messageId(jakarta.jms.Message message) {
      try {
        return message.getJMSMessageID();
      } catch (Exception failure) {
        throw new IllegalStateException("Cannot read JMS message ID", failure);
      }
    }

    private static void acknowledge(jakarta.jms.Message message) {
      try {
        message.acknowledge();
      } catch (Exception failure) {
        throw new IllegalStateException("JMS acknowledgement failed", failure);
      }
    }

    @Override
    public void close() {
      context.close();
    }
  }

  private static final class IbmBroker implements Broker {
    private final javax.jms.JMSContext context;

    private IbmBroker(ProtocolOperationDescriptor operation, String username, String password)
        throws Exception {
      var factory = new com.ibm.mq.jms.MQConnectionFactory();
      factory.setHostName(operation.endpoint().getHost());
      factory.setPort(operation.endpoint().getPort() < 0 ? 1414 : operation.endpoint().getPort());
      factory.setQueueManager(header(operation, "queueManager", ""));
      factory.setChannel(header(operation, "channel", "DEV.APP.SVRCONN"));
      factory.setTransportType(com.ibm.msg.client.wmq.WMQConstants.WMQ_CM_CLIENT);
      context =
          username == null
              ? factory.createContext(javax.jms.JMSContext.CLIENT_ACKNOWLEDGE)
              : factory.createContext(username, password, javax.jms.JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @Override
    public String publish(String destination, byte[] value) throws Exception {
      javax.jms.BytesMessage message = context.createBytesMessage();
      message.writeBytes(value);
      context.createProducer().send(context.createQueue(destination), message);
      return messageId(message);
    }

    @Override
    public void subscribe(String destination, DeliveryHandler handler) {
      context
          .createConsumer(context.createQueue(destination))
          .setMessageListener(
              message ->
                  handler.delivered(messageId(message), body(message), () -> acknowledge(message)));
      context.start();
    }

    private static byte[] body(javax.jms.Message message) {
      try {
        return message.getBody(byte[].class);
      } catch (Exception failure) {
        try {
          return message.getBody(String.class).getBytes(StandardCharsets.UTF_8);
        } catch (Exception nested) {
          throw new IllegalStateException("Cannot decode IBM MQ message", nested);
        }
      }
    }

    private static String messageId(javax.jms.Message message) {
      try {
        return message.getJMSMessageID();
      } catch (Exception failure) {
        throw new IllegalStateException("Cannot read IBM MQ message ID", failure);
      }
    }

    private static void acknowledge(javax.jms.Message message) {
      try {
        message.acknowledge();
      } catch (Exception failure) {
        throw new IllegalStateException("IBM MQ acknowledgement failed", failure);
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
