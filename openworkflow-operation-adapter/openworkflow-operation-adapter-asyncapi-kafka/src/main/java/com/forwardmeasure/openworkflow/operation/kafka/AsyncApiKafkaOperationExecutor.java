package com.forwardmeasure.openworkflow.operation.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolAuthenticationResolver;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.Done;

/** Durable-observation AsyncAPI Kafka publish/subscribe transport. */
public final class AsyncApiKafkaOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration pollTimeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final ProducerFactory producers;
  private final ConsumerFactory consumers;
  private final ProtocolAuthenticationResolver authentication;
  private final HttpClient oauthClient;
  private final HttpAuthenticationSupport oauthTokens;

  public AsyncApiKafkaOperationExecutor(Duration pollTimeout, HttpEgressPolicy egress) {
    this(
        pollTimeout,
        Clock.systemUTC(),
        egress,
        properties -> new KafkaProducer<>(properties),
        properties -> new KafkaConsumer<>(properties),
        SecretProvider.rejecting());
  }

  public AsyncApiKafkaOperationExecutor(
      Duration pollTimeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(
        pollTimeout,
        Clock.systemUTC(),
        egress,
        properties -> new KafkaProducer<>(properties),
        properties -> new KafkaConsumer<>(properties),
        secrets);
  }

  AsyncApiKafkaOperationExecutor(
      Duration pollTimeout,
      Clock clock,
      HttpEgressPolicy egress,
      ProducerFactory producers,
      ConsumerFactory consumers) {
    this(pollTimeout, clock, egress, producers, consumers, SecretProvider.rejecting());
  }

  AsyncApiKafkaOperationExecutor(
      Duration pollTimeout,
      Clock clock,
      HttpEgressPolicy egress,
      ProducerFactory producers,
      ConsumerFactory consumers,
      SecretProvider secrets) {
    this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.producers = Objects.requireNonNull(producers, "producers");
    this.consumers = Objects.requireNonNull(consumers, "consumers");
    this.authentication =
        new ProtocolAuthenticationResolver(Objects.requireNonNull(secrets, "secrets"));
    this.oauthClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    this.oauthTokens =
        new HttpAuthenticationSupport(oauthClient, JSON, Duration.ofSeconds(10), egress, secrets);
    if (pollTimeout.isZero() || pollTimeout.isNegative()) {
      throw new IllegalArgumentException("Kafka poll timeout must be positive");
    }
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.ASYNC_API
        || !(operation.protocol().equals("kafka") || operation.protocol().equals("kafka-secure"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI Kafka driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() != AuthenticationPlan.Kind.BASIC
        && operation.authentication().kind() != AuthenticationPlan.Kind.BEARER
        && operation.authentication().kind() != AuthenticationPlan.Kind.OAUTH2
        && operation.authentication().kind() != AuthenticationPlan.Kind.OIDC) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Kafka authentication supports Basic SASL/PLAIN or OAuth2/OIDC SASL/OAUTHBEARER"));
    }
    try {
      egress.authorize(executionId.tenantId(), operation.endpoint());
      return operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH
          ? publish(executionId, operation, sink)
          : subscribe(executionId, operation, sink);
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> publish(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink)
      throws Exception {
    Properties configured = producerProperties(executionId, operation);
    Producer<String, byte[]> producer;
    try {
      producer = producers.create(configured);
    } finally {
      unregisterOAuth(configured);
    }
    JsonNode message = operation.request();
    JsonNode payload = message.has("payload") ? message.get("payload") : message;
    var record =
        new ProducerRecord<String, byte[]>(topic(operation), JSON.writeValueAsBytes(payload));
    if (message.path("headers").isObject()) {
      message
          .path("headers")
          .properties()
          .forEach(
              entry ->
                  record
                      .headers()
                      .add(
                          new RecordHeader(
                              entry.getKey(),
                              (entry.getValue().isValueNode()
                                      ? entry.getValue().asText()
                                      : entry.getValue().toString())
                                  .getBytes(StandardCharsets.UTF_8))));
    }
    var result = new CompletableFuture<Done>();
    result.whenComplete(
        (done, failure) -> {
          if (result.isCancelled()) producer.close(Duration.ZERO);
        });
    producer.send(
        record,
        (metadata, failure) -> {
          if (result.isCancelled()) return;
          if (failure != null) {
            producer.close(Duration.ZERO);
            result.completeExceptionally(failure);
            return;
          }
          var receipt =
              JsonNodeFactory.instance
                  .objectNode()
                  .put("topic", metadata.topic())
                  .put("partition", metadata.partition())
                  .put("offset", metadata.offset());
          sink.observe(
                  observationId(metadata.topic(), metadata.partition(), metadata.offset()),
                  receipt,
                  false,
                  true,
                  clock.instant())
              .whenComplete(
                  (disposition, observationFailure) -> {
                    producer.close(Duration.ZERO);
                    if (observationFailure == null) {
                      result.complete(Done.getInstance());
                    } else {
                      result.completeExceptionally(observationFailure);
                    }
                  });
        });
    return result;
  }

  private CompletionStage<Done> subscribe(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    var result = new CompletableFuture<Done>();
    var active = new AtomicReference<Consumer<String, byte[]>>();
    result.whenComplete(
        (done, failure) -> {
          if (result.isCancelled()) {
            Consumer<String, byte[]> consumer = active.get();
            if (consumer != null) consumer.wakeup();
          }
        });
    Thread.ofVirtual()
        .name("openworkflow-kafka-" + operation.operationId())
        .start(
            () -> {
              Properties configured = consumerProperties(executionId, operation);
              Consumer<String, byte[]> created;
              try {
                created = consumers.create(configured);
              } finally {
                unregisterOAuth(configured);
              }
              try (Consumer<String, byte[]> consumer = created) {
                active.set(consumer);
                if (result.isCancelled()) return;
                consumer.subscribe(java.util.List.of(topic(operation)));
                while (!result.isDone()) {
                  ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
                  for (ConsumerRecord<String, byte[]> record : records) {
                    JsonNode value = JSON.readTree(record.value());
                    ObservationDisposition disposition =
                        sink.observe(
                                observationId(record.topic(), record.partition(), record.offset()),
                                value,
                                false,
                                false,
                                clock.instant())
                            .toCompletableFuture()
                            .join();
                    consumer.commitSync(
                        Map.of(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)));
                    if (disposition == ObservationDisposition.STOP) {
                      result.complete(Done.getInstance());
                      return;
                    }
                  }
                }
              } catch (WakeupException cancelled) {
                if (!result.isCancelled()) result.completeExceptionally(cancelled);
              } catch (Exception failure) {
                result.completeExceptionally(failure);
              } finally {
                active.set(null);
              }
            });
    return result;
  }

  private Properties producerProperties(
      ExecutionId executionId, ProtocolOperationDescriptor operation) {
    Properties properties = commonProperties(executionId, operation);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, identity(executionId, operation));
    return properties;
  }

  private Properties consumerProperties(
      ExecutionId executionId, ProtocolOperationDescriptor operation) {
    Properties properties = commonProperties(executionId, operation);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, identity(executionId, operation));
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, identity(executionId, operation));
    return properties;
  }

  private Properties commonProperties(
      ExecutionId executionId, ProtocolOperationDescriptor operation) {
    Properties properties = new Properties();
    int port = operation.endpoint().getPort() < 0 ? 9092 : operation.endpoint().getPort();
    properties.put("bootstrap.servers", operation.endpoint().getHost() + ":" + port);
    if (operation.authentication() != null) {
      properties.put(
          "security.protocol",
          operation.protocol().equals("kafka-secure") ? "SASL_SSL" : "SASL_PLAINTEXT");
      if (operation.authentication().kind() == AuthenticationPlan.Kind.BASIC) {
        var credential =
            authentication.resolve(
                executionId, operation.authentication(), operation.authenticationContext());
        String username = jaas(credential.username());
        String password = jaas(credential.password());
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username=\""
                + username
                + "\" password=\""
                + password
                + "\";");
      } else {
        oauthProperties(properties, executionId, operation);
      }
    } else if (operation.protocol().equals("kafka-secure")) {
      properties.put("security.protocol", "SSL");
    }
    return properties;
  }

  private void oauthProperties(
      Properties properties, ExecutionId executionId, ProtocolOperationDescriptor operation) {
    AuthenticationPlan policy = operation.authentication();
    if (policy.secretBacked()) {
      callbackProperties(properties, executionId, operation, JsonNodeFactory.instance.objectNode());
      return;
    }
    JsonNode configuration =
        authentication.materializeConfiguration(
            executionId, policy, operation.authenticationContext());
    if (!configuration.path("grant").asText().equals("client_credentials")) {
      callbackProperties(properties, executionId, operation, configuration);
      return;
    }
    JsonNode client = configuration.path("client");
    String clientId = requiredText(client, "id");
    String clientSecret = requiredText(client, "secret");
    String method = client.path("authentication").asText("client_secret_post");
    if (!method.equals("client_secret_post") && !method.equals("client_secret_basic")) {
      throw new IllegalArgumentException(
          "Kafka SASL/OAUTHBEARER does not support client authentication " + method);
    }
    URI tokenEndpoint = tokenEndpoint(executionId, policy.kind(), configuration);
    properties.put("sasl.mechanism", "OAUTHBEARER");
    properties.put(
        "sasl.login.callback.handler.class",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
    properties.put("sasl.oauthbearer.token.endpoint.url", tokenEndpoint.toString());
    if (configuration.path("scopes").isArray()) {
      var scopes = new java.util.ArrayList<String>();
      configuration.path("scopes").forEach(value -> scopes.add(value.asText()));
      if (!scopes.isEmpty()) properties.put("sasl.oauthbearer.scope", String.join(" ", scopes));
    }
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            + "required clientId=\""
            + jaas(clientId)
            + "\" clientSecret=\""
            + jaas(clientSecret)
            + "\";");
  }

  private void callbackProperties(
      Properties properties,
      ExecutionId executionId,
      ProtocolOperationDescriptor operation,
      JsonNode configuration) {
    var scopes = new java.util.LinkedHashSet<String>();
    configuration.path("scopes").forEach(value -> scopes.add(value.asText()));
    String handle =
        WorkflowOAuthLoginCallbackHandler.register(
            new WorkflowOAuthLoginCallbackHandler.Context(
                () ->
                    oauthTokens
                        .resolve(
                            executionId,
                            operation.authentication(),
                            operation.authenticationContext(),
                            operation.operationId())
                        .toCompletableFuture()
                        .join(),
                scopes,
                identity(executionId, operation),
                clock,
                Duration.ofMinutes(5)));
    properties.put(WorkflowOAuthLoginCallbackHandler.CONTEXT_CONFIG, handle);
    properties.put("sasl.mechanism", "OAUTHBEARER");
    properties.put(
        "sasl.login.callback.handler.class", WorkflowOAuthLoginCallbackHandler.class.getName());
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
  }

  private static void unregisterOAuth(Properties properties) {
    WorkflowOAuthLoginCallbackHandler.unregister(
        properties.getProperty(WorkflowOAuthLoginCallbackHandler.CONTEXT_CONFIG));
  }

  private URI tokenEndpoint(
      ExecutionId executionId, AuthenticationPlan.Kind kind, JsonNode configuration) {
    URI authority = URI.create(uriText(configuration.required("authority")));
    if (kind == AuthenticationPlan.Kind.OAUTH2) {
      URI endpoint =
          authority.resolve(configuration.path("endpoints").path("token").asText("/oauth2/token"));
      egress.authorize(executionId.tenantId(), endpoint);
      return endpoint;
    }
    URI discovery =
        URI.create(
            authority.toString().replaceAll("/+$", "") + "/.well-known/openid-configuration");
    egress.authorize(executionId.tenantId(), discovery);
    try {
      HttpResponse<byte[]> response =
          oauthClient.send(
              HttpRequest.newBuilder(discovery).timeout(Duration.ofSeconds(10)).GET().build(),
              HttpResponse.BodyHandlers.ofByteArray());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalArgumentException(
            "OIDC discovery returned status " + response.statusCode());
      }
      URI endpoint = URI.create(JSON.readTree(response.body()).required("token_endpoint").asText());
      egress.authorize(executionId.tenantId(), endpoint);
      return endpoint;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("OIDC discovery was interrupted", interrupted);
    } catch (java.io.IOException failure) {
      throw new IllegalArgumentException("OIDC discovery document is invalid", failure);
    }
  }

  private static String uriText(JsonNode value) {
    if (value.isTextual()) return value.asText();
    if (value.isObject() && value.hasNonNull("uri")) {
      return value.required("uri").asText();
    }
    throw new IllegalArgumentException("Authentication authority must be a URI");
  }

  private static String requiredText(JsonNode value, String name) {
    JsonNode candidate = value.path(name);
    if (!candidate.isTextual() || candidate.asText().isBlank()) {
      throw new IllegalArgumentException("Kafka OAuth field '" + name + "' must be non-blank text");
    }
    return candidate.asText();
  }

  private static String jaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String topic(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String topic = path == null ? "" : path.replaceFirst("^/+", "");
    if (topic.isBlank() || topic.contains("/")) {
      throw new IllegalArgumentException("AsyncAPI Kafka channel must resolve to one topic");
    }
    return topic;
  }

  private static String identity(ExecutionId executionId, ProtocolOperationDescriptor operation) {
    return "ow-"
        + UUID.nameUUIDFromBytes(
            (executionId.entityId() + "|" + operation.operationId())
                .getBytes(StandardCharsets.UTF_8));
  }

  private static String observationId(String topic, int partition, long offset) {
    return topic + "-" + partition + "-" + offset;
  }

  @FunctionalInterface
  interface ProducerFactory {
    Producer<String, byte[]> create(Properties properties);
  }

  @FunctionalInterface
  interface ConsumerFactory {
    Consumer<String, byte[]> create(Properties properties);
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
