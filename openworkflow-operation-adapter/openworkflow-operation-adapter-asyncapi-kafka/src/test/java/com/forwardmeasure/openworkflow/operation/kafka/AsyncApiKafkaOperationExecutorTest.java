package com.forwardmeasure.openworkflow.operation.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationObservation;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

final class AsyncApiKafkaOperationExecutorTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void publishesPayloadHeadersAndAStableDurableReceipt() {
    var properties = new AtomicReference<Properties>();
    var producer =
        new MockProducer<String, byte[]>(
            true, null, new StringSerializer(), new ByteArraySerializer());
    var executor =
        new AsyncApiKafkaOperationExecutor(
            Duration.ofMillis(10),
            Clock.fixed(AT, ZoneOffset.UTC),
            HttpEgressPolicy.allowAllForTesting(),
            configured -> {
              properties.set(configured);
              return producer;
            },
            ignored -> {
              throw new AssertionError("consumer not expected");
            },
            (tenant, name) -> "secret".toCharArray());
    var observations = new ArrayList<ProtocolOperationObservation>();

    executor
        .execute(
            execution(),
            operation(
                false,
                AuthenticationPlan.expressions(
                    AuthenticationPlan.Kind.BASIC,
                    "kafka-basic",
                    JsonNodeFactory.instance
                        .objectNode()
                        .put("username", "${ $input.username }")
                        .put("password", "${ $secrets[\"kafka-password\"] }"),
                    List.of("kafka-password"))),
            (id, value, failed, terminal, at) -> {
              observations.add(new ProtocolOperationObservation(id, value, failed, terminal, at));
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                      .CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals("broker.example.test:9092", properties.get().getProperty("bootstrap.servers"));
    assertEquals("SASL_PLAINTEXT", properties.get().getProperty("security.protocol"));
    assertTrue(
        properties
            .get()
            .getProperty("sasl.jaas.config")
            .contains("username=\"worker\" password=\"secret\""));
    assertEquals(1, producer.history().size());
    assertEquals("evidence", producer.history().getFirst().topic());
    assertEquals(
        "{\"value\":42}",
        new String(producer.history().getFirst().value(), java.nio.charset.StandardCharsets.UTF_8));
    assertEquals(
        "trace-1",
        new String(
            producer.history().getFirst().headers().lastHeader("X-Trace").value(),
            java.nio.charset.StandardCharsets.UTF_8));
    assertEquals("evidence-0-0", observations.getFirst().observationId());
    assertTrue(observations.getFirst().terminal());
  }

  @Test
  void configuresRefreshableTenantOAuthForKafkaWithoutChangingDurableIntent() {
    var properties = new AtomicReference<Properties>();
    var producer =
        new MockProducer<String, byte[]>(
            true, null, new StringSerializer(), new ByteArraySerializer());
    var executor =
        new AsyncApiKafkaOperationExecutor(
            Duration.ofMillis(10),
            Clock.fixed(AT, ZoneOffset.UTC),
            HttpEgressPolicy.allowAllForTesting(),
            configured -> {
              properties.set(configured);
              return producer;
            },
            ignored -> {
              throw new AssertionError("consumer not expected");
            },
            (tenant, name) -> "tenant-oauth-secret".toCharArray());
    var configuration =
        JsonNodeFactory.instance
            .objectNode()
            .put("authority", "https://identity.example.test")
            .put("grant", "client_credentials");
    configuration.putObject("endpoints").put("token", "/oauth/token");
    configuration.putArray("scopes").add("events.write").add("tenant.read");
    configuration
        .putObject("client")
        .put("id", "workflow-kafka")
        .put("secret", "${ $secrets[\"oauth-secret\"] }")
        .put("authentication", "client_secret_post");
    AuthenticationPlan authentication =
        AuthenticationPlan.expressions(
            AuthenticationPlan.Kind.OAUTH2, "kafka-oauth", configuration, List.of("oauth-secret"));
    ProtocolOperationDescriptor durable = operation(false, authentication);

    executor
        .execute(
            execution(),
            durable,
            (id, value, failed, terminal, at) ->
                CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                        .CONTINUE))
        .toCompletableFuture()
        .join();

    assertEquals("OAUTHBEARER", properties.get().getProperty("sasl.mechanism"));
    assertEquals(
        "https://identity.example.test/oauth/token",
        properties.get().getProperty("sasl.oauthbearer.token.endpoint.url"));
    assertEquals(
        "events.write tenant.read", properties.get().getProperty("sasl.oauthbearer.scope"));
    assertTrue(
        properties
            .get()
            .getProperty("sasl.jaas.config")
            .contains("clientId=\"workflow-kafka\" clientSecret=\"tenant-oauth-secret\""));
    assertTrue(!durable.toString().contains("tenant-oauth-secret"));
  }

  @Test
  void discoversOidcTokenEndpointBeforeConstructingKafkaClient() throws Exception {
    HttpServer identity = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var discovered = new java.util.concurrent.atomic.AtomicInteger();
    identity.createContext(
        "/.well-known/openid-configuration",
        exchange -> {
          discovered.incrementAndGet();
          String tokenEndpoint = "http://127.0.0.1:" + identity.getAddress().getPort() + "/token";
          byte[] body =
              ("{\"token_endpoint\":\"" + tokenEndpoint + "\"}")
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    identity.start();
    try {
      var properties = new AtomicReference<Properties>();
      var producer =
          new MockProducer<String, byte[]>(
              true, null, new StringSerializer(), new ByteArraySerializer());
      var executor =
          new AsyncApiKafkaOperationExecutor(
              Duration.ofMillis(10),
              Clock.fixed(AT, ZoneOffset.UTC),
              HttpEgressPolicy.allowAllForTesting(),
              configured -> {
                properties.set(configured);
                return producer;
              },
              ignored -> {
                throw new AssertionError("consumer not expected");
              },
              (tenant, name) -> "oidc-secret".toCharArray());
      String authority = "http://127.0.0.1:" + identity.getAddress().getPort();
      var configuration =
          JsonNodeFactory.instance
              .objectNode()
              .put("authority", authority)
              .put("grant", "client_credentials");
      configuration
          .putObject("client")
          .put("id", "oidc-kafka")
          .put("secret", "${ $secrets[\"oidc-secret\"] }")
          .put("authentication", "client_secret_post");

      executor
          .execute(
              execution(),
              operation(
                  false,
                  AuthenticationPlan.expressions(
                      AuthenticationPlan.Kind.OIDC,
                      "kafka-oidc",
                      configuration,
                      List.of("oidc-secret"))),
              (id, value, failed, terminal, at) ->
                  CompletableFuture.completedFuture(
                      com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                          .CONTINUE))
          .toCompletableFuture()
          .join();

      assertEquals(1, discovered.get());
      assertEquals(
          authority + "/token",
          properties.get().getProperty("sasl.oauthbearer.token.endpoint.url"));
    } finally {
      identity.stop(0);
    }
  }

  @Test
  void refreshCallbackAcquiresNonClientCredentialsGrantAtKafkaLoginEdge() throws Exception {
    HttpServer identity = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    var tokenRequest = new AtomicReference<String>();
    identity.createContext(
        "/token",
        exchange -> {
          tokenRequest.set(
              new String(
                  exchange.getRequestBody().readAllBytes(),
                  java.nio.charset.StandardCharsets.UTF_8));
          byte[] body =
              "{\"access_token\":\"password-grant-token\",\"token_type\":\"Bearer\"}"
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    identity.start();
    try {
      var acquired = new AtomicReference<String>();
      var producer =
          new MockProducer<String, byte[]>(
              true, null, new StringSerializer(), new ByteArraySerializer());
      var executor =
          new AsyncApiKafkaOperationExecutor(
              Duration.ofMillis(10),
              Clock.fixed(AT, ZoneOffset.UTC),
              HttpEgressPolicy.allowAllForTesting(),
              configured -> {
                var values = new java.util.HashMap<String, Object>();
                configured.forEach((name, value) -> values.put(name.toString(), value));
                var handler = new WorkflowOAuthLoginCallbackHandler();
                handler.configure(values, "OAUTHBEARER", List.of());
                var callback = new OAuthBearerTokenCallback();
                try {
                  handler.handle(new javax.security.auth.callback.Callback[] {callback});
                } catch (Exception failure) {
                  throw new IllegalStateException(failure);
                } finally {
                  handler.close();
                }
                acquired.set(callback.token().value());
                return producer;
              },
              ignored -> {
                throw new AssertionError("consumer not expected");
              },
              (tenant, name) ->
                  switch (name) {
                    case "oauth-user" -> "worker".toCharArray();
                    case "oauth-password" -> "tenant-password".toCharArray();
                    default -> throw new AssertionError(name);
                  });
      String authority = "http://127.0.0.1:" + identity.getAddress().getPort();
      var configuration =
          JsonNodeFactory.instance
              .objectNode()
              .put("authority", authority)
              .put("grant", "password")
              .put("username", "${ $secrets[\"oauth-user\"] }")
              .put("password", "${ $secrets[\"oauth-password\"] }");
      configuration.putObject("endpoints").put("token", "/token");
      configuration.putObject("client").put("authentication", "none");

      executor
          .execute(
              execution(),
              operation(
                  false,
                  AuthenticationPlan.expressions(
                      AuthenticationPlan.Kind.OAUTH2,
                      "kafka-password",
                      configuration,
                      List.of("oauth-user", "oauth-password"))),
              (id, value, failed, terminal, at) ->
                  CompletableFuture.completedFuture(
                      com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                          .CONTINUE))
          .toCompletableFuture()
          .join();

      assertEquals("password-grant-token", acquired.get());
      assertTrue(tokenRequest.get().contains("grant_type=password"));
      assertTrue(tokenRequest.get().contains("username=worker"));
      assertTrue(tokenRequest.get().contains("password=tenant-password"));
    } finally {
      identity.stop(0);
    }
  }

  @Test
  void commitsOnlyAfterEachDurableObservationAndStopsOnWorkflowDisposition() {
    var committed = new AtomicReference<Map<TopicPartition, OffsetAndMetadata>>();
    var consumer =
        new MockConsumer<String, byte[]>("earliest") {
          @Override
          public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            committed.set(Map.copyOf(offsets));
            super.commitSync(offsets);
          }
        };
    var partition = new TopicPartition("evidence", 0);
    consumer.schedulePollTask(
        () -> {
          consumer.rebalance(List.of(partition));
          consumer.updateBeginningOffsets(Map.of(partition, 0L));
          consumer.addRecord(
              new ConsumerRecord<>("evidence", 0, 0, "one", "{\"value\":1}".getBytes()));
          consumer.addRecord(
              new ConsumerRecord<>("evidence", 0, 1, "two", "{\"value\":2}".getBytes()));
        });
    var executor =
        new AsyncApiKafkaOperationExecutor(
            Duration.ofMillis(10),
            Clock.fixed(AT, ZoneOffset.UTC),
            HttpEgressPolicy.allowAllForTesting(),
            ignored -> {
              throw new AssertionError("producer not expected");
            },
            ignored -> consumer);
    var ids = new ArrayList<String>();

    executor
        .execute(
            execution(),
            operation(true),
            (id, value, failed, terminal, at) -> {
              ids.add(id);
              return CompletableFuture.completedFuture(
                  ids.size() == 2
                      ? com.forwardmeasure.openworkflow.actor.ProtocolTransport
                          .ObservationDisposition.STOP
                      : com.forwardmeasure.openworkflow.actor.ProtocolTransport
                          .ObservationDisposition.CONTINUE);
            })
        .toCompletableFuture()
        .join();

    assertEquals(List.of("evidence-0-0", "evidence-0-1"), ids);
    assertEquals(2L, committed.get().get(partition).offset());
  }

  @Test
  void cancellingSubscriptionWakesAndClosesConsumer() throws Exception {
    var consumer = new MockConsumer<String, byte[]>("earliest");
    var created = new CompletableFuture<Void>();
    var executor =
        new AsyncApiKafkaOperationExecutor(
            Duration.ofMillis(50),
            Clock.fixed(AT, ZoneOffset.UTC),
            HttpEgressPolicy.allowAllForTesting(),
            ignored -> {
              throw new AssertionError("producer not expected");
            },
            ignored -> {
              created.complete(null);
              return consumer;
            });
    CompletableFuture<org.apache.pekko.Done> transport =
        executor
            .execute(
                execution(),
                operation(true),
                (id, value, failed, terminal, at) ->
                    CompletableFuture.completedFuture(
                        com.forwardmeasure.openworkflow.actor.ProtocolTransport
                            .ObservationDisposition.CONTINUE))
            .toCompletableFuture();

    created.get(3, TimeUnit.SECONDS);
    assertTrue(transport.cancel(true));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!consumer.closed() && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(consumer.closed());
  }

  private static ExecutionId execution() {
    return new ExecutionId(
        new TenantId("did:web:forwardmeasure.com:tenant:kafka"),
        UUID.fromString("11111111-2222-3333-4444-555555555555"));
  }

  private static ProtocolOperationDescriptor operation(boolean subscribe) {
    return operation(subscribe, null);
  }

  private static ProtocolOperationDescriptor operation(
      boolean subscribe, AuthenticationPlan authentication) {
    var message = JsonNodeFactory.instance.objectNode();
    message.putObject("headers").put("X-Trace", "trace-1");
    message.putObject("payload").put("value", 42);
    var subscription =
        subscribe
            ? new AsyncApiSubscriptionPlan(
                null,
                new AsyncApiSubscriptionPlan.Consumption(
                    AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT, 2, null, null),
                null,
                null,
                null)
            : null;
    AuthenticationExpressionContext authenticationContext =
        authentication != null && !authentication.secretBacked()
            ? new AuthenticationExpressionContext(
                null,
                JsonNodeFactory.instance.objectNode().put("username", "worker"),
                null,
                null,
                null,
                null,
                null,
                Map.of())
            : null;
    return new ProtocolOperationDescriptor(
        "kafka-operation",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        subscribe
            ? ProtocolOperationDescriptor.Mode.SUBSCRIBE
            : ProtocolOperationDescriptor.Mode.PUBLISH,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "a".repeat(64)),
        "kafka",
        URI.create("kafka://broker.example.test:9092/evidence"),
        subscribe ? "receiveEvidence" : "publishEvidence",
        message,
        subscription,
        authentication,
        authenticationContext);
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
