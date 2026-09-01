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
package com.forwardmeasure.openworkflow.eventing.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import com.forwardmeasure.openworkflow.eventing.CloudEventRouteResult;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the real Kafka transport round trip end to end: {@link KafkaCloudEventPublisher} produces
 * a durably-serialized CloudEvent, {@link KafkaCloudEventConsumer} independently consumes it from
 * the same topic, decodes it back with the official CloudEvents SDK format, resolves the tenant
 * from the durable {@code tenant} extension, and calls {@link CloudEventIngress#route} with it -
 * the exact wiring {@code PekkoEngine*Binding} constructs in production. A hand-written {@link
 * CloudEventIngress} test double (the same pattern {@code CloudEventIngressResourceTest} already
 * uses for the HTTP transport's equivalent proof) captures the routed call directly, which is a
 * more direct proof of this wiring than standing up a full {@code CloudEventIngressGateway} backed
 * by real Pekko sharding and a fake subscription repository would be.
 */
@Testcontainers(disabledWithoutDocker = true)
final class KafkaCloudEventTransportIntegrationTest {
  @Container
  static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  private final ObjectMapper json = new ObjectMapper();

  @Test
  void publishedEventIsConsumedAndRoutedWithItsTenant() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String topic = "test.openworkflow.pekko-cloud-events." + suffix;
    createTopic(topic);

    TenantId expectedTenant = new TenantId("did:web:forwardmeasure.com:tenant:kafka-transport");
    WorkflowCloudEvent published = event(expectedTenant);

    var routedTenant = new AtomicReference<TenantId>();
    var routedEvent = new AtomicReference<WorkflowCloudEvent>();
    var routedAt = new AtomicReference<Instant>();
    var latch = new CountDownLatch(1);
    CloudEventIngress ingress =
        new CloudEventIngress() {
          @Override
          public CompletionStage<WorkflowReply> deliver(
              ExecutionId executionId, WorkflowCloudEvent event, Instant receivedAt) {
            throw new UnsupportedOperationException("Not exercised by this transport test");
          }

          @Override
          public CompletionStage<ScheduleReply> deliver(
              ScheduleId scheduleId, WorkflowCloudEvent event, Instant receivedAt) {
            throw new UnsupportedOperationException("Not exercised by this transport test");
          }

          @Override
          public CompletionStage<CloudEventRouteResult> route(
              TenantId tenantId, WorkflowCloudEvent event, Instant receivedAt) {
            routedTenant.set(tenantId);
            routedEvent.set(event);
            routedAt.set(receivedAt);
            latch.countDown();
            return CompletableFuture.completedFuture(new CloudEventRouteResult(1, 1, List.of()));
          }
        };

    try (KafkaCloudEventPublisher publisher =
            new KafkaCloudEventPublisher(baseProperties(), topic, json);
        KafkaCloudEventConsumer consumer =
            new KafkaCloudEventConsumer(
                baseProperties(),
                topic,
                "kafka-transport-test-" + suffix,
                "instance-1",
                ingress,
                json,
                Duration.ofSeconds(10))) {
      consumer.start();

      publisher.publish("operation-1", published).toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertTrue(latch.await(30, TimeUnit.SECONDS), "CloudEvent was not routed within 30 seconds");
      assertEquals(expectedTenant, routedTenant.get());
      assertEquals(published.id(), routedEvent.get().id());
      assertEquals(published.type(), routedEvent.get().type());
      assertEquals(42, routedEvent.get().data().path("answer").intValue());
      assertTrue(consumer.running(), () -> String.valueOf(consumer.failure()));
      assertNotNull(routedAt.get());
    }
  }

  private static WorkflowCloudEvent event(TenantId tenant) {
    return new WorkflowCloudEvent(
        "1.0",
        "kafka-transport-event-1",
        URI.create("urn:forwardmeasure:workflow:kafka-transport-test"),
        "com.forwardmeasure.workflow.completed.v1",
        "execution-kafka-1",
        Instant.parse("2026-08-15T12:00:00Z"),
        "application/json",
        JsonNodeFactory.instance.objectNode().put("answer", 42),
        Map.of("tenant", JsonNodeFactory.instance.textNode(tenant.toString())));
  }

  private static void createTopic(String name) throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
    }
  }

  private static Properties baseProperties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
    return properties;
  }
}
