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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.JsonSerde;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksInboundCloudEventGateway;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksTopics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the full, real path this task exists to close: an external system POSTs a CloudEvent to
 * {@link OksCloudEventIngressResource}, which authorizes it, and produces it onto a real broker's
 * {@code inbound-events} topic in exactly the shape {@code OksInboundEventProcessor} (unchanged)
 * already consumes - the same shape {@code OksRestorationIntegrationTest}'s {@code
 * inboundEventProducer()} already proves that consumer accepts: keyed by {@code
 * tenantId.toString()}, value serialized with {@code JsonSerde<InboundCloudEvent>}.
 *
 * <p>This test does not run a full {@code OksKafkaRuntime}/topology - {@code
 * OksRestorationIntegrationTest} (in {@code openworkflow-kafka-streams-engine}) already proves the
 * consumer side end-to-end, including subscription routing, against exactly this record shape. What
 * was missing, and what this proves, is the production path that gets a record onto the topic in
 * the first place: this test consumes the record this resource produced with a plain {@code
 * KafkaConsumer<String, InboundCloudEvent>} and asserts it is byte-shape-identical to what that
 * other test's raw producer already writes.
 */
@Testcontainers
class OksCloudEventIngressResourceKafkaIntegrationTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Container
  static final RedpandaContainer KAFKA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  @Test
  void postedStructuredCloudEventArrivesOnInboundEventsInTheConsumerExpectedShape()
      throws Exception {
    String suffix = UUID.randomUUID().toString();
    OksTopics topics = OksTopics.withPrefix("test.oks.jaxrs.structured." + suffix);
    createInboundEventsTopic(topics);
    TenantId tenantUuid = new TenantId(UUID.randomUUID());
    String organizationId = "org-" + suffix;
    ActiveOrganization organization =
        new ActiveOrganization(tenantUuid, organizationId, "actor-42", Set.of("event-ingress"));
    RecordingAuthorizationService authorization = new RecordingAuthorizationService(true);

    try (OksInboundCloudEventGateway gateway =
            new OksInboundCloudEventGateway(
                KAFKA.getBootstrapServers(), "test-ingress-" + suffix, topics);
        KafkaConsumer<String, InboundCloudEvent> consumer = inboundEventConsumer(topics)) {
      var resource =
          new OksCloudEventIngressResource(
              gateway, () -> organization, authorization, JSON, java.time.Clock.systemUTC());

      String body =
          """
          {
            "specversion": "1.0",
            "id": "evidence-42",
            "source": "https://events.example.test",
            "type": "evidence.received.v1",
            "data": {"evidenceId": "e-42"}
          }
          """;
      var response =
          resource
              .ingest(
                  "application/cloudevents+json",
                  Map.of("content-type", List.of("application/cloudevents+json")),
                  body.getBytes(java.nio.charset.StandardCharsets.UTF_8))
              .toCompletableFuture()
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);

      assertEquals(202, response.getStatus());
      var accepted = (CloudEventIngressAcceptedDocument) response.getEntity();
      assertEquals("evidence-42", accepted.eventId());
      assertEquals("https://events.example.test", accepted.eventSource());

      // authorization.requireAuthorized was called with the expected action/resource/tenant.
      assertNotNull(authorization.lastRequest);
      assertEquals("event:route", authorization.lastRequest.action().scope());
      assertEquals("openworkflow-event-target", authorization.lastRequest.resource().type());
      assertEquals(organizationId, authorization.lastRequest.organization().organizationId());

      // The real, decisive proof: consume the raw record this resource produced and confirm it
      // is exactly what OksInboundEventProcessor (and OksRestorationIntegrationTest's producer)
      // expect.
      ConsumerRecords<String, InboundCloudEvent> records = pollUntilNotEmpty(consumer);
      assertEquals(1, records.count());
      ConsumerRecord<String, InboundCloudEvent> record = records.iterator().next();

      String expectedTenantDid = "did:forwardmeasure:tenant:" + tenantUuid.value();
      assertEquals(expectedTenantDid, record.key(), "record must be keyed by the tenant DID");

      InboundCloudEvent inbound = record.value();
      assertEquals(expectedTenantDid, inbound.tenantId().toString());
      assertEquals("evidence-42", inbound.event().inlineValue().required("id").textValue());
      assertEquals(
          "https://events.example.test",
          inbound.event().inlineValue().required("source").textValue());
      assertEquals(
          "evidence.received.v1", inbound.event().inlineValue().required("type").textValue());
      assertEquals(
          "e-42", inbound.event().inlineValue().path("data").path("evidenceId").textValue());
      assertEquals(expectedTenantDid, inbound.acceptedBy().tenantId().toString());
      assertEquals("did:forwardmeasure:actor:actor-42", inbound.acceptedBy().actorId().toString());
      assertTrue(inbound.acceptedBy().roles().contains("event-ingress"));
      assertEquals(organizationId, inbound.acceptedBy().organizationId());

      // eventKey() is what OksInboundEventProcessor deduplicates on - confirm it round-trips.
      assertEquals(inbound.eventKey(), accepted.eventKey());
    }
  }

  @Test
  void postedBinaryModeCloudEventDecodesHeadersIntoTheSameEnvelopeShape() throws Exception {
    String suffix = UUID.randomUUID().toString();
    OksTopics topics = OksTopics.withPrefix("test.oks.jaxrs.binary." + suffix);
    createInboundEventsTopic(topics);
    TenantId tenantUuid = new TenantId(UUID.randomUUID());
    ActiveOrganization organization =
        new ActiveOrganization(tenantUuid, "org-" + suffix, "actor-77", Set.of());
    RecordingAuthorizationService authorization = new RecordingAuthorizationService(true);

    try (OksInboundCloudEventGateway gateway =
            new OksInboundCloudEventGateway(
                KAFKA.getBootstrapServers(), "test-ingress-bin-" + suffix, topics);
        KafkaConsumer<String, InboundCloudEvent> consumer = inboundEventConsumer(topics)) {
      var resource =
          new OksCloudEventIngressResource(
              gateway, () -> organization, authorization, JSON, java.time.Clock.systemUTC());

      Map<String, List<String>> headers =
          Map.of(
              "ce-specversion", List.of("1.0"),
              "ce-id", List.of("evidence-99"),
              "ce-source", List.of("https://events.example.test"),
              "ce-type", List.of("evidence.received.v1"));
      byte[] body = "{\"evidenceId\":\"e-99\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

      var response =
          resource
              .ingest("application/json", headers, body)
              .toCompletableFuture()
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertEquals(202, response.getStatus());

      ConsumerRecords<String, InboundCloudEvent> records = pollUntilNotEmpty(consumer);
      assertEquals(1, records.count());
      InboundCloudEvent inbound = records.iterator().next().value();
      assertEquals("evidence-99", inbound.event().inlineValue().required("id").textValue());
      assertEquals(
          "e-99", inbound.event().inlineValue().path("data").path("evidenceId").textValue());
    }
  }

  private static ConsumerRecords<String, InboundCloudEvent> pollUntilNotEmpty(
      KafkaConsumer<String, InboundCloudEvent> consumer) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      ConsumerRecords<String, InboundCloudEvent> records = consumer.poll(Duration.ofMillis(500));
      if (!records.isEmpty()) {
        return records;
      }
    }
    throw new AssertionError("No record observed on inbound-events within " + TIMEOUT);
  }

  private static void createInboundEventsTopic(OksTopics topics) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      admin
          .createTopics(List.of(new NewTopic(topics.inboundEvents(), 3, (short) 1)))
          .all()
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
  }

  private static KafkaConsumer<String, InboundCloudEvent> inboundEventConsumer(OksTopics topics) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "oks-jaxrs-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // The gateway writes transactionally - only observe committed records, same isolation level
    // OksRestorationIntegrationTest's own consumers use.
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    var consumer =
        new KafkaConsumer<>(
            properties,
            new StringDeserializer(),
            new JsonSerde<>(InboundCloudEvent.class).deserializer());
    consumer.subscribe(List.of(topics.inboundEvents()));
    return consumer;
  }

  private static final class RecordingAuthorizationService implements AuthorizationService {
    private final boolean permitted;
    private volatile AuthorizationRequest lastRequest;

    RecordingAuthorizationService(boolean permitted) {
      this.permitted = permitted;
    }

    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      lastRequest = request;
      return new AuthorizationDecision(permitted, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }
}
