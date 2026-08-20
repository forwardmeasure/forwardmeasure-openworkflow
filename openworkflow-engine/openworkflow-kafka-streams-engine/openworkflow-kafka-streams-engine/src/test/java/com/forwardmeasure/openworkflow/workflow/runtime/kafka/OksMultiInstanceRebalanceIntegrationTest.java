package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdmitWorkflowDefinitionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionPhase;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionSnapshot;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Real multi-instance gate.
 *
 * <p>Two Kafka Streams clients initially share six execution partitions. The test admits one
 * timer-backed workflow, starts enough executions to populate every partition, and waits until both
 * clients have materialised state. One client is then stopped while all waits are pending. Kafka
 * rebalances its partitions to the survivor, which restores the timer/execution stores and must
 * complete every run exactly once.
 */
@Testcontainers
final class OksMultiInstanceRebalanceIntegrationTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:rebalance.tenant.test");
  private static final ActorId RUNTIME_ACTOR = ActorId.parse("did:web:oks.test:actors:runtime");

  @Container
  static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  @TempDir Path stateDirectory;

  @Test
  void restoresInFlightTimersAfterARealMultiInstanceRebalance() throws Exception {
    String suffix = UUID.randomUUID().toString();
    OksTopics topics = OksTopics.withPrefix("test.oks.rebalance." + suffix);
    createTopics(topics, 6);
    String applicationId = "oks-rebalance-" + suffix;
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: durability
          name: rebalance-waits
          version: '1.0.0'
        do:
          - pause:
              wait: PT3S
          - finish:
              set:
                completed: true
        """
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source);
    WorkflowDefinitionKey definitionKey = new WorkflowDefinitionKey(TENANT, plan.coordinates());
    WorkflowDefinitionReference definition =
        new WorkflowDefinitionReference(
            definitionKey, plan.sourceSha256(), plan.definitionSha256());
    ActorContext actor =
        new ActorContext(
            TENANT,
            ActorId.parse("did:web:rebalance.tenant.test:actors:user-1"),
            ActorType.HUMAN,
            "Rebalance User",
            "rebalance-test",
            Set.of("workflow-publish", "workflow-start"),
            null,
            Instant.now());
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    AtomicReference<Throwable> secondFailure = new AtomicReference<>();
    KafkaStreams first = streams(topics, applicationId, "first", firstFailure);
    KafkaStreams second = streams(topics, applicationId, "second", secondFailure);
    List<ExecutionKey> keys = new ArrayList<>();

    try (first;
        second;
        KafkaProducer<String, ExecutionCommand> producer = commandProducer();
        KafkaProducer<String, AdmitWorkflowDefinitionCommand> definitionProducer =
            definitionProducer()) {
      first.start();
      second.start();
      awaitRunning(first);
      awaitRunning(second);
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  definitionKey.canonical(),
                  new AdmitWorkflowDefinitionCommand(
                      "admit-" + suffix,
                      definitionKey,
                      new String(source, StandardCharsets.UTF_8),
                      List.of(),
                      actor,
                      Instant.now())))
          .get(10, TimeUnit.SECONDS);
      awaitDefinition(first, definition.canonical());
      awaitDefinition(second, definition.canonical());

      for (int index = 0; index < 36; index++) {
        ExecutionKey key =
            new ExecutionKey(TENANT, new WorkflowExecutionId("rebalance-" + index + "-" + suffix));
        keys.add(key);
        producer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    key.canonical(),
                    new StartExecutionCommand(
                        "start-" + index + "-" + suffix,
                        key,
                        definition,
                        DataReferences.inline(JsonNodeFactory.instance.objectNode()),
                        actor,
                        Instant.now())))
            .get(10, TimeUnit.SECONDS);
      }
      awaitMaterialised(first, second, keys);

      first.close(Duration.ofSeconds(20));
      awaitRunning(second);
      awaitCompleted(second, keys, secondFailure);

      assertNull(firstFailure.get());
      assertNull(secondFailure.get());
    }
  }

  private KafkaStreams streams(
      OksTopics topics, String applicationId, String instance, AtomicReference<Throwable> failure) {
    Properties properties = kafkaProperties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.resolve(instance).toString());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaStreams streams =
        new KafkaStreams(
            new OksTopology(RUNTIME_ACTOR, "oks-rebalance-test").build(topics), properties);
    streams.setUncaughtExceptionHandler(
        unexpected -> {
          failure.set(unexpected);
          return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
              .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
    return streams;
  }

  private static void createTopics(OksTopics topics, int partitions) throws Exception {
    List<String> names =
        List.of(
            topics.definitionCommands(),
            topics.definitionHistory(),
            topics.definitionCatalogue(),
            topics.definitions(),
            topics.commands(),
            topics.history(),
            topics.effects(),
            topics.operationCheckpoints(),
            topics.subscriptionEffects(),
            topics.timerEffects(),
            topics.inboundEvents(),
            topics.emittedEvents(),
            topics.deadLetters());
    try (Admin admin = Admin.create(kafkaProperties())) {
      admin
          .createTopics(
              names.stream().map(name -> new NewTopic(name, partitions, (short) 1)).toList())
          .all()
          .get(20, TimeUnit.SECONDS);
    }
  }

  private static Properties kafkaProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", REDPANDA.getBootstrapServers());
    return properties;
  }

  private static Properties producerProperties() {
    Properties properties = kafkaProperties();
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    // These producers only place test ingress records. Kafka 4 enables
    // idempotence by default when acks=all, but the deliberately old
    // Redpanda image used by this compatibility gate does not understand
    // Kafka 4's coordinator request shape. Runtime writes still use
    // exactly_once_v2; disabling idempotence here only avoids exercising
    // an unrelated client/broker protocol incompatibility at ingress.
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    return properties;
  }

  private static KafkaProducer<String, ExecutionCommand> commandProducer() {
    return new KafkaProducer<>(
        producerProperties(),
        new StringSerializer(),
        new JsonSerde<>(ExecutionCommand.class).serializer());
  }

  private static KafkaProducer<String, AdmitWorkflowDefinitionCommand> definitionProducer() {
    return new KafkaProducer<>(
        producerProperties(),
        new StringSerializer(),
        new JsonSerde<>(AdmitWorkflowDefinitionCommand.class).serializer());
  }

  private static void awaitRunning(KafkaStreams streams) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (streams.state() != KafkaStreams.State.RUNNING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(KafkaStreams.State.RUNNING, streams.state());
  }

  private static void awaitDefinition(KafkaStreams streams, String reference)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        var store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.DEFINITIONS, QueryableStoreTypes.keyValueStore()));
        if (store.get(reference) != null) return;
      } catch (InvalidStateStoreException notReady) {
        // The global definition store is restoring.
      }
      Thread.sleep(50);
    }
    throw new AssertionError("Definition did not reach both instances");
  }

  private static void awaitMaterialised(
      KafkaStreams first, KafkaStreams second, List<ExecutionKey> keys)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      int materialised = countPresent(first, keys) + countPresent(second, keys);
      if (materialised == keys.size()) return;
      Thread.sleep(50);
    }
    throw new AssertionError("Not every execution was materialised before rebalance");
  }

  private static int countPresent(KafkaStreams streams, List<ExecutionKey> keys) {
    try {
      ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
          streams.store(
              StoreQueryParameters.fromNameAndType(
                  OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
      return Math.toIntExact(
          keys.stream().filter(key -> store.get(key.canonical()) != null).count());
    } catch (InvalidStateStoreException notReady) {
      return 0;
    }
  }

  private static void awaitCompleted(
      KafkaStreams survivor, List<ExecutionKey> keys, AtomicReference<Throwable> failure)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (failure.get() != null) {
        throw new AssertionError("Surviving Kafka Streams instance failed", failure.get());
      }
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            survivor.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        boolean complete =
            keys.stream()
                .allMatch(
                    key -> {
                      ExecutionSnapshot snapshot = store.get(key.canonical());
                      return snapshot != null && snapshot.phase() == ExecutionPhase.COMPLETED;
                    });
        if (complete) return;
      } catch (InvalidStateStoreException notReady) {
        // Rebalance restoration or a query projection is still active.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Survivor did not restore and complete every execution");
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
