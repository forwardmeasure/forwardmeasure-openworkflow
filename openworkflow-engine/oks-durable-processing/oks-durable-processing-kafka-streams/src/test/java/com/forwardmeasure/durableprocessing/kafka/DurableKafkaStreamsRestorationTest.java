package com.forwardmeasure.durableprocessing.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import com.forwardmeasure.durableprocessing.api.DurableProcess;
import com.forwardmeasure.durableprocessing.api.DurableTransition;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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
 * Standalone, real-broker proof of the reusable durability layer. This test deliberately contains
 * no OpenWorkflow types.
 */
@Testcontainers
class DurableKafkaStreamsRestorationTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Instant REQUESTED = Instant.parse("2026-07-28T21:00:00Z");
  private static final String PROCESS_ID = "raw-process-1";

  @Container
  static final RedpandaContainer KAFKA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  @TempDir Path stateDirectories;

  @Test
  void restoresAndResumesRawDurableProcessWithoutReplayingWork() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "durable-raw-" + suffix;
    DurableTopics topics = DurableTopics.withPrefix("test.durable.raw." + suffix);
    createTopics(topics);
    var topology = topology(topics);

    try (var producer = producer(topics);
        var history = consumer(topics.history(), RawEvent.class, "history");
        var deadLetters = consumer(topics.deadLetters(), DurableDeadLetter.class, "dead-letter")) {
      /*
       * Queue PAUSE before Streams starts. START commits revision 1 and
       * appends ADVANCE(1) to the command topic. PAUSE commits revision
       * 2 first, so that continuation is then rejected as stale.
       */
      producer
          .send(
              record(
                  topics, command("start-1", RawAction.START, null, "extract evidence", REQUESTED)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      producer
          .send(
              record(
                  topics,
                  command("pause-1", RawAction.PAUSE, null, null, REQUESTED.plusSeconds(1))))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      producer.flush();

      AtomicReference<Throwable> streamFailure = new AtomicReference<>();
      KafkaStreams original =
          streams(applicationId, topology, stateDirectories.resolve("original"), streamFailure);
      try {
        original.start();
        awaitRunning(original, streamFailure);
        List<RawEvent> initial = awaitRecords(history, 2, original, streamFailure);
        assertEquals(RawEventType.STARTED, initial.get(0).type());
        assertEquals(RawEventType.PAUSED, initial.get(1).type());

        RawState paused = awaitState(original, topology.stores(), state -> state.paused());
        assertEquals(0, paused.completedSteps());
        DurableAggregateMetadata metadata = awaitMetadata(original, topology.stores(), 2);
        assertEquals(PROCESS_ID, metadata.aggregateKey());
      } finally {
        assertTrue(original.close(Duration.ofSeconds(10)));
      }

      /*
       * A different local directory proves recovery is from Kafka
       * changelogs, not files left behind by the old process.
       */
      KafkaStreams replacement =
          streams(
              applicationId,
              topology(topics),
              stateDirectories.resolve("replacement"),
              streamFailure);
      try {
        replacement.start();
        awaitRunning(replacement, streamFailure);
        RawState restored = awaitState(replacement, topology.stores(), state -> state.paused());
        assertEquals("extract evidence", restored.instruction());

        producer
            .send(
                record(
                    topics,
                    command("resume-1", RawAction.RESUME, null, null, REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        producer.flush();

        List<RawEvent> resumed = awaitRecords(history, 4, replacement, streamFailure);
        assertEquals(RawEventType.RESUMED, resumed.get(0).type());
        assertEquals(
            List.of(1, 2, 3),
            resumed.stream()
                .filter(event -> event.type() == RawEventType.STEP_DONE)
                .map(RawEvent::completedSteps)
                .toList());
        RawState completed = awaitState(replacement, topology.stores(), RawState::completed);
        assertEquals(3, completed.completedSteps());
        assertEquals(6, awaitMetadata(replacement, topology.stores(), 6).revision());

        // Exact redelivery has no state or history effect.
        producer
            .send(
                record(
                    topics,
                    command("start-1", RawAction.START, null, "extract evidence", REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        producer.flush();
        assertTrue(history.poll(Duration.ofSeconds(3)).isEmpty());

        // Reusing an external command ID with different content is
        // explicit poison data, isolated on the dead-letter topic.
        producer
            .send(
                record(
                    topics,
                    command("start-1", RawAction.START, null, "different instruction", REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        producer.flush();
        DurableDeadLetter rejected =
            awaitRecords(deadLetters, 1, replacement, streamFailure).getFirst();
        assertEquals(PROCESS_ID, rejected.aggregateKey());
        assertEquals("start-1", rejected.commandId());
        assertTrue(rejected.failureType().endsWith("CommandIdentityConflictException"));
      } finally {
        assertTrue(replacement.close(Duration.ofSeconds(10)));
      }
    }
  }

  private static DurableKafkaStreamsTopology<RawState, RawCommand, RawEvent, String> topology(
      DurableTopics topics) {
    return new DurableKafkaStreamsTopology<>(
        "raw",
        "raw-process",
        topics,
        new JacksonSerde<>(RawState.class),
        new JacksonSerde<>(RawCommand.class),
        new JacksonSerde<>(RawEvent.class),
        Serdes.String(),
        metadata(),
        ignored -> process());
  }

  private static DurableCommandMetadata<RawCommand> metadata() {
    return new DurableCommandMetadata<>() {
      @Override
      public String aggregateKey(RawCommand command) {
        return command.processId();
      }

      @Override
      public String commandId(RawCommand command) {
        return command.commandId();
      }

      @Override
      public Instant requestedAt(RawCommand command) {
        return command.requestedAt();
      }

      @Override
      public OptionalLong expectedRevision(RawCommand command) {
        return command.expectedRevision() == null
            ? OptionalLong.empty()
            : OptionalLong.of(command.expectedRevision());
      }

      @Override
      public boolean deduplicate(RawCommand command) {
        return command.action() != RawAction.ADVANCE;
      }
    };
  }

  private static DurableProcess<RawState, RawCommand, RawEvent, String> process() {
    return (context, current, command) ->
        switch (command.action()) {
          case START -> {
            if (current != null) {
              throw new IllegalArgumentException("Process is already started");
            }
            RawState started = new RawState(command.instruction(), 0, false, false);
            yield DurableTransition.changed(
                started,
                List.of(event(RawEventType.STARTED, started)),
                List.of(advance(context.nextRevision())));
          }
          case PAUSE -> {
            requireActive(current);
            RawState paused =
                new RawState(current.instruction(), current.completedSteps(), true, false);
            yield DurableTransition.changed(
                paused, List.of(event(RawEventType.PAUSED, paused)), List.of());
          }
          case RESUME -> {
            requireActive(current);
            if (!current.paused()) {
              throw new IllegalArgumentException("Process is not paused");
            }
            RawState resumed =
                new RawState(current.instruction(), current.completedSteps(), false, false);
            yield DurableTransition.changed(
                resumed,
                List.of(event(RawEventType.RESUMED, resumed)),
                List.of(advance(context.nextRevision())));
          }
          case CANCEL -> {
            requireActive(current);
            RawState cancelled =
                new RawState(
                    current.instruction(), current.completedSteps(), current.paused(), true);
            yield DurableTransition.changed(
                cancelled, List.of(event(RawEventType.CANCELLED, cancelled)), List.of());
          }
          case ADVANCE -> {
            requireActive(current);
            if (current.paused()) {
              throw new IllegalArgumentException("Paused process cannot advance");
            }
            int completedSteps = Math.addExact(current.completedSteps(), 1);
            RawState advanced = new RawState(current.instruction(), completedSteps, false, false);
            yield DurableTransition.changed(
                advanced,
                List.of(event(RawEventType.STEP_DONE, advanced)),
                completedSteps == 3 ? List.of() : List.of(advance(context.nextRevision())));
          }
        };
  }

  private static void requireActive(RawState state) {
    if (state == null) {
      throw new IllegalArgumentException("Process is not started");
    }
    if (state.completed() || state.cancelled()) {
      throw new IllegalArgumentException("Process is terminal");
    }
  }

  private static RawCommand advance(long expectedRevision) {
    return command(
        "advance-" + expectedRevision,
        RawAction.ADVANCE,
        expectedRevision,
        null,
        REQUESTED.plusMillis(expectedRevision));
  }

  private static RawCommand command(
      String commandId,
      RawAction action,
      Long expectedRevision,
      String instruction,
      Instant requestedAt) {
    return new RawCommand(
        PROCESS_ID, commandId, action, expectedRevision, instruction, requestedAt);
  }

  private static RawEvent event(RawEventType type, RawState state) {
    return new RawEvent(type, state.completedSteps(), state.paused(), state.cancelled());
  }

  private static ProducerRecord<String, RawCommand> record(
      DurableTopics topics, RawCommand command) {
    return new ProducerRecord<>(topics.commands(), command.processId(), command);
  }

  private static KafkaStreams streams(
      String applicationId,
      DurableKafkaStreamsTopology<RawState, RawCommand, RawEvent, String> durableTopology,
      Path stateDirectory,
      AtomicReference<Throwable> streamFailure) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    var topology = new org.apache.kafka.streams.Topology();
    durableTopology.addTo(topology);
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.setUncaughtExceptionHandler(
        exception -> {
          streamFailure.set(exception);
          return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
              .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
    return streams;
  }

  private static KafkaProducer<String, RawCommand> producer(DurableTopics topics) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(
        properties, new StringSerializer(), new JacksonSerde<>(RawCommand.class).serializer());
  }

  private static <T> KafkaConsumer<String, T> consumer(
      String topic, Class<T> type, String groupPrefix) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    KafkaConsumer<String, T> consumer =
        new KafkaConsumer<>(
            properties, new StringDeserializer(), new JacksonSerde<>(type).deserializer());
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private static <T> List<T> awaitRecords(
      KafkaConsumer<String, T> consumer,
      int count,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    List<T> values = new ArrayList<>();
    while (values.size() < count && System.nanoTime() < deadline) {
      failIfStreamsFailed(streamFailure);
      consumer.poll(Duration.ofMillis(250)).forEach(record -> values.add(record.value()));
    }
    assertEquals(
        count, values.size(), "Timed out waiting for records; streams state=" + streams.state());
    return values;
  }

  private static RawState awaitState(
      KafkaStreams streams,
      DurableTopologyDescriptor stores,
      java.util.function.Predicate<RawState> expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, RawState> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    stores.stateStore(), QueryableStoreTypes.keyValueStore()));
        RawState state = store.get(PROCESS_ID);
        if (state != null && expected.test(state)) {
          return state;
        }
      } catch (InvalidStateStoreException notReady) {
        // Restoration is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for durable state");
  }

  private static DurableAggregateMetadata awaitMetadata(
      KafkaStreams streams, DurableTopologyDescriptor stores, long expectedRevision)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, DurableAggregateMetadata> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    stores.metadataStore(), QueryableStoreTypes.keyValueStore()));
        DurableAggregateMetadata metadata = store.get(PROCESS_ID);
        if (metadata != null && metadata.revision() == expectedRevision) {
          return metadata;
        }
      } catch (InvalidStateStoreException notReady) {
        // Restoration is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for revision " + expectedRevision);
  }

  private static void awaitRunning(KafkaStreams streams, AtomicReference<Throwable> streamFailure)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (streams.state() != KafkaStreams.State.RUNNING && System.nanoTime() < deadline) {
      failIfStreamsFailed(streamFailure);
      Thread.sleep(50);
    }
    failIfStreamsFailed(streamFailure);
    assertEquals(KafkaStreams.State.RUNNING, streams.state());
  }

  private static void failIfStreamsFailed(AtomicReference<Throwable> streamFailure) {
    if (streamFailure.get() != null) {
      throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
    }
  }

  private static void createTopics(DurableTopics topics) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(topics.commands(), 3, (short) 1),
                  new NewTopic(topics.history(), 3, (short) 1),
                  new NewTopic(topics.outbox(), 3, (short) 1),
                  new NewTopic(topics.deadLetters(), 3, (short) 1)))
          .all()
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
  }

  private enum RawAction {
    START,
    ADVANCE,
    PAUSE,
    RESUME,
    CANCEL
  }

  private enum RawEventType {
    STARTED,
    STEP_DONE,
    PAUSED,
    RESUMED,
    CANCELLED
  }

  private record RawCommand(
      String processId,
      String commandId,
      RawAction action,
      Long expectedRevision,
      String instruction,
      Instant requestedAt) {}

  private record RawState(
      String instruction, int completedSteps, boolean paused, boolean cancelled) {
    boolean completed() {
      return completedSteps == 3;
    }
  }

  private record RawEvent(
      RawEventType type, int completedSteps, boolean paused, boolean cancelled) {}
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
