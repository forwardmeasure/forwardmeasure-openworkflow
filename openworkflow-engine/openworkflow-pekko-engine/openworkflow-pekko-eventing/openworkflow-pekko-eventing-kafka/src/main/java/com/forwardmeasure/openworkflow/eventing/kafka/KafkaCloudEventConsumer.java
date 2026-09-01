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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.CloudEventsMapper;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka transport consumer for Pekko's {@code publish:emit:}/{@code receive:}/{@code listen:}
 * CloudEvents pub-sub.
 *
 * <p>Unlike the HTTP transport - where an external subscriber receives the published event over
 * HTTP and separately calls {@code CloudEventIngressResource} back to route it, using ITS OWN
 * authenticated tenant - this Kafka transport is a closed loop entirely within ForwardMeasure
 * OpenWorkflow: {@link KafkaCloudEventPublisher} produces to one topic and this consumer reads from
 * that same topic and calls {@link CloudEventIngress#route} directly. There is no authenticated
 * caller to supply the tenant, so it is instead recovered from the event itself: the durable {@code
 * tenant} CloudEvents extension that every lifecycle event already carries (see {@code
 * LifecycleCloudEventMapper}). A record published without it - a user-authored {@code emit:} event
 * whose template did not set a {@code tenant} property - can never be routed; it is logged and its
 * offset is committed anyway (skipped) rather than retried forever, since retrying cannot make it
 * routable.
 *
 * <p>Lifecycle mirrors {@code KafkaOperationAdapterDispatcher}'s conventions: a {@link
 * Properties}-based configuration (not a shared config record), a {@link StringDeserializer} key
 * and {@link ByteArrayDeserializer} value, manual offset commits (only once {@link
 * CloudEventIngress#route} has completed successfully), {@code isolation.level=read_committed} and
 * {@code auto.offset.reset=earliest}, a dedicated {@link Thread#ofVirtual()} consumer loop, and
 * {@code wakeup()}-based clean shutdown guarded by an {@link AtomicBoolean}. Unlike the dispatcher,
 * this consumer processes records within one partition strictly in order, one at a time, blocking
 * on each {@code route()} call before advancing: the dispatcher's windowed in-flight tracking
 * exists to bound concurrent external I/O for slow, arbitrary protocol adapters, but this
 * consumer's per-record work is a single bounded Pekko {@code ask}, so that complexity is not
 * needed here. A record whose {@code route()} call fails or times out is not committed - the
 * consumer seeks back to it so the same record is retried (rather than silently advancing past it)
 * on the next poll, and a process restart resumes correctly from the last committed offset.
 */
public final class KafkaCloudEventConsumer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCloudEventConsumer.class);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);

  private final Properties baseProperties;
  private final String topic;
  private final String groupId;
  private final String instanceId;
  private final CloudEventIngress ingress;
  private final CloudEventsMapper mapper;
  private final Duration routeTimeout;
  private final Clock clock;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private volatile KafkaConsumer<String, byte[]> consumer;
  private volatile Thread thread;

  public KafkaCloudEventConsumer(
      Properties kafkaProperties,
      String topic,
      String groupId,
      String instanceId,
      CloudEventIngress ingress,
      ObjectMapper json,
      Duration routeTimeout) {
    this(
        kafkaProperties,
        topic,
        groupId,
        instanceId,
        ingress,
        json,
        routeTimeout,
        Clock.systemUTC());
  }

  /** Test seam: inject a fixed {@link Clock} instead of the real one. */
  KafkaCloudEventConsumer(
      Properties kafkaProperties,
      String topic,
      String groupId,
      String instanceId,
      CloudEventIngress ingress,
      ObjectMapper json,
      Duration routeTimeout,
      Clock clock) {
    this.baseProperties = new Properties();
    this.baseProperties.putAll(Objects.requireNonNull(kafkaProperties, "kafkaProperties"));
    this.topic = requireText(topic, "topic");
    this.groupId = requireText(groupId, "groupId");
    this.instanceId = requireText(instanceId, "instanceId");
    this.ingress = Objects.requireNonNull(ingress, "ingress");
    this.mapper = new CloudEventsMapper(Objects.requireNonNull(json, "json"));
    this.routeTimeout = Objects.requireNonNull(routeTimeout, "routeTimeout");
    if (routeTimeout.isZero() || routeTimeout.isNegative()) {
      throw new IllegalArgumentException("routeTimeout must be positive");
    }
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public synchronized void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Kafka CloudEvent consumer is already running");
    }
    thread = Thread.ofVirtual().name("pekko-cloud-events-consumer-" + instanceId).start(this::loop);
  }

  public boolean running() {
    return running.get() && failure.get() == null;
  }

  public Throwable failure() {
    return failure.get();
  }

  private void loop() {
    try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties())) {
      consumer = kafkaConsumer;
      kafkaConsumer.subscribe(List.of(topic));
      while (running.get()) {
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(POLL_TIMEOUT);
        partitionLoop:
        for (TopicPartition partition : records.partitions()) {
          for (ConsumerRecord<String, byte[]> record : records.records(partition)) {
            if (!running.get()) break partitionLoop;
            if (!process(kafkaConsumer, record)) break;
          }
        }
      }
    } catch (WakeupException expected) {
      if (running.get()) fail(expected);
    } catch (Throwable unexpected) {
      fail(unexpected);
    } finally {
      consumer = null;
    }
  }

  /**
   * Returns {@code true} once this record's offset is safely committed - either a successful route,
   * or an unroutable poison record that is skipped. Returns {@code false} for a retryable routing
   * failure, having first seeked the consumer back to this record's offset so the next poll
   * re-delivers it instead of silently skipping past it.
   */
  private boolean process(
      KafkaConsumer<String, byte[]> kafkaConsumer, ConsumerRecord<String, byte[]> record) {
    WorkflowCloudEvent event;
    TenantId tenantId;
    try {
      event = decode(record.value());
      tenantId = tenant(event);
    } catch (RuntimeException malformed) {
      LOG.error(
          "Skipping unroutable CloudEvent at {}-{}@{}",
          record.topic(),
          record.partition(),
          record.offset(),
          malformed);
      commit(kafkaConsumer, record);
      return true;
    }
    try {
      ingress
          .route(tenantId, event, clock.instant())
          .toCompletableFuture()
          .get(routeTimeout.toMillis(), TimeUnit.MILLISECONDS);
      commit(kafkaConsumer, record);
      return true;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      seekBack(kafkaConsumer, record);
      return false;
    } catch (ExecutionException | TimeoutException retryable) {
      LOG.warn(
          "CloudEvent routing failed for {}-{}@{}, will retry",
          record.topic(),
          record.partition(),
          record.offset(),
          retryable);
      seekBack(kafkaConsumer, record);
      return false;
    }
  }

  private WorkflowCloudEvent decode(byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("CloudEvent record has no value");
    }
    var format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    if (format == null) {
      throw new IllegalStateException("CloudEvents JSON event format is unavailable");
    }
    return mapper.fromSdk(format.deserialize(value));
  }

  private static TenantId tenant(WorkflowCloudEvent event) {
    var tenant = event.extensions().get("tenant");
    if (tenant == null || !tenant.isTextual() || tenant.textValue().isBlank()) {
      throw new IllegalArgumentException(
          "CloudEvent " + event.id() + " has no routable tenant extension");
    }
    return new TenantId(tenant.textValue());
  }

  private static void seekBack(
      KafkaConsumer<String, byte[]> kafkaConsumer, ConsumerRecord<String, byte[]> record) {
    kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
  }

  private static void commit(
      KafkaConsumer<String, byte[]> kafkaConsumer, ConsumerRecord<String, byte[]> record) {
    kafkaConsumer.commitSync(
        Map.of(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)));
  }

  private void fail(Throwable unexpected) {
    failure.compareAndSet(null, unexpected);
    running.set(false);
    wakeup();
  }

  private void wakeup() {
    KafkaConsumer<String, byte[]> current = consumer;
    if (current != null) current.wakeup();
  }

  /** Idempotent: a never-started or already-closed consumer safely no-ops. */
  @Override
  public synchronized void close() {
    if (!running.getAndSet(false) && thread == null) {
      return;
    }
    wakeup();
    join(thread);
    thread = null;
  }

  private static void join(Thread candidate) {
    if (candidate == null) return;
    try {
      candidate.join(Duration.ofSeconds(10));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private Properties consumerProperties() {
    Properties properties = new Properties();
    properties.putAll(baseProperties);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "pekko-cloud-events-" + instanceId);
    return properties;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
