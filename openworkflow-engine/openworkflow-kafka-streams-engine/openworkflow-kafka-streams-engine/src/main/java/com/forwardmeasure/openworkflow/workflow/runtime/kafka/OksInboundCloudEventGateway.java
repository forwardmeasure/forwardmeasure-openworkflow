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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Transactional publisher for the CloudEvents HTTP ingress: the only production path that ever puts
 * a record onto {@link OksTopics#inboundEvents()}. Produces exactly the record shape {@link
 * OksInboundEventProcessor} already consumes and {@code OksRestorationIntegrationTest} already
 * proves end-to-end - keyed by {@code event.tenantId().toString()}, value serialized with the
 * shared {@link JsonSerde}. Mirrors {@link OksKafkaCommandGateway}'s transactional producer
 * conventions exactly (idempotence, {@code acks=all}, a dedicated transactional id,
 * begin/commit/abort around a single send).
 */
public final class OksInboundCloudEventGateway implements AutoCloseable {
  private final KafkaProducer<String, byte[]> producer;
  private final String topic;
  private final JsonSerde<InboundCloudEvent> events = new JsonSerde<>(InboundCloudEvent.class);

  public OksInboundCloudEventGateway(
      String bootstrapServers, String transactionalId, OksTopics topics) {
    this(topics.inboundEvents(), producer(bootstrapServers, transactionalId));
    producer.initTransactions();
  }

  OksInboundCloudEventGateway(String topic, KafkaProducer<String, byte[]> producer) {
    this.topic = Objects.requireNonNull(topic, "topic");
    this.producer = Objects.requireNonNull(producer, "producer");
  }

  public synchronized CompletionStage<Void> publish(InboundCloudEvent event) {
    Objects.requireNonNull(event, "event");
    try {
      producer.beginTransaction();
      producer
          .send(
              new ProducerRecord<>(
                  topic, event.tenantId().toString(), events.serializer().serialize(topic, event)))
          .get();
      producer.commitTransaction();
      return CompletableFuture.completedFuture(null);
    } catch (Exception failure) {
      try {
        producer.abortTransaction();
      } catch (RuntimeException ignored) {
      }
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static KafkaProducer<String, byte[]> producer(
      String bootstrapServers, String transactionalId) {
    Properties values = new Properties();
    values.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    values.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    values.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    values.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    values.put(ProducerConfig.ACKS_CONFIG, "all");
    values.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    return new KafkaProducer<>(values);
  }

  @Override
  public void close() {
    producer.close();
  }
}
