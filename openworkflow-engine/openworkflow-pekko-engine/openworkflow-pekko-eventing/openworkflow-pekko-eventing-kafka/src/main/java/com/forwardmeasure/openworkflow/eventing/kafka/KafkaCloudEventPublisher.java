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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.CloudEventsMapper;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventPublisher;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import java.time.Duration;
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
 * CloudEvents Kafka transport publisher/adapter port implementation. Writes the exact same
 * CloudEvents JSON structured-content wire format {@link
 * com.forwardmeasure.openworkflow.eventing.HttpCloudEventPublisher} already produces over HTTP -
 * the envelope is transport-neutral, only the carrier changes here, via the official {@code
 * io.cloudevents:cloudevents-json-jackson} SDK format.
 *
 * <p>Producer configuration mirrors {@code KafkaOperationAdapterDispatcher}'s conventions: a {@link
 * Properties}-based configuration (not a shared config record), a {@link StringSerializer} key and
 * {@link ByteArraySerializer} value, and an idempotent producer with {@code acks=all}.
 */
public final class KafkaCloudEventPublisher implements CloudEventPublisher, AutoCloseable {
  private final KafkaProducer<String, byte[]> producer;
  private final CloudEventsMapper mapper;
  private final String topic;

  public KafkaCloudEventPublisher(Properties kafkaProperties, String topic, ObjectMapper json) {
    this(new KafkaProducer<>(producerProperties(kafkaProperties)), topic, json);
  }

  /** Test seam: inject an already-constructed producer instead of building one from properties. */
  KafkaCloudEventPublisher(
      KafkaProducer<String, byte[]> producer, String topic, ObjectMapper json) {
    this.producer = Objects.requireNonNull(producer, "producer");
    this.topic = requireText(topic, "topic");
    this.mapper = new CloudEventsMapper(Objects.requireNonNull(json, "json"));
  }

  @Override
  public CompletionStage<Void> publish(String operationId, WorkflowCloudEvent event) {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(event, "event");
    var format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    if (format == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("CloudEvents JSON event format is unavailable"));
    }
    byte[] body = format.serialize(mapper.toSdk(event));
    var record = new ProducerRecord<>(topic, partitionKey(operationId, event), body);
    var result = new CompletableFuture<Void>();
    try {
      producer.send(
          record,
          (metadata, failure) -> {
            if (failure != null) {
              result.completeExceptionally(failure);
            } else {
              result.complete(null);
            }
          });
    } catch (RuntimeException immediate) {
      return CompletableFuture.failedFuture(immediate);
    }
    return result;
  }

  /** Idempotently releases the producer's network client and background I/O thread. */
  @Override
  public void close() {
    producer.close(Duration.ofSeconds(10));
  }

  /**
   * Tenant-qualified when the durable {@code tenant} CloudEvents extension is present - {@code
   * LifecycleCloudEventMapper} always sets it, so lifecycle events for one tenant/operation land on
   * the same partition - and falls back to the operation ID alone for a workflow-authored {@code
   * emit:} event whose template did not set a {@code tenant} property.
   */
  private static String partitionKey(String operationId, WorkflowCloudEvent event) {
    JsonNode tenant = event.extensions().get("tenant");
    return tenant != null && tenant.isTextual()
        ? tenant.textValue() + ":" + operationId
        : operationId;
  }

  private static Properties producerProperties(Properties base) {
    Properties properties = new Properties();
    properties.putAll(Objects.requireNonNull(base, "kafkaProperties"));
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
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
