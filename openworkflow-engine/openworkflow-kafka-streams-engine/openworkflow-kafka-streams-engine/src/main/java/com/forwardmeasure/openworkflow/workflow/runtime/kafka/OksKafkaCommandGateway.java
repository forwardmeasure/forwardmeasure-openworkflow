/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Transactional publisher translating the common product command into tenant-qualified OKS records.
 */
public final class OksKafkaCommandGateway implements KafkaCommandGateway, AutoCloseable {
  private final KafkaProducer<String, byte[]> producer;
  private final OksTopics topics;
  private final Clock clock;
  private final Map<java.util.UUID, PublishedCommand> receipts = new ConcurrentHashMap<>();
  private final Map<ExecutionId, Long> revisions = new ConcurrentHashMap<>();
  private final JsonSerde<WorkflowDefinitionBundle> bundles =
      new JsonSerde<>(WorkflowDefinitionBundle.class);
  private final JsonSerde<com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand>
      commands =
          new JsonSerde<>(
              com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand.class);

  public OksKafkaCommandGateway(
      String bootstrapServers, String transactionalId, OksTopics topics, Clock clock) {
    this(topics, clock, producer(bootstrapServers, transactionalId));
    producer.initTransactions();
  }

  OksKafkaCommandGateway(OksTopics topics, Clock clock, KafkaProducer<String, byte[]> producer) {
    this.topics = Objects.requireNonNull(topics, "topics");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.producer = Objects.requireNonNull(producer, "producer");
  }

  @Override
  public synchronized CompletionStage<PublishedCommand> publish(ExecutionCommandEnvelope envelope) {
    PublishedCommand duplicate = receipts.get(envelope.commandId());
    if (duplicate != null) return CompletableFuture.completedFuture(duplicate);
    try {
      var key = key(envelope.command().executionId());
      var actor = actor(envelope);
      var command = command(envelope, key, actor);
      producer.beginTransaction();
      if (envelope.command()
          instanceof com.forwardmeasure.openworkflow.engine.api.ExecutionCommand.Start start) {
        if (start.sourceDocument() == null) {
          throw new IllegalArgumentException(
              "Kafka execution requires immutable definition source");
        }
        var definitionKey = new WorkflowDefinitionKey(key.tenantId(), start.plan().coordinates());
        var bundle =
            new WorkflowDefinitionBundle(
                definitionKey,
                start.sourceDocument(),
                start.plan(),
                start.plan().compilerSha256(),
                envelope.commandId() + ":definition",
                actor,
                envelope.issuedAt());
        producer
            .send(
                new ProducerRecord<>(
                    topics.definitions(),
                    bundle.reference().canonical(),
                    bundles.serializer().serialize(topics.definitions(), bundle)))
            .get();
      }
      producer
          .send(
              new ProducerRecord<>(
                  topics.commands(),
                  key.canonical(),
                  commands.serializer().serialize(topics.commands(), command)))
          .get();
      producer.commitTransaction();
      long revision =
          revisions.compute(
              envelope.command().executionId(),
              (ignored, prior) -> prior == null ? 0L : prior + 1L);
      PublishedCommand published = new PublishedCommand(revision, Instant.now(clock));
      PublishedCommand raced = receipts.putIfAbsent(envelope.commandId(), published);
      return CompletableFuture.completedFuture(raced == null ? published : raced);
    } catch (Exception failure) {
      try {
        producer.abortTransaction();
      } catch (RuntimeException ignored) {
      }
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand command(
      ExecutionCommandEnvelope envelope, ExecutionKey key, ActorContext actor) {
    return switch (envelope.command()) {
      case com.forwardmeasure.openworkflow.engine.api.ExecutionCommand.Start start -> {
        var definition = new WorkflowDefinitionKey(key.tenantId(), start.plan().coordinates());
        yield new StartExecutionCommand(
            envelope.commandId().toString(),
            key,
            new com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference(
                definition, start.plan().sourceSha256(), start.plan().definitionSha256()),
            DataReferences.inline(start.input()),
            actor,
            envelope.issuedAt());
      }
      case com.forwardmeasure.openworkflow.engine.api.ExecutionCommand.Pause ignored ->
          new ControlExecutionCommand(
              envelope.commandId().toString(),
              key,
              ExecutionControlAction.PAUSE,
              actor,
              envelope.issuedAt());
      case com.forwardmeasure.openworkflow.engine.api.ExecutionCommand.Resume ignored ->
          new ControlExecutionCommand(
              envelope.commandId().toString(),
              key,
              ExecutionControlAction.RESUME,
              actor,
              envelope.issuedAt());
      case com.forwardmeasure.openworkflow.engine.api.ExecutionCommand.Cancel ignored ->
          new ControlExecutionCommand(
              envelope.commandId().toString(),
              key,
              ExecutionControlAction.CANCEL,
              actor,
              envelope.issuedAt());
    };
  }

  private static ExecutionKey key(ExecutionId id) {
    return new ExecutionKey(
        OksTenantId.parse("did:forwardmeasure:tenant:" + id.tenantId().value()),
        new WorkflowExecutionId(id.value().toString()));
  }

  static ActorContext actor(ExecutionCommandEnvelope envelope) {
    String value = envelope.context().actorId().value();
    if (!value.startsWith("did:")) value = "did:forwardmeasure:actor:" + value;
    return new ActorContext(
        OksTenantId.parse("did:forwardmeasure:tenant:" + envelope.context().tenantId().value()),
        com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId.parse(value),
        ActorType.HUMAN,
        null,
        null,
        new BusinessCorrelationId(envelope.correlationId()),
        envelope.context().organizationRoles(),
        null,
        envelope.issuedAt(),
        null,
        null,
        envelope.context().organizationId());
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
