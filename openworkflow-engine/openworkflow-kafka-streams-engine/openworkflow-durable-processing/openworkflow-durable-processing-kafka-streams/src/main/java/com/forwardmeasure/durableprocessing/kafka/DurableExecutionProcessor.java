package com.forwardmeasure.durableprocessing.kafka;

import com.forwardmeasure.durableprocessing.api.DurableAggregate;
import com.forwardmeasure.durableprocessing.api.DurableCommandKey;
import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import com.forwardmeasure.durableprocessing.api.DurableCommandOutcome;
import com.forwardmeasure.durableprocessing.api.DurableCommandOutcomeStatus;
import com.forwardmeasure.durableprocessing.core.CommandIdentityConflictException;
import com.forwardmeasure.durableprocessing.core.DurableProcessingKernel;
import com.forwardmeasure.durableprocessing.core.FutureRevisionException;
import java.time.Instant;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

final class DurableExecutionProcessor<S, C, E, O>
    implements Processor<String, C, String, DurableProcessorOutput<C, E, O>> {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DurableExecutionProcessor.class);

  private final DurableCommandMetadata<C> metadata;
  private final DurableKafkaProcessFactory<S, C, E, O> processFactory;
  private final Serializer<C> commandSerializer;
  private final String commandTopic;
  private final String stateStoreName;
  private final String metadataStoreName;
  private final String receiptStoreName;
  private final String outcomeStoreName;
  private final String historyOutput;
  private final String commandOutput;
  private final String outboxOutput;
  private final String deadLetterOutput;
  private final String outcomeOutput;

  private ProcessorContext<String, DurableProcessorOutput<C, E, O>> context;
  private DurableProcessingKernel<S, C, E, O> kernel;
  private KeyValueStore<String, S> states;
  private KeyValueStore<String, DurableAggregateMetadata> aggregateMetadata;
  private KeyValueStore<String, String> receipts;
  private KeyValueStore<String, DurableCommandOutcome> outcomes;

  DurableExecutionProcessor(
      DurableCommandMetadata<C> metadata,
      DurableKafkaProcessFactory<S, C, E, O> processFactory,
      Serializer<C> commandSerializer,
      String commandTopic,
      DurableTopologyDescriptor stores,
      String historyOutput,
      String commandOutput,
      String outboxOutput,
      String deadLetterOutput,
      String outcomeOutput) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.processFactory = Objects.requireNonNull(processFactory, "processFactory");
    this.commandSerializer = Objects.requireNonNull(commandSerializer, "commandSerializer");
    this.commandTopic = Objects.requireNonNull(commandTopic, "commandTopic");
    this.stateStoreName = stores.stateStore();
    this.metadataStoreName = stores.metadataStore();
    this.receiptStoreName = stores.commandReceiptStore();
    this.outcomeStoreName = stores.commandOutcomeStore();
    this.historyOutput = historyOutput;
    this.commandOutput = commandOutput;
    this.outboxOutput = outboxOutput;
    this.deadLetterOutput = deadLetterOutput;
    this.outcomeOutput = outcomeOutput;
  }

  @Override
  public void init(ProcessorContext<String, DurableProcessorOutput<C, E, O>> context) {
    this.context = Objects.requireNonNull(context, "context");
    this.states = context.getStateStore(stateStoreName);
    this.aggregateMetadata = context.getStateStore(metadataStoreName);
    this.receipts = context.getStateStore(receiptStoreName);
    this.outcomes = context.getStateStore(outcomeStoreName);
    this.kernel = new DurableProcessingKernel<>(metadata, processFactory.create(context));
  }

  @Override
  public void process(Record<String, C> record) {
    if (record.value() == null) return;
    C command = record.value();
    String aggregateKey = metadata.aggregateKey(command);
    String commandId = metadata.commandId(command);
    String fingerprint =
        CommandFingerprints.sha256(commandSerializer.serialize(commandTopic, command));
    String receiptKey = DurableCommandKey.canonical(aggregateKey, commandId);
    String priorFingerprint = receipts.get(receiptKey);
    DurableAggregate<S> current = current(record.key());

    try {
      var decision = kernel.decide(record.key(), current, command, fingerprint, priorFingerprint);
      if (decision.receiptFingerprint() != null) {
        receipts.put(receiptKey, decision.receiptFingerprint());
      }
      var outcome =
          new DurableCommandOutcome(
              aggregateKey,
              commandId,
              metadata.commandType(command),
              DurableCommandOutcomeStatus.valueOf(decision.disposition().name()),
              outcomeRevision(current, decision),
              metadata.requestedAt(command),
              Instant.ofEpochMilli(record.timestamp()),
              null,
              null);
      if (decision.stateRemoved()) {
        states.delete(record.key());
        aggregateMetadata.delete(record.key());
        deletePriorReceipts(aggregateKey, receiptKey);
        deletePriorOutcomes(aggregateKey, receiptKey);
      } else if (decision.stateChanged()) {
        DurableAggregate<S> aggregate = decision.aggregate();
        states.put(record.key(), aggregate.state());
        aggregateMetadata.put(
            record.key(),
            new DurableAggregateMetadata(
                aggregate.aggregateKey(),
                aggregate.revision(),
                aggregate.startedAt(),
                aggregate.updatedAt()));
      }
      for (E event : decision.events()) {
        context.forward(
            record.withValue(new DurableProcessorOutput.History<C, E, O>(event)), historyOutput);
      }
      for (C followUp : decision.followUpCommands()) {
        context.forward(
            record.withValue(new DurableProcessorOutput.Command<C, E, O>(followUp)), commandOutput);
      }
      for (O output : decision.outbox()) {
        context.forward(
            record.withValue(new DurableProcessorOutput.Outbox<C, E, O>(output)), outboxOutput);
      }
      outcomes.put(receiptKey, outcome);
      context.forward(
          record
              .withKey(receiptKey)
              .withValue(new DurableProcessorOutput.Outcome<C, E, O>(outcome)),
          outcomeOutput);
    } catch (CommandIdentityConflictException
        | FutureRevisionException
        | IllegalArgumentException
        | SecurityException rejected) {
      if (!(rejected instanceof FutureRevisionException)
          && priorFingerprint == null
          && metadata.deduplicate(command)) {
        receipts.put(receiptKey, fingerprint);
      }
      String message =
          rejected.getMessage() == null ? rejected.getClass().getName() : rejected.getMessage();
      LOGGER.warn(
          "Rejected durable command: aggregateKey={}, commandId={}, rejectionType={}, reason={}",
          aggregateKey,
          commandId,
          rejected.getClass().getName(),
          message);
      var deadLetter =
          new DurableDeadLetter(
              aggregateKey + ":" + commandId + ":" + fingerprint,
              aggregateKey,
              commandId,
              fingerprint,
              rejected.getClass().getName(),
              message,
              metadata.requestedAt(command));
      context.forward(
          record.withValue(new DurableProcessorOutput.Rejected<C, E, O>(deadLetter)),
          deadLetterOutput);
      var outcome =
          new DurableCommandOutcome(
              aggregateKey,
              commandId,
              metadata.commandType(command),
              DurableCommandOutcomeStatus.REJECTED,
              current == null ? 0 : current.revision(),
              metadata.requestedAt(command),
              Instant.ofEpochMilli(record.timestamp()),
              rejected.getClass().getName(),
              message);
      outcomes.put(receiptKey, outcome);
      context.forward(
          record
              .withKey(receiptKey)
              .withValue(new DurableProcessorOutput.Outcome<C, E, O>(outcome)),
          outcomeOutput);
    }
  }

  private static <S, C, E, O> long outcomeRevision(
      DurableAggregate<S> current,
      com.forwardmeasure.durableprocessing.api.DurableDecision<S, C, E, O> decision) {
    if (decision.aggregate() != null) return decision.aggregate().revision();
    if (current == null) return 0;
    return decision.stateRemoved() ? Math.addExact(current.revision(), 1) : current.revision();
  }

  private DurableAggregate<S> current(String recordKey) {
    S state = states.get(recordKey);
    DurableAggregateMetadata stored = aggregateMetadata.get(recordKey);
    if (state == null && stored == null) {
      return null;
    }
    if (state == null || stored == null) {
      throw new IllegalStateException("Durable state and metadata stores are inconsistent");
    }
    return new DurableAggregate<>(
        stored.aggregateKey(), stored.revision(), state, stored.startedAt(), stored.updatedAt());
  }

  private void deletePriorReceipts(String aggregateKey, String retainedReceiptKey) {
    String prefix = DurableCommandKey.aggregatePrefix(aggregateKey);
    java.util.ArrayList<String> removed = new java.util.ArrayList<>();
    try (org.apache.kafka.streams.state.KeyValueIterator<String, String> values =
        receipts.range(prefix, prefix + '\uffff')) {
      while (values.hasNext()) {
        String key = values.next().key;
        if (!retainedReceiptKey.equals(key)) removed.add(key);
      }
    }
    removed.forEach(receipts::delete);
  }

  private void deletePriorOutcomes(String aggregateKey, String retainedReceiptKey) {
    String prefix = DurableCommandKey.aggregatePrefix(aggregateKey);
    java.util.ArrayList<String> removed = new java.util.ArrayList<>();
    try (org.apache.kafka.streams.state.KeyValueIterator<String, DurableCommandOutcome> values =
        outcomes.range(prefix, prefix + '\uffff')) {
      while (values.hasNext()) {
        String key = values.next().key;
        if (!retainedReceiptKey.equals(key)) removed.add(key);
      }
    }
    removed.forEach(outcomes::delete);
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
