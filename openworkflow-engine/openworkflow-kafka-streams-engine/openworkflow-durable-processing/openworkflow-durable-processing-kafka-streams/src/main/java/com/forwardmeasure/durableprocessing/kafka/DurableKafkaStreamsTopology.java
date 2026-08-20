package com.forwardmeasure.durableprocessing.kafka;

import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

/** Reusable Kafka Streams topology for a deterministic keyed durable process. */
public final class DurableKafkaStreamsTopology<S, C, E, O> {
  private final String nodePrefix;
  private final DurableTopologyDescriptor stores;
  private final DurableTopics topics;
  private final Serde<S> stateSerde;
  private final Serde<C> commandSerde;
  private final Serde<E> eventSerde;
  private final Serde<O> outboxSerde;
  private final DurableCommandMetadata<C> metadata;
  private final DurableKafkaProcessFactory<S, C, E, O> processFactory;

  public DurableKafkaStreamsTopology(
      String nodePrefix,
      String storePrefix,
      DurableTopics topics,
      Serde<S> stateSerde,
      Serde<C> commandSerde,
      Serde<E> eventSerde,
      Serde<O> outboxSerde,
      DurableCommandMetadata<C> metadata,
      DurableKafkaProcessFactory<S, C, E, O> processFactory) {
    this.nodePrefix = requireText(nodePrefix, "nodePrefix");
    String storesPrefix = requireText(storePrefix, "storePrefix");
    this.stores =
        new DurableTopologyDescriptor(
            storesPrefix + "-state",
            storesPrefix + "-metadata",
            storesPrefix + "-command-receipts",
            storesPrefix + "-command-outcomes");
    this.topics = Objects.requireNonNull(topics, "topics");
    this.stateSerde = Objects.requireNonNull(stateSerde, "stateSerde");
    this.commandSerde = Objects.requireNonNull(commandSerde, "commandSerde");
    this.eventSerde = Objects.requireNonNull(eventSerde, "eventSerde");
    this.outboxSerde = Objects.requireNonNull(outboxSerde, "outboxSerde");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.processFactory = Objects.requireNonNull(processFactory, "processFactory");
  }

  public DurableTopologyDescriptor addTo(Topology topology) {
    Objects.requireNonNull(topology, "topology");
    String source = node("command-source");
    String processor = node("processor");
    String historyOutput = node("history-output");
    String commandOutput = node("command-output");
    String outboxOutput = node("outbox-output");
    String deadLetterOutput = node("dead-letter-output");
    String outcomeOutput = node("command-outcome-output");
    String historySink = node("history-sink");
    String commandSink = node("command-sink");
    String outboxSink = node("outbox-sink");
    String deadLetterSink = node("dead-letter-sink");
    String outcomeSink = node("command-outcome-sink");
    var strings = Serdes.String();
    var aggregateMetadata = new JacksonSerde<>(DurableAggregateMetadata.class);
    var deadLetters = new JacksonSerde<>(DurableDeadLetter.class);
    var outcomes =
        new JacksonSerde<>(com.forwardmeasure.durableprocessing.api.DurableCommandOutcome.class);

    topology.addSource(
        source, strings.deserializer(), commandSerde.deserializer(), topics.commands());
    topology.addProcessor(
        processor,
        () ->
            new DurableExecutionProcessor<>(
                metadata,
                processFactory,
                commandSerde.serializer(),
                topics.commands(),
                stores,
                historyOutput,
                commandOutput,
                outboxOutput,
                deadLetterOutput,
                outcomeOutput),
        source);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stores.stateStore()), strings, stateSerde),
        processor);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stores.commandOutcomeStore()), strings, outcomes),
        processor);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stores.metadataStore()), strings, aggregateMetadata),
        processor);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stores.commandReceiptStore()), strings, strings),
        processor);
    topology.addProcessor(historyOutput, DurableHistoryOutputProcessor<C, E, O>::new, processor);
    topology.addProcessor(commandOutput, DurableCommandOutputProcessor<C, E, O>::new, processor);
    topology.addProcessor(outboxOutput, DurableOutboxOutputProcessor<C, E, O>::new, processor);
    topology.addProcessor(
        deadLetterOutput, DurableDeadLetterOutputProcessor<C, E, O>::new, processor);
    topology.addProcessor(
        outcomeOutput, DurableCommandOutcomeOutputProcessor<C, E, O>::new, processor);
    topology.addSink(
        historySink,
        topics.history(),
        strings.serializer(),
        eventSerde.serializer(),
        historyOutput);
    topology.addSink(
        commandSink,
        topics.commands(),
        strings.serializer(),
        commandSerde.serializer(),
        commandOutput);
    topology.addSink(
        outboxSink, topics.outbox(), strings.serializer(), outboxSerde.serializer(), outboxOutput);
    topology.addSink(
        deadLetterSink,
        topics.deadLetters(),
        strings.serializer(),
        deadLetters.serializer(),
        deadLetterOutput);
    topology.addSink(
        outcomeSink,
        topics.commandOutcomes(),
        strings.serializer(),
        outcomes.serializer(),
        outcomeOutput);
    return stores;
  }

  public DurableTopologyDescriptor stores() {
    return stores;
  }

  private String node(String suffix) {
    return nodePrefix + "-" + suffix;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
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
