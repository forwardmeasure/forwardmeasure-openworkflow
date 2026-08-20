package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

class OksPurgeProjectionTest {
  private static final String HISTORY_TOPIC = "history";
  private static final String EFFECT_TOPIC = "effects";
  private static final Instant NOW = Instant.parse("2026-07-31T20:00:00Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");
  private static final ExecutionKey KEY =
      new ExecutionKey(TENANT, new WorkflowExecutionId("run-purge"));
  private static final ActorContext ACTOR =
      new ActorContext(
          TENANT,
          ActorId.parse("did:web:tenant.example.test:actors:runtime"),
          ActorType.SYSTEM,
          "OKS Runtime",
          "oks-workflow-runtime",
          Set.of(),
          null,
          NOW);

  @Test
  void purgeCollapsesHistoryAndEffectProjectionsToMinimalReceipts() {
    Topology topology = topology();
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "purge-projection-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    try (TopologyTestDriver driver = new TopologyTestDriver(topology, properties)) {
      TestInputTopic<String, ExecutionHistoryEvent> history =
          driver.createInputTopic(
              HISTORY_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(ExecutionHistoryEvent.class).serializer());
      TestInputTopic<String, WorkflowEffect> effects =
          driver.createInputTopic(
              EFFECT_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(WorkflowEffect.class).serializer());

      history.pipeInput(KEY.canonical(), history(0, ExecutionEventType.EXECUTION_STARTED));
      history.pipeInput(KEY.canonical(), history(1, ExecutionEventType.EXECUTION_COMPLETED));
      effects.pipeInput(
          KEY.canonical(), effect("operation-1", WorkflowEffectType.DISPATCH_OPERATION));
      history.pipeInput(KEY.canonical(), history(2, ExecutionEventType.EXECUTION_PURGED));
      effects.pipeInput(
          KEY.canonical(),
          effect("purge-1:projections", WorkflowEffectType.PURGE_EXECUTION_PROJECTIONS));

      KeyValueStore<String, ExecutionHistoryEvent> historyStore =
          driver.getKeyValueStore(OksStores.HISTORY);
      assertNull(historyStore.get(OksQueryKeys.history(KEY, 0)));
      assertNull(historyStore.get(OksQueryKeys.history(KEY, 1)));
      assertEquals(
          ExecutionEventType.EXECUTION_PURGED,
          historyStore.get(OksQueryKeys.history(KEY, 2)).type());

      KeyValueStore<String, WorkflowEffect> effectStore =
          driver.getKeyValueStore(OksStores.EFFECTS);
      assertNull(effectStore.get(OksQueryKeys.effect(KEY, "operation-1")));
      assertEquals(
          WorkflowEffectType.PURGE_EXECUTION_PROJECTIONS,
          effectStore.get(OksQueryKeys.effect(KEY, "purge-1:projections")).type());
    }
  }

  private static Topology topology() {
    Topology topology = new Topology();
    var strings = Serdes.String();
    var histories = new JsonSerde<>(ExecutionHistoryEvent.class);
    var effects = new JsonSerde<>(WorkflowEffect.class);
    topology.addSource(
        "history-source", strings.deserializer(), histories.deserializer(), HISTORY_TOPIC);
    topology.addProcessor("history-projection", OksHistoryQueryProcessor::new, "history-source");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(OksStores.HISTORY), strings, histories),
        "history-projection");
    topology.addSource(
        "effect-source", strings.deserializer(), effects.deserializer(), EFFECT_TOPIC);
    topology.addProcessor("effect-projection", OksEffectQueryProcessor::new, "effect-source");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(OksStores.EFFECTS), strings, effects),
        "effect-projection");
    return topology;
  }

  private static ExecutionHistoryEvent history(long sequence, ExecutionEventType type) {
    return new ExecutionHistoryEvent(
        KEY.canonical() + ":" + sequence,
        KEY,
        sequence,
        type,
        "a".repeat(64),
        null,
        null,
        null,
        DataReferences.inline(JsonNodeFactory.instance.objectNode()),
        DataReferences.inline(JsonNodeFactory.instance.objectNode()),
        List.of(),
        List.of(),
        null,
        null,
        ACTOR,
        NOW.plusSeconds(sequence));
  }

  private static WorkflowEffect effect(String effectId, WorkflowEffectType type) {
    return new WorkflowEffect(
        effectId,
        KEY,
        type,
        "$purge",
        DataReferences.inline(JsonNodeFactory.instance.objectNode()),
        ACTOR,
        NOW);
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
