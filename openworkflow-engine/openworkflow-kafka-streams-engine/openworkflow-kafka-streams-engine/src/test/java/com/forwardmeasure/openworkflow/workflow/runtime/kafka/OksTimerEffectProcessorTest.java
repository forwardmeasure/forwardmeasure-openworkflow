package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.FireTimerCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

class OksTimerEffectProcessorTest {
  private static final String EFFECTS = "timer-effects";
  private static final String COMMANDS = "commands";
  private static final Instant NOW = Instant.parse("2026-08-01T21:00:00Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");
  private static final ExecutionKey KEY =
      new ExecutionKey(TENANT, new WorkflowExecutionId("run-timer-once"));
  private static final ActorId RUNTIME =
      ActorId.parse("did:web:openworkflow-kafka-streams.test:actors:runtime");
  private static final ActorContext ACTOR =
      new ActorContext(
          TENANT,
          ActorId.parse("did:web:tenant.example.test:actors:owner"),
          ActorType.HUMAN,
          "Workflow Owner",
          "test-client",
          Set.of(),
          null,
          NOW);

  @Test
  void overdueOneShotTimerFiresExactlyOnceAcrossRepeatedPunctuations() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "timer-effect-once-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    try (TopologyTestDriver driver = new TopologyTestDriver(topology(), properties, NOW)) {
      TestInputTopic<String, WorkflowEffect> effects =
          driver.createInputTopic(
              EFFECTS,
              Serdes.String().serializer(),
              new JsonSerde<>(WorkflowEffect.class).serializer());
      TestOutputTopic<String, ExecutionCommand> commands =
          driver.createOutputTopic(
              COMMANDS,
              Serdes.String().deserializer(),
              new JsonSerde<>(ExecutionCommand.class).deserializer());

      String timerId = KEY.canonical() + ":timer:cancellation";
      effects.pipeInput(timerId, schedule(timerId));

      driver.advanceWallClockTime(Duration.ofMillis(100));
      assertEquals(1, commands.getQueueSize());
      ExecutionCommand fired = commands.readValue();
      assertTrue(fired instanceof FireTimerCommand);
      assertEquals("timer:" + timerId, fired.commandId());

      driver.advanceWallClockTime(Duration.ofSeconds(2));
      assertTrue(commands.isEmpty(), "A fired one-shot timer must not be emitted again");
    }
  }

  private static Topology topology() {
    Topology topology = new Topology();
    var strings = Serdes.String();
    var effects = new JsonSerde<>(WorkflowEffect.class);
    var commands = new JsonSerde<>(ExecutionCommand.class);
    topology.addSource("timer-source", strings.deserializer(), effects.deserializer(), EFFECTS);
    topology.addProcessor(
        "timer-processor",
        () -> new OksTimerEffectProcessor(RUNTIME, "oks-workflow-runtime"),
        "timer-source");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(OksStores.TIMERS), strings, effects),
        "timer-processor");
    topology.addSink(
        "command-sink", COMMANDS, strings.serializer(), commands.serializer(), "timer-processor");
    return topology;
  }

  private static WorkflowEffect schedule(String timerId) {
    var payload = JsonNodeFactory.instance.objectNode();
    payload.put("timerId", timerId);
    payload.put("purpose", "cancellation-deadline");
    payload.put("taskPath", "/");
    payload.put("dueAt", NOW.plusMillis(50).toString());
    return new WorkflowEffect(
        timerId + ":schedule",
        KEY,
        WorkflowEffectType.SCHEDULE_TIMER,
        "/",
        DataReferences.inline(payload),
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
