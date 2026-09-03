package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.data.DataReferenceJson;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
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

/**
 * Exercises the real production processors that route a subworkflow launch and its eventual
 * completion - not a simulation of them, the actual classes wired the way {@link OksTopology} wires
 * them, over a small standalone topology in the same style as {@link OksTimerEffectProcessorTest}.
 */
class OksSubworkflowRoutingProcessorsTest {
  private static final String EFFECTS = "effects";
  private static final String SUBWORKFLOW_EFFECTS = "subworkflow-effects";
  private static final String HISTORY = "history";
  private static final String COMMANDS = "commands";
  private static final Instant NOW = Instant.parse("2026-08-01T21:00:00Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");
  private static final ExecutionKey PARENT_KEY =
      new ExecutionKey(TENANT, new WorkflowExecutionId("subworkflow-parent"));
  private static final ExecutionKey CHILD_KEY =
      new ExecutionKey(TENANT, new WorkflowExecutionId("subworkflow-child"));
  private static final String OPERATION_ID = CHILD_KEY.canonical();
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
  void launchesTheChildAndResumesTheParentOnceItCompletes() {
    try (TopologyTestDriver driver = driver()) {
      TestInputTopic<String, WorkflowEffect> effects =
          driver.createInputTopic(
              EFFECTS,
              Serdes.String().serializer(),
              new JsonSerde<>(WorkflowEffect.class).serializer());
      TestInputTopic<String, ExecutionHistoryEvent> history =
          driver.createInputTopic(
              HISTORY,
              Serdes.String().serializer(),
              new JsonSerde<>(ExecutionHistoryEvent.class).serializer());
      TestOutputTopic<String, ExecutionCommand> commands =
          driver.createOutputTopic(
              COMMANDS,
              Serdes.String().deserializer(),
              new JsonSerde<>(ExecutionCommand.class).deserializer());

      effects.pipeInput(PARENT_KEY.canonical(), launchEffect());
      assertEquals(1, commands.getQueueSize());
      ExecutionCommand started = commands.readValue();
      StartExecutionCommand start = assertInstanceOf(StartExecutionCommand.class, started);
      assertEquals(CHILD_KEY, start.key());
      assertEquals(
          new WorkflowDefinitionReference(
              new WorkflowDefinitionKey(
                  TENANT, new WorkflowCoordinates("evidence", "child", "2.0.0", "1.0.3")),
              "a".repeat(64),
              "b".repeat(64)),
          start.definition());
      assertEquals("seed-value", start.input().inlineValue().required("seed").textValue());
      assertTrue(commands.isEmpty(), "The launch must not also resume the parent yet");

      var completedOutput =
          DataReferences.inline(
              JsonNodeFactory.instance.objectNode().put("childSaw", "seed-value"));
      history.pipeInput(
          CHILD_KEY.canonical(),
          new ExecutionHistoryEvent(
              CHILD_KEY.canonical() + ":1",
              CHILD_KEY,
              1,
              ExecutionEventType.EXECUTION_COMPLETED,
              "c".repeat(64),
              null,
              null,
              null,
              completedOutput,
              completedOutput,
              ACTOR,
              NOW.plusSeconds(1)));
      assertEquals(1, commands.getQueueSize());
      ExecutionCommand resumed = commands.readValue();
      ObserveOperationCommand observe = assertInstanceOf(ObserveOperationCommand.class, resumed);
      assertEquals(PARENT_KEY, observe.key());
      assertEquals(OPERATION_ID, observe.operationId());
      assertEquals(OperationObservationStatus.SUCCEEDED, observe.observation().status());
      assertEquals(
          "seed-value",
          observe.observation().output().inlineValue().required("childSaw").textValue());

      // A redelivered terminal history record must not resume the parent a second time - the
      // wait was already consumed.
      history.pipeInput(
          CHILD_KEY.canonical(),
          new ExecutionHistoryEvent(
              CHILD_KEY.canonical() + ":1",
              CHILD_KEY,
              1,
              ExecutionEventType.EXECUTION_COMPLETED,
              "c".repeat(64),
              null,
              null,
              null,
              completedOutput,
              completedOutput,
              ACTOR,
              NOW.plusSeconds(1)));
      assertTrue(commands.isEmpty(), "A replayed completion must be a safe no-op");
    }
  }

  @Test
  void propagatesParentPauseAsAControlCommandKeyedToTheChild() {
    try (TopologyTestDriver driver = driver()) {
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

      ObjectNode control = JsonNodeFactory.instance.objectNode();
      control.put("operationId", OPERATION_ID);
      control.put("childExecutionKey", CHILD_KEY.canonical());
      control.put("action", "PAUSE");
      var controlEffect =
          new WorkflowEffect(
              OPERATION_ID + ":pause",
              PARENT_KEY,
              WorkflowEffectType.CONTROL_SUBWORKFLOW,
              "/invoke",
              DataReferences.inline(control),
              ACTOR,
              NOW);
      effects.pipeInput(PARENT_KEY.canonical(), controlEffect);

      assertEquals(1, commands.getQueueSize());
      ControlExecutionCommand command =
          assertInstanceOf(ControlExecutionCommand.class, commands.readValue());
      assertEquals(CHILD_KEY, command.key());
      assertEquals(ExecutionControlAction.PAUSE, command.action());
    }
  }

  private static WorkflowEffect launchEffect() {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", OPERATION_ID);
    descriptor.put("operationKind", "run-workflow");
    descriptor.put("executionKey", PARENT_KEY.canonical());
    descriptor.put("taskPath", "/invoke");
    descriptor.put("parentExecutionKey", PARENT_KEY.canonical());
    descriptor.put("childExecutionKey", CHILD_KEY.canonical());
    descriptor.put("childNamespace", "evidence");
    descriptor.put("childName", "child");
    descriptor.put("childVersion", "2.0.0");
    descriptor.put("childDsl", "1.0.3");
    descriptor.put("childSourceSha256", "a".repeat(64));
    descriptor.put("childDefinitionSha256", "b".repeat(64));
    descriptor.put("awaitParent", true);
    descriptor.set(
        "childInput",
        DataReferenceJson.encode(
            DataReferences.inline(
                JsonNodeFactory.instance.objectNode().put("seed", "seed-value"))));
    return new WorkflowEffect(
        OPERATION_ID + ":dispatch",
        PARENT_KEY,
        WorkflowEffectType.START_SUBWORKFLOW,
        "/invoke",
        DataReferences.inline(descriptor),
        ACTOR,
        NOW);
  }

  private static TopologyTestDriver driver() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "subworkflow-routing-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    return new TopologyTestDriver(topology(), properties, NOW);
  }

  private static Topology topology() {
    Topology topology = new Topology();
    var strings = Serdes.String();
    var effects = new JsonSerde<>(WorkflowEffect.class);
    var history = new JsonSerde<>(ExecutionHistoryEvent.class);
    var commands = new JsonSerde<>(ExecutionCommand.class);
    var waits = new JsonSerde<>(SubworkflowWait.class);

    topology.addSource("effects-source", strings.deserializer(), effects.deserializer(), EFFECTS);
    topology.addProcessor(
        "launch-output", OksSubworkflowLaunchOutputProcessor::new, "effects-source");
    topology.addProcessor(
        "control-processor", OksSubworkflowControlProcessor::new, "effects-source");
    topology.addSink(
        "subworkflow-effects-sink",
        SUBWORKFLOW_EFFECTS,
        strings.serializer(),
        effects.serializer(),
        "launch-output");
    topology.addSource(
        "subworkflow-effects-source",
        strings.deserializer(),
        effects.deserializer(),
        SUBWORKFLOW_EFFECTS);
    topology.addProcessor(
        "launch-processor", OksSubworkflowLaunchProcessor::new, "subworkflow-effects-source");
    topology.addSource("history-source", strings.deserializer(), history.deserializer(), HISTORY);
    topology.addProcessor(
        "completion-processor",
        () -> new OksSubworkflowCompletionProcessor(RUNTIME, "oks-workflow-runtime"),
        "history-source");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(OksStores.SUBWORKFLOW_WAITS), strings, waits),
        "launch-processor",
        "completion-processor");
    topology.addSink(
        "command-sink",
        COMMANDS,
        strings.serializer(),
        commands.serializer(),
        "launch-processor",
        "completion-processor",
        "control-processor");
    return topology;
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
