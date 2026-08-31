package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.durableprocessing.kafka.DurableKafkaStreamsTopology;
import com.forwardmeasure.durableprocessing.kafka.DurableTopics;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdmitWorkflowDefinitionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionCatalogueEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionSnapshot;
import com.forwardmeasure.openworkflow.workflow.runtime.core.OpenWorkflowCommandMetadata;
import com.forwardmeasure.openworkflow.workflow.runtime.core.WorkflowExecutionEngine;
import com.forwardmeasure.openworkflow.workflow.runtime.core.WorkflowRuntimeDataAccess;
import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * OpenWorkflow definition admission plus an OpenWorkflow state-machine adapter hosted by the
 * reusable durable-processing Kafka Streams topology.
 */
public final class OksTopology {
  private static final String DEFINITION_SOURCE = "oks-definition-command-source";
  private static final String DEFINITION_PROCESSOR = "oks-definition-admission-processor";
  static final String DEFINITION_HISTORY_OUTPUT = "oks-definition-history-output";
  static final String DEFINITION_BUNDLE_OUTPUT = "oks-definition-bundle-output";
  static final String DEFINITION_CATALOGUE_OUTPUT = "oks-definition-catalogue-output";
  private static final String DEFINITION_SCHEDULE_PROCESSOR = "oks-definition-schedule-processor";
  private static final String DEFINITION_SCHEDULE_SINK = "oks-definition-schedule-sink";
  private static final String DEFINITION_HISTORY_SINK = "oks-definition-history-sink";
  private static final String DEFINITION_HISTORY_QUERY = "oks-definition-history-query-processor";
  private static final String DEFINITION_BUNDLE_SINK = "oks-definition-bundle-sink";
  private static final String DEFINITION_CATALOGUE_SINK = "oks-definition-catalogue-sink";
  private static final String GLOBAL_DEFINITION_SOURCE = "oks-global-definition-source";
  private static final String GLOBAL_DEFINITION_PROCESSOR = "oks-global-definition-processor";
  private static final String EMITTED_EVENT_SOURCE = "oks-emitted-event-source";
  private static final String EMITTED_EVENT_OUTPUT = "oks-emitted-event-output";
  private static final String EMITTED_EVENT_SINK = "oks-emitted-event-sink";
  private static final String SUBSCRIPTION_EFFECT_PROCESSOR = "oks-subscription-effect-processor";
  private static final String SUBSCRIPTION_EFFECT_OUTPUT = "oks-subscription-effect-output";
  private static final String SUBSCRIPTION_EFFECT_SINK = "oks-subscription-effect-sink";
  private static final String SUBSCRIPTION_EFFECT_SOURCE = "oks-subscription-effect-source";
  private static final String INBOUND_EVENT_SOURCE = "oks-inbound-event-source";
  private static final String INBOUND_EVENT_PROCESSOR = "oks-inbound-event-processor";
  private static final String EVENT_SCHEDULE_PROCESSOR = "oks-event-schedule-processor";
  private static final String ROUTED_EVENT_COMMAND_SINK = "oks-routed-event-command-sink";
  private static final String TIMER_EFFECT_OUTPUT = "oks-timer-effect-output";
  private static final String TIMER_EFFECT_SINK = "oks-timer-effect-sink";
  private static final String TIMER_EFFECT_SOURCE = "oks-timer-effect-source";
  private static final String TIMER_EFFECT_PROCESSOR = "oks-timer-effect-processor";
  private static final String FIRED_TIMER_COMMAND_SINK = "oks-fired-timer-command-sink";
  private static final String HISTORY_QUERY_SOURCE = "oks-history-query-source";
  private static final String HISTORY_QUERY_PROCESSOR = "oks-history-query-processor";
  private static final String COMPLETION_SCHEDULE_PROCESSOR = "oks-completion-schedule-processor";
  private static final String COMPLETION_SCHEDULE_SINK = "oks-completion-schedule-sink";
  private static final String EFFECT_QUERY_PROCESSOR = "oks-effect-query-processor";
  private static final String SUBWORKFLOW_LAUNCH_OUTPUT = "oks-subworkflow-launch-output";
  private static final String SUBWORKFLOW_LAUNCH_SINK = "oks-subworkflow-launch-sink";
  private static final String SUBWORKFLOW_LAUNCH_SOURCE = "oks-subworkflow-launch-source";
  private static final String SUBWORKFLOW_LAUNCH_PROCESSOR = "oks-subworkflow-launch-processor";
  private static final String SUBWORKFLOW_CONTROL_PROCESSOR = "oks-subworkflow-control-processor";
  private static final String SUBWORKFLOW_COMPLETION_PROCESSOR =
      "oks-subworkflow-completion-processor";

  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private final Duration cancellationGracePeriod;
  private final boolean deferredComputationEnabled;

  public OksTopology(ActorId runtimeActorId, String runtimeComponent) {
    this(runtimeActorId, runtimeComponent, Duration.ofSeconds(30), false);
  }

  public OksTopology(
      ActorId runtimeActorId, String runtimeComponent, Duration cancellationGracePeriod) {
    this(runtimeActorId, runtimeComponent, cancellationGracePeriod, false);
  }

  public OksTopology(
      ActorId runtimeActorId,
      String runtimeComponent,
      Duration cancellationGracePeriod,
      boolean deferredComputationEnabled) {
    this.runtimeActorId = Objects.requireNonNull(runtimeActorId, "runtimeActorId");
    this.runtimeComponent = Objects.requireNonNull(runtimeComponent, "runtimeComponent");
    if (runtimeComponent.isBlank()) {
      throw new IllegalArgumentException("runtimeComponent must not be blank");
    }
    this.cancellationGracePeriod =
        Objects.requireNonNull(cancellationGracePeriod, "cancellationGracePeriod");
    if (cancellationGracePeriod.isZero() || cancellationGracePeriod.isNegative()) {
      throw new IllegalArgumentException("cancellationGracePeriod must be positive");
    }
    this.deferredComputationEnabled = deferredComputationEnabled;
  }

  public Topology build(OksTopics topics) {
    Objects.requireNonNull(topics, "topics");
    var strings = Serdes.String();
    var definitionCommands = new JsonSerde<>(AdmitWorkflowDefinitionCommand.class);
    var definitionBundles = new JsonSerde<>(WorkflowDefinitionBundle.class);
    var definitionHistory = new JsonSerde<>(WorkflowDefinitionAdmissionEvent.class);
    var definitionCatalogue = new JsonSerde<>(WorkflowDefinitionCatalogueEvent.class);
    var topology = new Topology();

    topology.addSource(
        DEFINITION_SOURCE,
        strings.deserializer(),
        definitionCommands.deserializer(),
        topics.definitionCommands());
    topology.addProcessor(
        DEFINITION_PROCESSOR, OksDefinitionAdmissionProcessor::new, DEFINITION_SOURCE);
    topology.addProcessor(
        DEFINITION_HISTORY_OUTPUT, OksDefinitionHistoryOutputProcessor::new, DEFINITION_PROCESSOR);
    topology.addProcessor(
        DEFINITION_BUNDLE_OUTPUT, OksDefinitionBundleOutputProcessor::new, DEFINITION_PROCESSOR);
    topology.addProcessor(
        DEFINITION_CATALOGUE_OUTPUT,
        OksDefinitionCatalogueOutputProcessor::new,
        DEFINITION_PROCESSOR);
    topology.addProcessor(
        DEFINITION_SCHEDULE_PROCESSOR,
        () -> new OksDefinitionScheduleProcessor(runtimeActorId, runtimeComponent),
        DEFINITION_BUNDLE_OUTPUT);
    topology.addSink(
        DEFINITION_HISTORY_SINK,
        topics.definitionHistory(),
        strings.serializer(),
        definitionHistory.serializer(),
        DEFINITION_HISTORY_OUTPUT);
    topology.addProcessor(
        DEFINITION_HISTORY_QUERY,
        OksDefinitionHistoryQueryProcessor::new,
        DEFINITION_HISTORY_OUTPUT);
    topology.addSink(
        DEFINITION_BUNDLE_SINK,
        topics.definitions(),
        strings.serializer(),
        definitionBundles.serializer(),
        DEFINITION_BUNDLE_OUTPUT);
    topology.addSink(
        DEFINITION_CATALOGUE_SINK,
        topics.definitionCatalogue(),
        strings.serializer(),
        definitionCatalogue.serializer(),
        DEFINITION_CATALOGUE_OUTPUT);
    topology.addSink(
        DEFINITION_SCHEDULE_SINK,
        topics.timerEffects(),
        strings.serializer(),
        new JsonSerde<>(WorkflowEffect.class).serializer(),
        DEFINITION_SCHEDULE_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.DEFINITION_ADMISSIONS),
            strings,
            definitionBundles),
        DEFINITION_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.DEFINITION_HISTORY),
            strings,
            definitionHistory),
        DEFINITION_HISTORY_QUERY);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.DEFINITION_COMMANDS), strings, strings),
        DEFINITION_PROCESSOR);
    topology.addGlobalStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.DEFINITIONS), strings, definitionBundles),
        GLOBAL_DEFINITION_SOURCE,
        strings.deserializer(),
        definitionBundles.deserializer(),
        topics.definitions(),
        GLOBAL_DEFINITION_PROCESSOR,
        OksGlobalDefinitionProcessor::new);

    var durableExecutions =
        new DurableKafkaStreamsTopology<
            ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>(
            "oks-execution",
            OksStores.EXECUTION_STORE_PREFIX,
            new DurableTopics(
                topics.commands(), topics.history(), topics.effects(), topics.deadLetters()),
            new JsonSerde<>(ExecutionSnapshot.class),
            new JsonSerde<>(ExecutionCommand.class),
            new JsonSerde<>(ExecutionHistoryEvent.class),
            new JsonSerde<>(WorkflowEffect.class),
            new OpenWorkflowCommandMetadata(),
            context -> {
              KeyValueStore<String, WorkflowDefinitionBundle> definitions =
                  context.getStateStore(OksStores.DEFINITIONS);
              return new WorkflowExecutionEngine(
                  reference -> definitions.get(reference.canonical()),
                  runtimeActorId,
                  runtimeComponent,
                  cancellationGracePeriod,
                  WorkflowRuntimeDataAccess.inlineOnly(),
                  deferredComputationEnabled);
            });
    durableExecutions.addTo(topology);
    var executionHistory = new JsonSerde<>(ExecutionHistoryEvent.class);
    topology.addSource(
        HISTORY_QUERY_SOURCE,
        strings.deserializer(),
        executionHistory.deserializer(),
        topics.history());
    topology.addProcessor(
        HISTORY_QUERY_PROCESSOR, OksHistoryQueryProcessor::new, HISTORY_QUERY_SOURCE);
    topology.addProcessor(
        COMPLETION_SCHEDULE_PROCESSOR,
        () -> new OksCompletionScheduleProcessor(runtimeActorId, runtimeComponent),
        HISTORY_QUERY_SOURCE);
    topology.addSink(
        COMPLETION_SCHEDULE_SINK,
        topics.timerEffects(),
        strings.serializer(),
        new JsonSerde<>(WorkflowEffect.class).serializer(),
        COMPLETION_SCHEDULE_PROCESSOR);
    topology.addProcessor(
        SUBWORKFLOW_COMPLETION_PROCESSOR,
        () -> new OksSubworkflowCompletionProcessor(runtimeActorId, runtimeComponent),
        HISTORY_QUERY_SOURCE);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.HISTORY), strings, executionHistory),
        HISTORY_QUERY_PROCESSOR);
    var workflowEffects = new JsonSerde<>(WorkflowEffect.class);
    topology.addSource(
        EMITTED_EVENT_SOURCE,
        strings.deserializer(),
        workflowEffects.deserializer(),
        topics.effects());
    topology.addProcessor(
        EMITTED_EVENT_OUTPUT, OksEmittedEventOutputProcessor::new, EMITTED_EVENT_SOURCE);
    topology.addProcessor(
        SUBSCRIPTION_EFFECT_OUTPUT,
        OksSubscriptionEffectOutputProcessor::new,
        EMITTED_EVENT_SOURCE);
    topology.addProcessor(
        TIMER_EFFECT_OUTPUT, OksTimerEffectOutputProcessor::new, EMITTED_EVENT_SOURCE);
    topology.addProcessor(
        EFFECT_QUERY_PROCESSOR, OksEffectQueryProcessor::new, EMITTED_EVENT_SOURCE);
    topology.addProcessor(
        SUBWORKFLOW_LAUNCH_OUTPUT, OksSubworkflowLaunchOutputProcessor::new, EMITTED_EVENT_SOURCE);
    topology.addProcessor(
        SUBWORKFLOW_CONTROL_PROCESSOR, OksSubworkflowControlProcessor::new, EMITTED_EVENT_SOURCE);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.EFFECTS), strings, workflowEffects),
        EFFECT_QUERY_PROCESSOR);
    topology.addSink(
        SUBSCRIPTION_EFFECT_SINK,
        topics.subscriptionEffects(),
        strings.serializer(),
        workflowEffects.serializer(),
        SUBSCRIPTION_EFFECT_OUTPUT);
    topology.addSink(
        SUBWORKFLOW_LAUNCH_SINK,
        topics.subworkflowEffects(),
        strings.serializer(),
        workflowEffects.serializer(),
        SUBWORKFLOW_LAUNCH_OUTPUT);
    topology.addSink(
        TIMER_EFFECT_SINK,
        topics.timerEffects(),
        strings.serializer(),
        workflowEffects.serializer(),
        TIMER_EFFECT_OUTPUT);
    topology.addSource(
        SUBSCRIPTION_EFFECT_SOURCE,
        strings.deserializer(),
        workflowEffects.deserializer(),
        topics.subscriptionEffects());
    topology.addProcessor(
        SUBSCRIPTION_EFFECT_PROCESSOR,
        () -> new OksSubscriptionEffectProcessor(runtimeActorId, runtimeComponent),
        SUBSCRIPTION_EFFECT_SOURCE);
    topology.addSource(
        SUBWORKFLOW_LAUNCH_SOURCE,
        strings.deserializer(),
        workflowEffects.deserializer(),
        topics.subworkflowEffects());
    topology.addProcessor(
        SUBWORKFLOW_LAUNCH_PROCESSOR,
        OksSubworkflowLaunchProcessor::new,
        SUBWORKFLOW_LAUNCH_SOURCE);
    var inboundEvents = new JsonSerde<>(InboundCloudEvent.class);
    topology.addSource(
        INBOUND_EVENT_SOURCE,
        strings.deserializer(),
        inboundEvents.deserializer(),
        topics.inboundEvents());
    topology.addSource(
        TIMER_EFFECT_SOURCE,
        strings.deserializer(),
        workflowEffects.deserializer(),
        topics.timerEffects());
    topology.addProcessor(
        TIMER_EFFECT_PROCESSOR,
        () -> new OksTimerEffectProcessor(runtimeActorId, runtimeComponent),
        TIMER_EFFECT_SOURCE);
    topology.addProcessor(
        INBOUND_EVENT_PROCESSOR,
        () -> new OksInboundEventProcessor(runtimeActorId, runtimeComponent),
        INBOUND_EVENT_SOURCE);
    topology.addProcessor(
        EVENT_SCHEDULE_PROCESSOR,
        () -> new OksEventScheduleProcessor(runtimeActorId, runtimeComponent),
        INBOUND_EVENT_SOURCE);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.EVENT_SUBSCRIPTIONS),
            strings,
            workflowEffects),
        SUBSCRIPTION_EFFECT_PROCESSOR,
        INBOUND_EVENT_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.SUBWORKFLOW_WAITS),
            strings,
            new JsonSerde<>(SubworkflowWait.class)),
        SUBWORKFLOW_LAUNCH_PROCESSOR,
        SUBWORKFLOW_COMPLETION_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.TIMERS), strings, workflowEffects),
        TIMER_EFFECT_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.INBOUND_EVENTS), strings, inboundEvents),
        SUBSCRIPTION_EFFECT_PROCESSOR,
        INBOUND_EVENT_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.SCHEDULE_EVENT_STATES),
            strings,
            new JsonSerde<>(ScheduleEventState.class)),
        EVENT_SCHEDULE_PROCESSOR);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OksStores.SCHEDULE_EVENT_RECEIPTS), strings, strings),
        EVENT_SCHEDULE_PROCESSOR);
    topology.addSink(
        EMITTED_EVENT_SINK,
        topics.emittedEvents(),
        strings.serializer(),
        new JsonSerde<>(JsonNode.class).serializer(),
        EMITTED_EVENT_OUTPUT);
    topology.addSink(
        ROUTED_EVENT_COMMAND_SINK,
        topics.commands(),
        strings.serializer(),
        new JsonSerde<>(ExecutionCommand.class).serializer(),
        SUBSCRIPTION_EFFECT_PROCESSOR,
        INBOUND_EVENT_PROCESSOR,
        EVENT_SCHEDULE_PROCESSOR,
        SUBWORKFLOW_LAUNCH_PROCESSOR,
        SUBWORKFLOW_COMPLETION_PROCESSOR,
        SUBWORKFLOW_CONTROL_PROCESSOR);
    topology.addSink(
        FIRED_TIMER_COMMAND_SINK,
        topics.commands(),
        strings.serializer(),
        new JsonSerde<>(ExecutionCommand.class).serializer(),
        TIMER_EFFECT_PROCESSOR);
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
