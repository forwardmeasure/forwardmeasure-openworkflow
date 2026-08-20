package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidationException;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidator;
import com.forwardmeasure.openworkflow.definition.EventConsumptionPlan;
import com.forwardmeasure.openworkflow.definition.EventFilterPlan;
import com.forwardmeasure.openworkflow.definition.EventReadMode;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable {@code schedule.on} consumer.
 *
 * <p>All events for a tenant are keyed by that tenant DID, so correlated groups for that tenant
 * remain on one Kafka partition. The group and receipt stores are changelogged; a process
 * replacement therefore resumes ALL/ANY consumption without replaying already accepted CloudEvents.
 */
final class OksEventScheduleProcessor
    implements Processor<String, InboundCloudEvent, String, ExecutionCommand> {
  private static final Logger LOG = LoggerFactory.getLogger(OksEventScheduleProcessor.class);

  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();
  private final Map<String, DataSchemaValidator> schemaValidators = new LinkedHashMap<>();
  private ProcessorContext<String, ExecutionCommand> context;
  private KeyValueStore<String, WorkflowDefinitionBundle> definitions;
  private KeyValueStore<String, ScheduleEventState> states;
  private KeyValueStore<String, String> receipts;

  OksEventScheduleProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, ExecutionCommand> context) {
    this.context = context;
    definitions = context.getStateStore(OksStores.DEFINITIONS);
    states = context.getStateStore(OksStores.SCHEDULE_EVENT_STATES);
    receipts = context.getStateStore(OksStores.SCHEDULE_EVENT_RECEIPTS);
  }

  @Override
  public void process(Record<String, InboundCloudEvent> record) {
    InboundCloudEvent inbound = record.value();
    if (inbound == null) return;
    if (!inbound.tenantId().toString().equals(record.key())) {
      throw new IllegalArgumentException("Scheduled CloudEvents must be keyed by tenant DID");
    }
    if (receipts.get(inbound.eventKey()) != null) return;

    int eligibleDefinitions = 0;
    int acceptedDefinitions = 0;
    try (var values = definitions.all()) {
      while (values.hasNext()) {
        WorkflowDefinitionBundle bundle = values.next().value;
        if (!bundle.key().tenantId().equals(inbound.tenantId())
            || bundle.plan().schedule() == null
            || bundle.plan().schedule().on() == null) {
          continue;
        }
        eligibleDefinitions++;
        try {
          if (consume(record, bundle, inbound)) {
            acceptedDefinitions++;
          }
        } catch (DataSchemaValidationException invalid) {
          LOG.warn(
              "CloudEvent {} matched workflow {} but its data "
                  + "failed the pinned event schema: {}",
              inbound.event().inlineValue().required("id").textValue(),
              bundle.reference().canonical(),
              invalid.getMessage());
        }
      }
    }
    receipts.put(inbound.eventKey(), inbound.eventKey());
    LOG.info(
        "Observed scheduled CloudEvent {} of type {} for tenant {}; "
            + "eligible definitions={}, accepted definitions={}",
        inbound.event().inlineValue().required("id").textValue(),
        inbound.event().inlineValue().required("type").textValue(),
        inbound.tenantId(),
        eligibleDefinitions,
        acceptedDefinitions);
  }

  private boolean consume(
      Record<String, InboundCloudEvent> record,
      WorkflowDefinitionBundle bundle,
      InboundCloudEvent inbound) {
    String stateKey = bundle.reference().canonical();
    ScheduleEventState state = states.get(stateKey);
    if (state == null) state = ScheduleEventState.empty();
    List<ScheduleEventGroup> groups = new ArrayList<>(state.groups());
    boolean accepted = false;
    for (int index = 0; index < groups.size(); index++) {
      ConsumeUpdate update =
          consumeStrategy(
              bundle,
              bundle.plan().schedule().on(),
              bundle.plan().schedule().readAs(),
              "primary",
              groups.get(index),
              inbound.event().inlineValue());
      if (!update.accepted()) continue;
      accepted = true;
      if (update.complete()) {
        groups.remove(index);
        emitStart(record, bundle, inbound, update.group());
      } else {
        groups.set(index, update.group());
      }
      break;
    }
    if (!accepted) {
      ConsumeUpdate update =
          consumeStrategy(
              bundle,
              bundle.plan().schedule().on(),
              bundle.plan().schedule().readAs(),
              "primary",
              ScheduleEventGroup.empty(),
              inbound.event().inlineValue());
      if (update.accepted()) {
        accepted = true;
        if (update.complete()) {
          emitStart(record, bundle, inbound, update.group());
        } else {
          groups.add(update.group());
        }
      }
    }
    states.put(stateKey, new ScheduleEventState(groups));
    return accepted;
  }

  private ConsumeUpdate consumeStrategy(
      WorkflowDefinitionBundle bundle,
      EventConsumptionPlan strategy,
      EventReadMode readAs,
      String strategyPath,
      ScheduleEventGroup original,
      JsonNode envelope) {
    Map<String, Set<Integer>> progress = new LinkedHashMap<>(original.matchedStrategies());
    Map<String, JsonNode> correlations = new LinkedHashMap<>(original.correlations());
    Set<Integer> matched = new LinkedHashSet<>(progress.getOrDefault(strategyPath, Set.of()));
    boolean accepted =
        strategy.mode() == EventConsumptionPlan.Mode.ANY && strategy.filters().isEmpty();
    for (int index = 0; index < strategy.filters().size(); index++) {
      if (strategy.mode() == EventConsumptionPlan.Mode.ALL && matched.contains(index)) {
        continue;
      }
      Map<String, JsonNode> candidate =
          matches(bundle, strategy.filters().get(index), envelope, correlations);
      if (candidate == null) continue;
      correlations.clear();
      correlations.putAll(candidate);
      matched.add(index);
      accepted = true;
      if (strategy.mode() != EventConsumptionPlan.Mode.ALL) break;
    }
    progress.put(strategyPath, Set.copyOf(matched));

    boolean complete =
        switch (strategy.mode()) {
          case ONE -> accepted;
          case ALL -> matched.size() == strategy.filters().size();
          case ANY ->
              accepted && strategy.untilCondition() == null && strategy.untilConsumed() == null;
        };
    if (strategy.mode() == EventConsumptionPlan.Mode.ANY && strategy.untilConsumed() != null) {
      ConsumeUpdate until =
          consumeStrategy(
              bundle,
              strategy.untilConsumed(),
              readAs,
              strategyPath + "/until",
              new ScheduleEventGroup(progress, correlations, original.consumed()),
              envelope);
      progress.clear();
      progress.putAll(until.group().matchedStrategies());
      correlations.clear();
      correlations.putAll(until.group().correlations());
      complete = until.complete();
    }

    List<DataReference> consumed = new ArrayList<>(original.consumed());
    if (accepted) {
      consumed.add(DataReferences.inline(readEvent(readAs, envelope)));
    }
    if (accepted && strategy.untilCondition() != null) {
      ArrayNode values = JsonNodeFactory.instance.arrayNode();
      consumed.forEach(value -> values.add(value.inlineValue().deepCopy()));
      complete =
          expressions.evaluateCondition(
              OpenWorkflowCompiler.requiredExpression(strategy.untilCondition()),
              values,
              RuntimeExpressionArguments.empty(),
              bundle.plan().expressions().mode());
    }
    return new ConsumeUpdate(
        accepted, complete, new ScheduleEventGroup(progress, correlations, consumed));
  }

  private static JsonNode readEvent(EventReadMode readAs, JsonNode envelope) {
    return switch (readAs) {
      case DATA -> envelope.path("data").deepCopy();
      case ENVELOPE, RAW -> envelope.deepCopy();
    };
  }

  private Map<String, JsonNode> matches(
      WorkflowDefinitionBundle bundle,
      EventFilterPlan filter,
      JsonNode envelope,
      Map<String, JsonNode> existingCorrelations) {
    JsonNode eventData = envelope.path("data");
    var properties = filter.properties().properties().iterator();
    while (properties.hasNext()) {
      var property = properties.next();
      JsonNode actual =
          "data".equals(property.getKey()) ? eventData : envelope.path(property.getKey());
      JsonNode expected = property.getValue();
      if (expected.isTextual() && expected.textValue().trim().startsWith("${")) {
        if ("data".equals(property.getKey())) {
          if (!expressions.evaluateCondition(
              expected.textValue(),
              actual,
              RuntimeExpressionArguments.empty(),
              bundle.plan().expressions().mode())) {
            return null;
          }
        } else {
          expected =
              expressions.evaluateExpression(
                  expected.textValue(),
                  eventData,
                  RuntimeExpressionArguments.empty(),
                  bundle.plan().expressions().mode());
          if (!actual.equals(expected)) return null;
        }
      } else if (!actual.equals(expected)) {
        return null;
      }
    }

    schemaValidator(bundle).validate(filter.dataSchema(), envelope.path("data"));

    Map<String, JsonNode> correlations = new LinkedHashMap<>(existingCorrelations);
    for (var correlation : filter.correlations()) {
      JsonNode extracted =
          expressions.evaluateExpression(
              OpenWorkflowCompiler.requiredExpression(correlation.fromExpression()),
              eventData,
              RuntimeExpressionArguments.empty(),
              bundle.plan().expressions().mode());
      JsonNode expected;
      if (correlation.expected() == null) {
        expected = correlations.get(correlation.name());
        if (expected == null) {
          correlations.put(correlation.name(), extracted.deepCopy());
          continue;
        }
      } else if (correlation.expected().trim().startsWith("${")) {
        expected =
            expressions.evaluateExpression(
                correlation.expected(),
                eventData,
                RuntimeExpressionArguments.empty(),
                bundle.plan().expressions().mode());
      } else {
        expected = JsonNodeFactory.instance.textNode(correlation.expected());
      }
      if (!expected.equals(extracted)) return null;
    }
    return correlations;
  }

  private DataSchemaValidator schemaValidator(WorkflowDefinitionBundle bundle) {
    return schemaValidators.computeIfAbsent(
        bundle.plan().definitionSha256(),
        ignored -> new DataSchemaValidator(bundle.plan().resources()));
  }

  private void emitStart(
      Record<String, InboundCloudEvent> record,
      WorkflowDefinitionBundle bundle,
      InboundCloudEvent inbound,
      ScheduleEventGroup completed) {
    JsonNode input;
    if (completed.consumed().size() == 1) {
      input = completed.consumed().getFirst().inlineValue().deepCopy();
    } else {
      ArrayNode values = JsonNodeFactory.instance.arrayNode();
      completed.consumed().forEach(value -> values.add(value.inlineValue().deepCopy()));
      input = values;
    }
    StringBuilder identity = new StringBuilder();
    completed.consumed().forEach(value -> identity.append(value.inlineValue()).append('\n'));
    identity.append(inbound.eventKey());
    ExecutionCommand start =
        OksScheduleSupport.eventStart(
            bundle, identity.toString(), input, inbound.acceptedBy(), inbound.receivedAt());
    LOG.info(
        "Starting scheduled workflow {} as execution {} from " + "CloudEvent {}",
        bundle.reference().canonical(),
        start.key().executionId(),
        inbound.event().inlineValue().required("id").textValue());
    context.forward(record.withKey(start.key().canonical()).withValue(start));
  }

  private record ConsumeUpdate(boolean accepted, boolean complete, ScheduleEventGroup group) {}
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
