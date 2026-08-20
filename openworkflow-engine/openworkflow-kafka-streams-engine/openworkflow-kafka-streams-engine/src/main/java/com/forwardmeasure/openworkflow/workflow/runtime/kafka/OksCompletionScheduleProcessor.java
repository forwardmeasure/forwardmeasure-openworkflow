package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import java.time.Duration;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Creates the next durable {@code schedule.after} trigger on completion. */
final class OksCompletionScheduleProcessor
    implements Processor<String, ExecutionHistoryEvent, String, WorkflowEffect> {
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private ProcessorContext<String, WorkflowEffect> context;
  private KeyValueStore<String, WorkflowDefinitionBundle> definitions;

  OksCompletionScheduleProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, WorkflowEffect> context) {
    this.context = context;
    definitions = context.getStateStore(OksStores.DEFINITIONS);
  }

  @Override
  public void process(Record<String, ExecutionHistoryEvent> record) {
    ExecutionHistoryEvent completed = record.value();
    if (completed == null || completed.type() != ExecutionEventType.EXECUTION_COMPLETED) {
      return;
    }
    WorkflowDefinitionBundle bundle = definition(completed);
    if (bundle == null
        || bundle.plan().schedule() == null
        || bundle.plan().schedule().after() == null) {
      return;
    }
    JsonNode input =
        completed.output() == null
            ? JsonNodeFactory.instance.objectNode()
            : completed.output().inlineValue();
    Duration after =
        OksScheduleSupport.duration(
            bundle.plan().schedule().after(),
            input,
            bundle.plan().expressions().mode(),
            completed.occurredAt());
    WorkflowEffect effect =
        OksScheduleSupport.timer(
            bundle,
            OksScheduleSupport.KIND_AFTER,
            completed.occurredAt().plus(after),
            null,
            input,
            Actors.systemCorrelated(
                bundle.key().tenantId(),
                runtimeActorId,
                runtimeComponent,
                completed.actor().correlationId(),
                completed.occurredAt()));
    context.forward(new Record<>(effect.key().canonical(), effect, record.timestamp()));
  }

  private WorkflowDefinitionBundle definition(ExecutionHistoryEvent completed) {
    try (var values = definitions.all()) {
      while (values.hasNext()) {
        WorkflowDefinitionBundle candidate = values.next().value;
        if (candidate.key().tenantId().equals(completed.key().tenantId())
            && candidate.plan().definitionSha256().equals(completed.definitionSha256())) {
          return candidate;
        }
      }
    }
    return null;
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
