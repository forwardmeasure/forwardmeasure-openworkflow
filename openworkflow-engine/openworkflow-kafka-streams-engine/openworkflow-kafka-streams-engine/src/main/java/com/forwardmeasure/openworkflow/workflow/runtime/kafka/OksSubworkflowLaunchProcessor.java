package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferenceJson;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Starts the child execution for one {@code run: workflow:} step and, when the parent is awaiting
 * it, durably registers the parent's interest so {@link OksSubworkflowCompletionProcessor} can find
 * it again once the child reaches a terminal state.
 *
 * <p>The incoming record is already keyed by the child's own canonical execution key - {@link
 * OksSubworkflowLaunchOutputProcessor} rekeyed it - so the {@link OksStores#SUBWORKFLOW_WAITS}
 * write here lands in the same partition the child's own history will later be processed in.
 */
final class OksSubworkflowLaunchProcessor
    implements Processor<String, WorkflowEffect, String, ExecutionCommand> {
  private ProcessorContext<String, ExecutionCommand> context;
  private KeyValueStore<String, SubworkflowWait> waits;

  @Override
  public void init(ProcessorContext<String, ExecutionCommand> context) {
    this.context = context;
    waits = context.getStateStore(OksStores.SUBWORKFLOW_WAITS);
  }

  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null || effect.type() != WorkflowEffectType.START_SUBWORKFLOW) {
      return;
    }
    JsonNode descriptor = effect.payload().inlineValue();
    String operationId = descriptor.required("operationId").textValue();
    String parentExecutionKey = descriptor.required("parentExecutionKey").textValue();
    ExecutionKey childKey = ExecutionKey.parse(record.key());
    boolean awaitParent = descriptor.path("awaitParent").asBoolean(true);
    if (awaitParent) {
      waits.put(record.key(), new SubworkflowWait(parentExecutionKey, operationId));
    }

    WorkflowDefinitionReference childDefinition =
        new WorkflowDefinitionReference(
            new WorkflowDefinitionKey(
                childKey.tenantId(),
                new WorkflowCoordinates(
                    descriptor.required("childNamespace").textValue(),
                    descriptor.required("childName").textValue(),
                    descriptor.required("childVersion").textValue(),
                    descriptor.required("childDsl").textValue())),
            descriptor.required("childSourceSha256").textValue(),
            descriptor.required("childDefinitionSha256").textValue());
    DataReference childInput = DataReferenceJson.decode(descriptor.required("childInput"));
    StartExecutionCommand start =
        new StartExecutionCommand(
            "subworkflow-start:" + operationId,
            childKey,
            childDefinition,
            childInput,
            effect.actor(),
            effect.requestedAt());
    context.forward(record.withKey(childKey.canonical()).withValue(start));
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
