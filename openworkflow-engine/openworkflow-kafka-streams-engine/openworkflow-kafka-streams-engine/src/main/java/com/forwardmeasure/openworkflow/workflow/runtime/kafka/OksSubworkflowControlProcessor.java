package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Propagates a parent's pause, resume or cancellation onto the child execution one of its {@code
 * run: workflow:} steps is waiting on - the actual dispatch, not just "stop watching" the child.
 * Re-keys straight to the child's execution id, the same move {@link
 * OksSubscriptionEffectProcessor} makes for routed events; unlike the subworkflow launch this needs
 * no shared state store, so it forwards directly instead of round-tripping a topic.
 */
final class OksSubworkflowControlProcessor
    extends ContextualProcessor<String, WorkflowEffect, String, ExecutionCommand> {
  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null || effect.type() != WorkflowEffectType.CONTROL_SUBWORKFLOW) {
      return;
    }
    JsonNode control = effect.payload().inlineValue();
    ExecutionKey childKey = ExecutionKey.parse(control.required("childExecutionKey").textValue());
    ExecutionControlAction action =
        ExecutionControlAction.valueOf(control.required("action").textValue());
    ControlExecutionCommand command =
        new ControlExecutionCommand(
            effect.effectId(), childKey, action, effect.actor(), effect.requestedAt());
    context().forward(record.withKey(childKey.canonical()).withValue(command));
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
