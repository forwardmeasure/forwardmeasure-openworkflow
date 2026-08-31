package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Watches every execution's own history for the terminal events of a child that some parent started
 * with a {@code run: workflow:} step, and resumes that parent when its child finishes.
 *
 * <p>Same shape as {@link OksCompletionScheduleProcessor} - both watch {@code topics.history()} for
 * {@code EXECUTION_COMPLETED} - but that processor looks up a bundle keyed off the completed
 * execution's own definition and reschedules a follow-on run of that same definition. This one
 * looks up a specific execution id in {@link OksStores#SUBWORKFLOW_WAITS} and, when a parent is
 * found waiting on it, forwards a resume command re-keyed to that PARENT's execution id, not the
 * completed execution's own id. That parent/child re-key is the genuinely new piece.
 */
final class OksSubworkflowCompletionProcessor
    implements Processor<String, ExecutionHistoryEvent, String, ExecutionCommand> {
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private ProcessorContext<String, ExecutionCommand> context;
  private KeyValueStore<String, SubworkflowWait> waits;

  OksSubworkflowCompletionProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, ExecutionCommand> context) {
    this.context = context;
    waits = context.getStateStore(OksStores.SUBWORKFLOW_WAITS);
  }

  @Override
  public void process(Record<String, ExecutionHistoryEvent> record) {
    ExecutionHistoryEvent event = record.value();
    if (event == null) {
      return;
    }
    OperationObservation observation = terminalObservation(event);
    if (observation == null) {
      return;
    }
    SubworkflowWait wait = waits.get(record.key());
    if (wait == null) {
      return;
    }
    // At-least-once redelivery of the same terminal history record must not resume the parent
    // twice; once the wait is consumed here a replay finds nothing and safely no-ops.
    waits.delete(record.key());

    ExecutionKey parentKey = ExecutionKey.parse(wait.parentExecutionKey());
    var actor =
        Actors.systemCorrelated(
            parentKey.tenantId(),
            runtimeActorId,
            runtimeComponent,
            event.actor().correlationId(),
            event.occurredAt());
    ObserveOperationCommand resume =
        new ObserveOperationCommand(
            "subworkflow-complete:" + wait.operationId(),
            parentKey,
            wait.operationId(),
            observation,
            actor,
            event.occurredAt());
    context.forward(new Record<>(parentKey.canonical(), resume, record.timestamp()));
  }

  private static OperationObservation terminalObservation(ExecutionHistoryEvent event) {
    return switch (event.type()) {
      case EXECUTION_COMPLETED ->
          new OperationObservation(
              OperationObservationStatus.SUCCEEDED, event.output(), null, null);
      case EXECUTION_CANCELLED ->
          new OperationObservation(
              OperationObservationStatus.CANCELLED, null, cancelledError(event), null);
      case EXECUTION_FAILED ->
          new OperationObservation(
              OperationObservationStatus.FAILED, null, failedError(event.failure()), null);
      default -> null;
    };
  }

  private static WorkflowError cancelledError(ExecutionHistoryEvent event) {
    return new WorkflowError(
        "https://forwardmeasure.com/oks/errors/subworkflow/cancelled",
        499,
        event.key().canonical(),
        "Subworkflow cancelled",
        "Child workflow execution " + event.key().canonical() + " was cancelled");
  }

  private static WorkflowError failedError(ExecutionFailure failure) {
    return new WorkflowError(
        failure.type(),
        failure.status() != null ? failure.status() : 500,
        failure.instance() != null ? failure.instance() : failure.definitionPath(),
        failure.title() != null ? failure.title() : failure.type(),
        failure.detail() != null ? failure.detail() : failure.message());
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
