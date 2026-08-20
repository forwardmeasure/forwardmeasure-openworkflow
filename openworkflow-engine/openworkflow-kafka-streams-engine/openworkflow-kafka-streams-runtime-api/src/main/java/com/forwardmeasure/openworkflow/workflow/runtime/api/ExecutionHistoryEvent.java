package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.definition.PlanStepKind;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Append-only audit event. Task events retain the exact input and output needed by the future
 * execution-flow UI.
 */
public record ExecutionHistoryEvent(
    String eventId,
    ExecutionKey key,
    long sequence,
    ExecutionEventType type,
    String definitionSha256,
    String taskPath,
    String taskName,
    PlanStepKind taskKind,
    DataReference input,
    DataReference output,
    List<IterationPosition> iterations,
    List<ForkPosition> forks,
    ExecutionFailure failure,
    SwitchDecision switchDecision,
    ActorContext actor,
    Instant occurredAt) {

  public ExecutionHistoryEvent {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(definitionSha256, "definitionSha256");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(occurredAt, "occurredAt");
    if (!key.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException(
          "History actor and execution must belong to the " + "same tenant");
    }
    iterations = iterations == null ? List.of() : List.copyOf(iterations);
    forks = forks == null ? List.of() : List.copyOf(forks);
    if (sequence < 0) {
      throw new IllegalArgumentException("sequence must not be negative");
    }
    if ((type == ExecutionEventType.EXECUTION_FAILED) != (failure != null)) {
      throw new IllegalArgumentException("Only EXECUTION_FAILED events carry a failure");
    }
    if (switchDecision != null
        && (type != ExecutionEventType.TASK_COMPLETED || taskKind != PlanStepKind.SWITCH)) {
      throw new IllegalArgumentException("Switch decisions belong only to completed SWITCH tasks");
    }
    if ((type == ExecutionEventType.ITERATION_STARTED
            || type == ExecutionEventType.ITERATION_COMPLETED)
        && (taskKind != PlanStepKind.FOR
                && taskKind != PlanStepKind.LISTEN
                && taskKind != PlanStepKind.CALL
            || iterations.isEmpty()
            || !iterations.getLast().taskPath().equals(taskPath))) {
      throw new IllegalArgumentException("Iteration events require their iterable task position");
    }
    if ((type == ExecutionEventType.FORK_BRANCH_STARTED
            || type == ExecutionEventType.FORK_BRANCH_COMPLETED
            || type == ExecutionEventType.FORK_BRANCH_ABANDONED)
        && (taskKind != PlanStepKind.FORK
            || forks.isEmpty()
            || !forks.getLast().forkTaskPath().equals(taskPath))) {
      throw new IllegalArgumentException("Fork branch events require their FORK position");
    }
    if (type == ExecutionEventType.EVENT_EMITTED
        && (taskKind != PlanStepKind.EMIT || output == null)) {
      throw new IllegalArgumentException(
          "EVENT_EMITTED requires an EMIT task and CloudEvent output");
    }
  }

  public ExecutionHistoryEvent(
      String eventId,
      ExecutionKey key,
      long sequence,
      ExecutionEventType type,
      String definitionSha256,
      String taskPath,
      String taskName,
      PlanStepKind taskKind,
      DataReference input,
      DataReference output,
      ActorContext actor,
      Instant occurredAt) {
    this(
        eventId,
        key,
        sequence,
        type,
        definitionSha256,
        taskPath,
        taskName,
        taskKind,
        input,
        output,
        List.of(),
        List.of(),
        null,
        null,
        actor,
        occurredAt);
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
