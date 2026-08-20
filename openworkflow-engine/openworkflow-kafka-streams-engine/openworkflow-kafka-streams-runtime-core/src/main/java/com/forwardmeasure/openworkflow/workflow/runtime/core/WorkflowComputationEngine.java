package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.durableprocessing.api.DurableProcessContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import java.time.Duration;
import java.util.Objects;

/**
 * Executes one deferred reducer decision on a worker thread with blocking data access enabled.
 *
 * <p>Commands that arrived after the cutpoint remain queued and are returned to Kafka after the
 * computed transition commits. This preserves Kafka order without allowing the worker to mutate the
 * controller state directly.
 */
public final class WorkflowComputationEngine {
  private final WorkflowExecutionEngine engine;
  private final PreparedWorkflowTransitionCodec codec;

  public WorkflowComputationEngine(
      WorkflowDefinitionResolver definitions,
      ActorId runtimeActorId,
      String runtimeComponent,
      Duration cancellationGracePeriod,
      WorkflowRuntimeDataAccess dataAccess,
      ObjectMapper json) {
    this.engine =
        new WorkflowExecutionEngine(
            definitions, runtimeActorId, runtimeComponent, cancellationGracePeriod, dataAccess);
    this.codec = new PreparedWorkflowTransitionCodec(json);
  }

  public PreparedWorkflowTransitionCodec.Encoded compute(ExecutionSnapshot computing) {
    Objects.requireNonNull(computing, "computing");
    if (computing.phase() != ExecutionPhase.COMPUTING || computing.pendingComputation() == null) {
      throw new IllegalArgumentException("Execution has no pending workflow computation");
    }
    PendingWorkflowComputation pending = computing.pendingComputation();
    ExecutionSnapshot base =
        pending.startsExecution() ? null : restoreBase(computing, pending.basePhase());
    var transition =
        engine.decide(
            new DurableProcessContext(
                computing.key().canonical(),
                pending.command().commandId(),
                pending.basisRevision(),
                Math.addExact(pending.basisRevision(), 1),
                pending.command().requestedAt()),
            base,
            pending.command());
    if (transition.state().phase() == ExecutionPhase.COMPUTING) {
      throw new IllegalStateException("Workflow computation reached another data cutpoint");
    }
    return codec.encode(
        new PreparedWorkflowTransition(
            transition.state(),
            transition.events(),
            transition.followUpCommands(),
            transition.outbox()));
  }

  private static ExecutionSnapshot restoreBase(
      ExecutionSnapshot computing, ExecutionPhase basePhase) {
    return new ExecutionSnapshot(
        computing.key(),
        computing.definition(),
        computing.plan(),
        computing.startedBy(),
        computing.startedAt(),
        basePhase,
        computing.cursor(),
        computing.initialInput(),
        computing.context(),
        computing.data(),
        computing.nextSequence(),
        computing.failure(),
        computing.laneRootTaskPath(),
        computing.activeFork(),
        computing.forkPositions(),
        computing.pendingInteraction(),
        computing.activeTimeouts(),
        computing.cancellation(),
        null);
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
