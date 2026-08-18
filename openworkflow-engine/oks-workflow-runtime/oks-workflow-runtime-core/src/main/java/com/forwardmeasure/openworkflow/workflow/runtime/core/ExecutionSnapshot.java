package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ForkPosition;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** OpenWorkflow-specific state; durability metadata is owned by the host. */
public record ExecutionSnapshot(
    ExecutionKey key,
    WorkflowDefinitionReference definition,
    @JsonIgnore WorkflowPlan plan,
    ActorContext startedBy,
    Instant startedAt,
    ExecutionPhase phase,
    ExecutionCursor cursor,
    DataReference initialInput,
    DataReference context,
    DataReference data,
    long nextSequence,
    ExecutionFailure failure,
    String laneRootTaskPath,
    ForkRuntimeState activeFork,
    List<ForkPosition> forkPositions,
    PendingInteraction pendingInteraction,
    List<ActiveTimeoutState> activeTimeouts,
    CancellationState cancellation,
    PendingWorkflowComputation pendingComputation) {

  public ExecutionSnapshot {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(startedBy, "startedBy");
    Objects.requireNonNull(startedAt, "startedAt");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(cursor, "cursor");
    Objects.requireNonNull(initialInput, "initialInput");
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(data, "data");
    if (!key.tenantId().equals(startedBy.tenantId())) {
      throw new IllegalArgumentException("Execution actor and key must belong to the same tenant");
    }
    forkPositions = forkPositions == null ? List.of() : List.copyOf(forkPositions);
    activeTimeouts = activeTimeouts == null ? List.of() : List.copyOf(activeTimeouts);
    if (nextSequence < 0) {
      throw new IllegalArgumentException("nextSequence must not be negative");
    }
    boolean purgePreservesFailure =
        phase == ExecutionPhase.PURGING
            && pendingInteraction instanceof ActiveExecutionPurgeState purge
            && purge.terminalPhase() == ExecutionPhase.FAILED
            && purge.terminalFailure() != null;
    if ((phase == ExecutionPhase.FAILED) != (failure != null) && !purgePreservesFailure) {
      throw new IllegalArgumentException("Only a FAILED execution carries a failure");
    }
    if (!key.tenantId().equals(definition.key().tenantId())) {
      throw new IllegalArgumentException("Execution key and definition tenants do not match");
    }
    if (plan != null
        && (!definition.key().coordinates().equals(plan.coordinates())
            || !definition.sourceSha256().equals(plan.sourceSha256())
            || !definition.definitionSha256().equals(plan.definitionSha256()))) {
      throw new IllegalArgumentException("Execution definition reference does not match its plan");
    }
    if (laneRootTaskPath != null && laneRootTaskPath.isBlank()) {
      throw new IllegalArgumentException("laneRootTaskPath must not be blank");
    }
    if (cursor.complete()
        && !phase.terminal()
        && phase != ExecutionPhase.PURGING
        && laneRootTaskPath == null) {
      throw new IllegalArgumentException("Only terminal executions may have a completed cursor");
    }
    if ((phase == ExecutionPhase.COMPLETED || phase == ExecutionPhase.CANCELLED)
        && (!cursor.complete() || laneRootTaskPath != null)) {
      throw new IllegalArgumentException(phase + " executions require a completed cursor");
    }
    if (activeFork != null && cursor.complete()) {
      throw new IllegalArgumentException("An active fork requires its parent cursor");
    }
    if (pendingInteraction != null && phase.terminal()) {
      throw new IllegalArgumentException(
          "A terminal execution cannot retain an active subscription");
    }
    if (phase.terminal() && !activeTimeouts.isEmpty()) {
      throw new IllegalArgumentException("A terminal execution cannot retain active timeouts");
    }
    long workflowTimeouts =
        activeTimeouts.stream().filter(ActiveTimeoutState::workflowTimeout).count();
    if (workflowTimeouts > 1) {
      throw new IllegalArgumentException("An execution may have only one workflow timeout");
    }
    if ((phase == ExecutionPhase.CANCEL_REQUESTED) != (cancellation != null)
        && !(phase == ExecutionPhase.COMPUTING
            && pendingComputation != null
            && pendingComputation.basePhase() == ExecutionPhase.CANCEL_REQUESTED
            && cancellation != null)) {
      throw new IllegalArgumentException(
          "Only a CANCEL_REQUESTED execution carries " + "cancellation state");
    }
    if ((phase == ExecutionPhase.COMPUTING) != (pendingComputation != null)) {
      throw new IllegalArgumentException(
          "Only a COMPUTING execution carries pending " + "workflow computation");
    }
    if (pendingComputation != null) {
      if (!key.equals(pendingComputation.command().key())) {
        throw new IllegalArgumentException(
            "Pending computation command targets another " + "execution");
      }
      if (pendingComputation.startsExecution() && nextSequence != 0) {
        throw new IllegalArgumentException("A deferred start has not emitted workflow history");
      }
    }
  }

  /**
   * Attaches the immutable admitted plan for one reducer decision.
   *
   * <p>The plan is deliberately excluded from the serialized execution snapshot. It is already
   * durably stored once in the compacted definition catalogue and embedding it in every execution
   * would make Kafka state records grow with definition size.
   */
  public ExecutionSnapshot withPlan(WorkflowPlan resolvedPlan) {
    Objects.requireNonNull(resolvedPlan, "resolvedPlan");
    if (plan != null) {
      if (!plan.equals(resolvedPlan)) {
        throw new IllegalArgumentException("Execution already carries another workflow plan");
      }
      return this;
    }
    return new ExecutionSnapshot(
        key,
        definition,
        resolvedPlan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        laneRootTaskPath,
        activeFork,
        forkPositions,
        pendingInteraction,
        activeTimeouts,
        cancellation,
        pendingComputation);
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence,
      ExecutionFailure failure,
      String laneRootTaskPath,
      ForkRuntimeState activeFork,
      List<ForkPosition> forkPositions,
      PendingInteraction pendingInteraction,
      List<ActiveTimeoutState> activeTimeouts,
      CancellationState cancellation) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        laneRootTaskPath,
        activeFork,
        forkPositions,
        pendingInteraction,
        activeTimeouts,
        cancellation,
        null);
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence,
      ExecutionFailure failure,
      String laneRootTaskPath,
      ForkRuntimeState activeFork,
      List<ForkPosition> forkPositions,
      PendingInteraction pendingInteraction,
      List<ActiveTimeoutState> activeTimeouts) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        laneRootTaskPath,
        activeFork,
        forkPositions,
        pendingInteraction,
        activeTimeouts,
        null);
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence,
      ExecutionFailure failure,
      String laneRootTaskPath,
      ForkRuntimeState activeFork,
      List<ForkPosition> forkPositions,
      PendingInteraction pendingInteraction) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        laneRootTaskPath,
        activeFork,
        forkPositions,
        pendingInteraction,
        List.of());
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence,
      ExecutionFailure failure,
      String laneRootTaskPath,
      ForkRuntimeState activeFork,
      List<ForkPosition> forkPositions) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        laneRootTaskPath,
        activeFork,
        forkPositions,
        null,
        List.of());
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        null,
        null,
        null,
        List.of(),
        null,
        List.of());
  }

  public ExecutionSnapshot(
      ExecutionKey key,
      WorkflowDefinitionReference definition,
      WorkflowPlan plan,
      ActorContext startedBy,
      Instant startedAt,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference initialInput,
      DataReference context,
      DataReference data,
      long nextSequence,
      ExecutionFailure failure) {
    this(
        key,
        definition,
        plan,
        startedBy,
        startedAt,
        phase,
        cursor,
        initialInput,
        context,
        data,
        nextSequence,
        failure,
        null,
        null,
        List.of(),
        null,
        List.of());
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
