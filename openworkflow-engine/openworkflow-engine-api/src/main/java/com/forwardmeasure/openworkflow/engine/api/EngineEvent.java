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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Durable facts emitted by the workflow engine. */
public sealed interface EngineEvent
    permits EngineEvent.Started,
        EngineEvent.TaskEntered,
        EngineEvent.ExtensionEntered,
        EngineEvent.FunctionEntered,
        EngineEvent.ForEntered,
        EngineEvent.ForIterationAdvanced,
        EngineEvent.WaitScheduled,
        EngineEvent.DeadlineScheduled,
        EngineEvent.TryEntered,
        EngineEvent.ErrorRaised,
        EngineEvent.ErrorCaught,
        EngineEvent.RetryScheduled,
        EngineEvent.RetryStarted,
        EngineEvent.ForkEntered,
        EngineEvent.ForkBranchAdvanced,
        EngineEvent.ForkBranchTaskEntered,
        EngineEvent.ForkBranchExtensionEntered,
        EngineEvent.ForkBranchFunctionEntered,
        EngineEvent.ForkBranchTaskCompleted,
        EngineEvent.ForkBranchForEntered,
        EngineEvent.ForkBranchForAdvanced,
        EngineEvent.ForkNestedEntered,
        EngineEvent.ForkNestedBranchAdvanced,
        EngineEvent.ForkNestedCompleted,
        EngineEvent.ForkNestedTaskEntered,
        EngineEvent.ForkNestedExtensionEntered,
        EngineEvent.ForkNestedFunctionEntered,
        EngineEvent.ForkNestedTaskCompleted,
        EngineEvent.ForkNestedForEntered,
        EngineEvent.ForkNestedForAdvanced,
        EngineEvent.ForkBranchWaitScheduled,
        EngineEvent.ForkBranchWaitCompleted,
        EngineEvent.ForkBranchesWaiting,
        EngineEvent.ForkBranchContextUpdated,
        EngineEvent.ForkBranchTryEntered,
        EngineEvent.ForkBranchTryCompleted,
        EngineEvent.ForkBranchErrorCaught,
        EngineEvent.ForkBranchRetryScheduled,
        EngineEvent.ForkBranchRetryStarted,
        EngineEvent.ForkBranchEmitRequested,
        EngineEvent.ForkBranchEmitAcknowledged,
        EngineEvent.ForkBranchHttpCallRequested,
        EngineEvent.ForkBranchHttpCallCompleted,
        EngineEvent.ForkBranchProtocolCallRequested,
        EngineEvent.ForkBranchProtocolCallItemAccepted,
        EngineEvent.ForkBranchProtocolCallCompleted,
        EngineEvent.ForkBranchProtocolCallIterationStarted,
        EngineEvent.ForkBranchProtocolCallIterationAdvanced,
        EngineEvent.ForkBranchListenStarted,
        EngineEvent.ForkBranchListenAccepted,
        EngineEvent.ForkBranchListenIterationAdvanced,
        EngineEvent.ForkBranchEffectSkipped,
        EngineEvent.ForkBranchSubworkflowRequested,
        EngineEvent.ForkBranchSubworkflowCompleted,
        EngineEvent.SubworkflowRequested,
        EngineEvent.SubworkflowCompleted,
        EngineEvent.EmitRequested,
        EngineEvent.EmitAcknowledged,
        EngineEvent.HttpCallRequested,
        EngineEvent.HttpCallCompleted,
        EngineEvent.ProtocolCallRequested,
        EngineEvent.ProtocolCallItemAccepted,
        EngineEvent.ProtocolCallCompleted,
        EngineEvent.ProtocolCallIterationStarted,
        EngineEvent.ProtocolCallIterationAdvanced,
        EngineEvent.CorrelatedWorkerRequested,
        EngineEvent.CorrelatedWorkerCommandPublished,
        EngineEvent.CorrelatedWorkerProgressObserved,
        EngineEvent.CorrelatedWorkerCompleted,
        EngineEvent.CorrelatedWorkerCancellationDispatched,
        EngineEvent.ListenStarted,
        EngineEvent.ListenEventAccepted,
        EngineEvent.ListenUntilAdvanced,
        EngineEvent.ListenIterationStarted,
        EngineEvent.ListenIterationAdvanced,
        EngineEvent.TaskCompleted,
        EngineEvent.PauseRequested,
        EngineEvent.Paused,
        EngineEvent.Resumed,
        EngineEvent.CancellationRequested,
        EngineEvent.Cancelled,
        EngineEvent.Completed,
        EngineEvent.Failed {

  UUID commandId();

  Instant occurredAt();

  record Started(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      WorkflowPlan plan,
      JsonNode input,
      Instant occurredAt)
      implements EngineEvent {

    public Started {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(plan, "plan");
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record TaskEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {

    public TaskEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Extension applicability decisions captured exactly once at entry. */
  record ExtensionEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      List<Boolean> decisions,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ExtensionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      decisions = List.copyOf(Objects.requireNonNull(decisions, "decisions"));
      if (decisions.isEmpty() || nextStep < 0) {
        throw new IllegalArgumentException("Invalid durable extension entry");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A reusable function's caller input and immutable invocation descriptor. */
  record FunctionEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      FunctionOperationDescriptor operation,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {

    public FunctionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operation, "operation");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record TaskCompleted(
      UUID commandId,
      String taskPath,
      int nextStep,
      JsonNode output,
      JsonNode context,
      Instant occurredAt)
      implements EngineEvent {

    public TaskCompleted(
        UUID commandId, String taskPath, int nextStep, JsonNode output, Instant occurredAt) {
      this(commandId, taskPath, nextStep, output, output, occurredAt);
    }

    public TaskCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = context == null ? output.deepCopy() : context.deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A for-task's collection and first cursor, captured once for durable replay. */
  record ForEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || iterationIndex < 0 || iterationIndex >= collection.size()) {
        throw new IllegalArgumentException("Invalid durable for cursor");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Completion of one iteration and the durable cursor for the next one. */
  record ForIterationAdvanced(
      UUID commandId,
      String taskPath,
      JsonNode data,
      int iterationIndex,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForIterationAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      data = Objects.requireNonNull(data, "data").deepCopy();
      if (iterationIndex < 0 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid durable for cursor");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A durable wait deadline established before the actor starts a timer. */
  record WaitScheduled(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant deadline,
      Instant occurredAt)
      implements EngineEvent {
    public WaitScheduled {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** An absolute workflow or task timeout persisted before timer scheduling. */
  record DeadlineScheduled(
      UUID commandId, DeadlineScope scope, String taskPath, Instant deadline, Instant occurredAt)
      implements EngineEvent {
    public DeadlineScheduled {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(scope, "scope");
      if (scope == DeadlineScope.TASK && (taskPath == null || taskPath.isBlank())) {
        throw new IllegalArgumentException("A task deadline requires taskPath");
      }
      if (scope == DeadlineScope.WORKFLOW && taskPath != null) {
        throw new IllegalArgumentException("A workflow deadline cannot carry taskPath");
      }
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Entry into a durable try scope before any guarded task is run. */
  record TryEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public TryEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A structured RFC 9457-style Open Workflow error occurrence. */
  record ErrorRaised(UUID commandId, String taskPath, JsonNode error, Instant occurredAt)
      implements EngineEvent {
    public ErrorRaised {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      error = Objects.requireNonNull(error, "error").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A matching catch clause accepted the structured error. */
  record ErrorCaught(
      UUID commandId, String tryTaskPath, JsonNode error, int nextStep, Instant occurredAt)
      implements EngineEvent {
    public ErrorCaught {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      error = Objects.requireNonNull(error, "error").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A retry deadline persisted before Pekko installs its wake-up timer. */
  record RetryScheduled(
      UUID commandId,
      String tryTaskPath,
      JsonNode error,
      int nextAttempt,
      int retryStep,
      Instant deadline,
      Instant retryStartedAt,
      Instant occurredAt)
      implements EngineEvent {
    public RetryScheduled {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      error = Objects.requireNonNull(error, "error").deepCopy();
      if (nextAttempt < 2 || retryStep < 0) {
        throw new IllegalArgumentException("Invalid retry cursor");
      }
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(retryStartedAt, "retryStartedAt");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** The persisted retry deadline elapsed and the guarded body may run again. */
  record RetryStarted(
      UUID commandId, String tryTaskPath, int attempt, int nextStep, Instant occurredAt)
      implements EngineEvent {
    public RetryStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      if (attempt < 2 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid retry cursor");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Durable declaration-order lanes created for a fork task. */
  record ForkEntered(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      List<String> branchNames,
      List<Integer> branchStarts,
      List<Integer> branchEnds,
      boolean compete,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      branchNames = List.copyOf(branchNames);
      branchStarts = List.copyOf(branchStarts);
      branchEnds = List.copyOf(branchEnds);
      if (branchNames.isEmpty()
          || branchNames.size() != branchStarts.size()
          || branchNames.size() != branchEnds.size()) {
        throw new IllegalArgumentException("Invalid durable fork lanes");
      }
      for (int index = 0; index < branchNames.size(); index++) {
        if (branchNames.get(index).isBlank()
            || branchStarts.get(index) < 0
            || branchEnds.get(index) < branchStarts.get(index)) {
          throw new IllegalArgumentException("Invalid durable fork lane");
        }
      }
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** One lane advanced by one deterministic instruction. */
  record ForkBranchAdvanced(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      JsonNode data,
      int nextStep,
      int nextBranch,
      Integer winner,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      if (branchIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork advance");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A nested task frame entered inside one durable fork lane. */
  record ForkBranchTaskEntered(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      int nextBranch,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchTaskEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (branchIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork task entry");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchExtensionEntered(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      List<Boolean> decisions,
      int nextStep,
      int nextBranch,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchExtensionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      decisions = List.copyOf(Objects.requireNonNull(decisions, "decisions"));
      if (decisions.isEmpty() || branchIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork extension entry");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A reusable function entered in one root-fork lane. */
  record ForkBranchFunctionEntered(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      FunctionOperationDescriptor operation,
      int nextStep,
      int nextBranch,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchFunctionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operation, "operation");
      if (branchIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork function entry");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A nested task frame completed inside one durable fork lane. */
  record ForkBranchTaskCompleted(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode output,
      JsonNode context,
      int nextStep,
      int nextBranch,
      Integer winner,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchTaskCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (branchIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork task completion");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A for-task captured its collection inside one durable fork lane. */
  record ForkBranchForEntered(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable,
      int nextStep,
      int nextBranch,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchForEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (branchIndex < 0
          || !collection.isArray()
          || collection.isEmpty()
          || iterationIndex < 0
          || iterationIndex >= collection.size()
          || nextStep < 0
          || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork iteration entry");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** One fork-local iteration completed and its next index was persisted. */
  record ForkBranchForAdvanced(
      UUID commandId,
      String forkTaskPath,
      int branchIndex,
      String taskPath,
      JsonNode data,
      int iterationIndex,
      int nextStep,
      int nextBranch,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchForAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(forkTaskPath, "forkTaskPath");
      Objects.requireNonNull(taskPath, "taskPath");
      data = Objects.requireNonNull(data, "data").deepCopy();
      if (branchIndex < 0 || iterationIndex < 0 || nextStep < 0 || nextBranch < 0) {
        throw new IllegalArgumentException("Invalid durable fork iteration advance");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A nested fork entered within the branch identified from the root fork. */
  record ForkNestedEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> parentBranchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      List<String> branchNames,
      List<Integer> branchStarts,
      List<Integer> branchEnds,
      boolean compete,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      parentBranchPath = requireBranchPath(parentBranchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      branchNames = List.copyOf(branchNames);
      branchStarts = List.copyOf(branchStarts);
      branchEnds = List.copyOf(branchEnds);
      if (branchNames.isEmpty()
          || branchNames.size() != branchStarts.size()
          || branchNames.size() != branchEnds.size()) {
        throw new IllegalArgumentException("Invalid nested fork lanes");
      }
      for (int index = 0; index < branchNames.size(); index++) {
        if (branchNames.get(index).isBlank()
            || branchStarts.get(index) < 0
            || branchEnds.get(index) < branchStarts.get(index)) {
          throw new IllegalArgumentException("Invalid nested fork lane");
        }
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** One leaf lane in a nested fork tree advanced by one instruction. */
  record ForkNestedBranchAdvanced(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      JsonNode data,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedBranchAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      if (branchPath.size() < 2) {
        throw new IllegalArgumentException(
            "A nested fork advance requires at least two branch coordinates");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A completed nested fork joined back into its containing branch. */
  record ForkNestedCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> parentBranchPath,
      String taskPath,
      JsonNode output,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      parentBranchPath = requireBranchPath(parentBranchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkNestedTaskEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedTaskEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkNestedExtensionEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      List<Boolean> decisions,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedExtensionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      decisions = List.copyOf(Objects.requireNonNull(decisions, "decisions"));
      if (decisions.isEmpty() || nextStep < 0) {
        throw new IllegalArgumentException("Invalid durable nested-fork extension entry");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A reusable function entered at an arbitrary nested-fork coordinate. */
  record ForkNestedFunctionEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      FunctionOperationDescriptor operation,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedFunctionEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operation, "operation");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkNestedTaskCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode output,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedTaskCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkNestedForEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedForEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray()
          || collection.isEmpty()
          || iterationIndex < 0
          || iterationIndex >= collection.size()
          || nextStep < 0) {
        throw new IllegalArgumentException("Invalid nested fork iteration entry");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkNestedForAdvanced(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode data,
      int iterationIndex,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkNestedForAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireNestedBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      data = Objects.requireNonNull(data, "data").deepCopy();
      if (iterationIndex < 0 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid nested fork iteration advance");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A wait frame persisted at an arbitrary declaration-index fork coordinate. */
  record ForkBranchWaitScheduled(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant deadline,
      boolean allBranchesWaiting,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchWaitScheduled {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Timer completion for a durable wait at an arbitrary fork coordinate. */
  record ForkBranchWaitCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode output,
      int nextStep,
      boolean allBranchesWaiting,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchWaitCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Every unfinished leaf in the owning fork tree is durably blocked. */
  record ForkBranchesWaiting(
      UUID commandId, String rootForkTaskPath, Instant deadline, Instant occurredAt)
      implements EngineEvent {
    public ForkBranchesWaiting {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A complete branch-local context snapshot at a durable fork coordinate. */
  record ForkBranchContextUpdated(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      JsonNode context,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchContextUpdated {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      context = Objects.requireNonNull(context, "context").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchTryEntered(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchTryEntered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchTryCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchTryCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchErrorCaught(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String tryTaskPath,
      JsonNode error,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchErrorCaught {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      error = Objects.requireNonNull(error, "error").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchRetryScheduled(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String tryTaskPath,
      JsonNode error,
      int nextAttempt,
      int retryStep,
      Instant deadline,
      Instant retryStartedAt,
      boolean allBranchesWaiting,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchRetryScheduled {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      error = Objects.requireNonNull(error, "error").deepCopy();
      if (nextAttempt < 2 || retryStep < 0) {
        throw new IllegalArgumentException("Invalid branch retry position");
      }
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(retryStartedAt, "retryStartedAt");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchRetryStarted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String tryTaskPath,
      int attempt,
      int nextStep,
      boolean allBranchesWaiting,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchRetryStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      if (attempt < 2 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid branch retry start");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Coordinate-qualified child launch intent owned by one fork leaf. */
  record ForkBranchSubworkflowRequested(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      String operationId,
      ExecutionId childExecutionId,
      ActorIdentity actor,
      com.forwardmeasure.openworkflow.definition.ResolvedSubflow subflow,
      JsonNode childInput,
      boolean await,
      JsonNode detachedOutput,
      JsonNode detachedContext,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchSubworkflowRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(subflow, "subflow");
      childInput = Objects.requireNonNull(childInput, "childInput").deepCopy();
      detachedOutput =
          await && (detachedOutput == null || detachedOutput.isNull())
              ? null
              : detachedOutput == null ? null : detachedOutput.deepCopy();
      detachedContext =
          await && (detachedContext == null || detachedContext.isNull())
              ? null
              : detachedContext == null ? null : detachedContext.deepCopy();
      if (await && (detachedOutput != null || detachedContext != null)) {
        throw new IllegalArgumentException(
            "Awaited fork subworkflow cannot have a detached result");
      }
      if (!await && (detachedOutput == null || detachedContext == null)) {
        throw new IllegalArgumentException(
            "Detached fork subworkflow requires its immediate lane result");
      }
      if (!childExecutionId.tenantId().equals(actor.tenantId())) {
        throw new IllegalArgumentException("Child execution and actor must share a tenant");
      }
      if (!childExecutionId.value().toString().equals(operationId)) {
        throw new IllegalArgumentException(
            "Subworkflow operation ID must equal its child execution UUID");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Persisted fork-leaf transition after an awaited child terminates. */
  record ForkBranchSubworkflowCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      ExecutionId childExecutionId,
      ExecutionStatus childStatus,
      JsonNode output,
      JsonNode context,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchSubworkflowCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(childStatus, "childStatus");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Durable launch intent for an immutable tenant-owned child workflow. */
  record SubworkflowRequested(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      String operationId,
      ExecutionId childExecutionId,
      ActorIdentity actor,
      com.forwardmeasure.openworkflow.definition.ResolvedSubflow subflow,
      JsonNode childInput,
      boolean await,
      JsonNode detachedOutput,
      JsonNode detachedContext,
      Instant occurredAt)
      implements EngineEvent {
    public SubworkflowRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(subflow, "subflow");
      childInput = Objects.requireNonNull(childInput, "childInput").deepCopy();
      detachedOutput =
          await && (detachedOutput == null || detachedOutput.isNull())
              ? null
              : detachedOutput == null ? null : detachedOutput.deepCopy();
      detachedContext =
          await && (detachedContext == null || detachedContext.isNull())
              ? null
              : detachedContext == null ? null : detachedContext.deepCopy();
      if (await && (detachedOutput != null || detachedContext != null)) {
        throw new IllegalArgumentException("Awaited subworkflow cannot have a detached result");
      }
      if (!await && (detachedOutput == null || detachedContext == null)) {
        throw new IllegalArgumentException(
            "Detached subworkflow requires its immediate parent result");
      }
      Objects.requireNonNull(occurredAt, "occurredAt");
      if (!childExecutionId.tenantId().equals(actor.tenantId())) {
        throw new IllegalArgumentException("Child execution and actor must share a tenant");
      }
      if (!childExecutionId.value().toString().equals(operationId)) {
        throw new IllegalArgumentException(
            "Subworkflow operation ID must equal its child execution UUID");
      }
    }
  }

  /** Persisted parent transition after one child terminal result is observed. */
  record SubworkflowCompleted(
      UUID commandId,
      String taskPath,
      String operationId,
      ExecutionId childExecutionId,
      ExecutionStatus childStatus,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public SubworkflowCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(childStatus, "childStatus");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Durable outbox intent; adapters publish this CloudEvent idempotently. */
  record EmitRequested(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      String operationId,
      WorkflowCloudEvent event,
      Instant occurredAt)
      implements EngineEvent {
    public EmitRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record EmitAcknowledged(
      UUID commandId,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public EmitAcknowledged {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Credential-free HTTP/OpenAPI intent persisted before adapter dispatch. */
  record HttpCallRequested(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      HttpOperationDescriptor operation,
      Instant occurredAt)
      implements EngineEvent {
    public HttpCallRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operation, "operation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Persist-confirmed HTTP result after task output transformation. */
  record HttpCallCompleted(
      UUID commandId,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public HttpCallCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Credential-free protocol intent persisted before external dispatch. */
  record ProtocolCallRequested(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      ProtocolOperationDescriptor operation,
      Instant occurredAt)
      implements EngineEvent {
    public ProtocolCallRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operation, "operation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** One persist-confirmed item from a streaming protocol operation. */
  record ProtocolCallItemAccepted(
      UUID commandId, String taskPath, String operationId, JsonNode item, Instant occurredAt)
      implements EngineEvent {
    public ProtocolCallItemAccepted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      item = Objects.requireNonNull(item, "item").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ProtocolCallCompleted(
      UUID commandId,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ProtocolCallCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ProtocolCallIterationStarted(
      UUID commandId,
      String taskPath,
      String operationId,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      String itemVariable,
      String indexVariable,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ProtocolCallIterationStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || collection.isEmpty()) {
        throw new IllegalArgumentException("Protocol foreach requires a non-empty collection");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ProtocolCallIterationAdvanced(
      UUID commandId,
      String taskPath,
      JsonNode collection,
      int iterationIndex,
      JsonNode data,
      JsonNode context,
      int nextStep,
      boolean completed,
      Instant occurredAt)
      implements EngineEvent {
    public ProtocolCallIterationAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || collection.isEmpty() || iterationIndex < 0 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid protocol foreach cursor");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /**
   * A {@code correlated-worker} call materialized its command/events/cancellation operations and
   * entered its durable waiting frame. Mirrors {@code ProtocolCallRequested}'s shape, but a
   * correlated-worker task owns up to three correlated operation identities instead of one.
   */
  record CorrelatedWorkerRequested(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      String lifecycleId,
      ProtocolOperationDescriptor commandOperation,
      ProtocolOperationDescriptor eventsOperation,
      ProtocolOperationDescriptor cancellationOperation,
      Instant occurredAt)
      implements EngineEvent {
    public CorrelatedWorkerRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      requireLifecycleId(lifecycleId);
      Objects.requireNonNull(commandOperation, "commandOperation");
      Objects.requireNonNull(eventsOperation, "eventsOperation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Persist-confirmed acknowledgement that the correlated-worker command was published. */
  record CorrelatedWorkerCommandPublished(
      UUID commandId, String taskPath, String lifecycleId, Instant occurredAt)
      implements EngineEvent {
    public CorrelatedWorkerCommandPublished {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      requireLifecycleId(lifecycleId);
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A non-terminal {@code ACCEPTED}/{@code PROGRESS} correlated-worker event was observed. */
  record CorrelatedWorkerProgressObserved(
      UUID commandId,
      String taskPath,
      String lifecycleId,
      String status,
      JsonNode payload,
      Instant occurredAt)
      implements EngineEvent {
    public CorrelatedWorkerProgressObserved {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      requireLifecycleId(lifecycleId);
      if (status == null || status.isBlank()) {
        throw new IllegalArgumentException("status must not be blank");
      }
      payload = Objects.requireNonNull(payload, "payload").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** The correlated-worker's events channel delivered a terminal {@code SUCCEEDED} outcome. */
  record CorrelatedWorkerCompleted(
      UUID commandId,
      String taskPath,
      String lifecycleId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public CorrelatedWorkerCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      requireLifecycleId(lifecycleId);
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /**
   * The workflow was cancelled while a correlated-worker call was pending and its authoritative
   * cancellation operation was dispatched through the same adapter path as its command/events
   * operations - not merely a local wait being dropped.
   */
  record CorrelatedWorkerCancellationDispatched(
      UUID commandId,
      String taskPath,
      String lifecycleId,
      ProtocolOperationDescriptor cancellationOperation,
      Instant occurredAt)
      implements EngineEvent {
    public CorrelatedWorkerCancellationDispatched {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      requireLifecycleId(lifecycleId);
      Objects.requireNonNull(cancellationOperation, "cancellationOperation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  private static void requireLifecycleId(String lifecycleId) {
    if (lifecycleId == null || lifecycleId.isBlank()) {
      throw new IllegalArgumentException("lifecycleId must not be blank");
    }
  }

  record ListenStarted(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      int nextStep,
      String operationId,
      java.util.Set<String> eventTypes,
      Instant occurredAt)
      implements EngineEvent {
    public ListenStarted(
        UUID commandId,
        String taskPath,
        JsonNode rawInput,
        JsonNode input,
        int nextStep,
        String operationId,
        Instant occurredAt) {
      this(
          commandId,
          taskPath,
          rawInput,
          input,
          nextStep,
          operationId,
          java.util.Set.of(),
          occurredAt);
    }

    public ListenStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(operationId, "operationId");
      eventTypes = eventTypes == null ? java.util.Set.of() : java.util.Set.copyOf(eventTypes);
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ListenEventAccepted(
      UUID commandId,
      String taskPath,
      String operationId,
      WorkflowCloudEvent event,
      List<WorkflowCloudEvent> accepted,
      java.util.Map<String, JsonNode> correlations,
      java.util.Set<Integer> matchedFilters,
      boolean completed,
      JsonNode output,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ListenEventAccepted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(event, "event");
      accepted = List.copyOf(Objects.requireNonNull(accepted, "accepted"));
      correlations = correlations == null ? java.util.Map.of() : java.util.Map.copyOf(correlations);
      matchedFilters =
          matchedFilters == null ? java.util.Set.of() : java.util.Set.copyOf(matchedFilters);
      if (output != null && output.isNull()) output = null;
      if (context != null && context.isNull()) context = null;
      if (completed) {
        output = Objects.requireNonNull(output, "output").deepCopy();
        context = Objects.requireNonNull(context, "context").deepCopy();
      } else if (output != null || context != null) {
        throw new IllegalArgumentException("Incomplete listen cannot have output");
      }
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Partial durable progress through an event-based listen until strategy. */
  record ListenUntilAdvanced(
      UUID commandId,
      String taskPath,
      String operationId,
      WorkflowCloudEvent event,
      EventConsumptionWindow untilWindow,
      Instant occurredAt)
      implements EngineEvent {
    public ListenUntilAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(untilWindow, "untilWindow");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A completed listen collection entering its durable FIFO foreach body. */
  record ListenIterationStarted(
      UUID commandId,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      String itemVariable,
      String indexVariable,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ListenIterationStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || collection.isEmpty()) {
        throw new IllegalArgumentException(
            "Listen foreach requires a non-empty durable collection");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** One FIFO listen item completed and the next durable item cursor was selected. */
  record ListenIterationAdvanced(
      UUID commandId,
      String taskPath,
      JsonNode collection,
      int iterationIndex,
      JsonNode data,
      JsonNode context,
      int nextStep,
      Instant occurredAt)
      implements EngineEvent {
    public ListenIterationAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(taskPath, "taskPath");
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || iterationIndex < 0 || iterationIndex >= collection.size()) {
        throw new IllegalArgumentException("Invalid durable listen foreach cursor");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Materialized emit intent owned by one arbitrary-depth fork lane. */
  record ForkBranchEmitRequested(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      String operationId,
      WorkflowCloudEvent event,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchEmitRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Persist-confirmed completion of one fork-lane emit effect. */
  record ForkBranchEmitAcknowledged(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchEmitAcknowledged {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Materialized HTTP/OpenAPI intent owned by an arbitrary-depth fork lane. */
  record ForkBranchHttpCallRequested(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      HttpOperationDescriptor operation,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchHttpCallRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operation, "operation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchHttpCallCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchHttpCallCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchProtocolCallRequested(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      ProtocolOperationDescriptor operation,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchProtocolCallRequested {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operation, "operation");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchProtocolCallItemAccepted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      JsonNode item,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchProtocolCallItemAccepted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      item = Objects.requireNonNull(item, "item").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchProtocolCallCompleted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      JsonNode output,
      JsonNode context,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchProtocolCallCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchProtocolCallIterationStarted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      String itemVariable,
      String indexVariable,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchProtocolCallIterationStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || collection.isEmpty() || nextStep < 0) {
        throw new IllegalArgumentException("Invalid fork protocol foreach start");
      }
      Objects.requireNonNull(itemVariable, "itemVariable");
      Objects.requireNonNull(indexVariable, "indexVariable");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ForkBranchProtocolCallIterationAdvanced(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode collection,
      int iterationIndex,
      JsonNode data,
      JsonNode context,
      int nextStep,
      boolean completed,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchProtocolCallIterationAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || collection.isEmpty() || iterationIndex < 0 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid fork protocol foreach cursor");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Durable subscription boundary owned by one arbitrary-depth fork lane. */
  record ForkBranchListenStarted(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      String operationId,
      java.util.Set<String> eventTypes,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchListenStarted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(operationId, "operationId");
      eventTypes = eventTypes == null ? java.util.Set.of() : java.util.Set.copyOf(eventTypes);
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  enum ForkListenDisposition {
    PARTIAL,
    COMPLETE,
    ITERATE
  }

  /** One lane update produced while offering a CloudEvent to all active fork listens. */
  record ForkListenUpdate(
      List<Integer> branchPath,
      String taskPath,
      String operationId,
      java.util.List<WorkflowCloudEvent> accepted,
      java.util.Map<String, JsonNode> correlations,
      java.util.Set<Integer> matchedFilters,
      EventConsumptionWindow untilWindow,
      ForkListenDisposition disposition,
      JsonNode output,
      JsonNode context,
      int nextStep,
      JsonNode collection,
      String itemVariable,
      String indexVariable) {
    public ForkListenUpdate {
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(operationId, "operationId");
      accepted = java.util.List.copyOf(Objects.requireNonNull(accepted, "accepted"));
      correlations = correlations == null ? java.util.Map.of() : java.util.Map.copyOf(correlations);
      matchedFilters =
          matchedFilters == null ? java.util.Set.of() : java.util.Set.copyOf(matchedFilters);
      Objects.requireNonNull(disposition, "disposition");
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      output = output == null ? null : output.deepCopy();
      context = context == null ? null : context.deepCopy();
      collection = collection == null ? null : collection.deepCopy();
      if (disposition == ForkListenDisposition.COMPLETE && (output == null || context == null)) {
        throw new IllegalArgumentException("Completed fork listen requires output/context");
      }
      if (disposition == ForkListenDisposition.ITERATE
          && (collection == null
              || !collection.isArray()
              || collection.isEmpty()
              || itemVariable == null
              || indexVariable == null)) {
        throw new IllegalArgumentException("Fork listen iteration requires collection/variables");
      }
    }
  }

  /** One inbound event atomically offered to every currently active fork-lane listen. */
  record ForkBranchListenAccepted(
      UUID commandId,
      String rootForkTaskPath,
      WorkflowCloudEvent event,
      List<ForkListenUpdate> updates,
      boolean hasActiveListeners,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchListenAccepted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      Objects.requireNonNull(event, "event");
      updates = List.copyOf(Objects.requireNonNull(updates, "updates"));
      if (updates.isEmpty()) throw new IllegalArgumentException("updates must not be empty");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Durable foreach cursor/result update for a completed fork-lane listen collection. */
  record ForkBranchListenIterationAdvanced(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode collection,
      int iterationIndex,
      JsonNode data,
      JsonNode context,
      int nextStep,
      boolean completed,
      boolean hasActiveListeners,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchListenIterationAdvanced {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      collection = Objects.requireNonNull(collection, "collection").deepCopy();
      if (!collection.isArray() || iterationIndex < 0 || nextStep < 0) {
        throw new IllegalArgumentException("Invalid fork listen iteration advance");
      }
      data = Objects.requireNonNull(data, "data").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Condition-false completion of an effect task inside a fork lane. */
  record ForkBranchEffectSkipped(
      UUID commandId,
      String rootForkTaskPath,
      List<Integer> branchPath,
      String taskPath,
      JsonNode output,
      JsonNode context,
      int nextStep,
      boolean allBranchesBlocked,
      Instant occurredAt)
      implements EngineEvent {
    public ForkBranchEffectSkipped {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(rootForkTaskPath, "rootForkTaskPath");
      branchPath = requireBranchPath(branchPath);
      Objects.requireNonNull(taskPath, "taskPath");
      output = Objects.requireNonNull(output, "output").deepCopy();
      context = Objects.requireNonNull(context, "context").deepCopy();
      if (nextStep < 0) throw new IllegalArgumentException("nextStep must not be negative");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  private static List<Integer> requireNestedBranchPath(List<Integer> path) {
    path = requireBranchPath(path);
    if (path.size() < 2) {
      throw new IllegalArgumentException(
          "A nested fork event requires at least two branch coordinates");
    }
    return path;
  }

  private static List<Integer> requireBranchPath(List<Integer> path) {
    path = List.copyOf(Objects.requireNonNull(path, "path"));
    if (path.isEmpty() || path.stream().anyMatch(index -> index == null || index < 0)) {
      throw new IllegalArgumentException("Invalid fork branch path");
    }
    return path;
  }

  record PauseRequested(UUID commandId, ActorIdentity actor, Instant occurredAt)
      implements EngineEvent {
    public PauseRequested {
      requireControl(commandId, actor, occurredAt);
    }
  }

  record Paused(UUID commandId, List<String> activeTaskPaths, Instant occurredAt)
      implements EngineEvent {
    public Paused(UUID commandId, Instant occurredAt) {
      this(commandId, List.of(), occurredAt);
    }

    public Paused {
      Objects.requireNonNull(commandId, "commandId");
      activeTaskPaths = activeTaskPaths == null ? List.of() : List.copyOf(activeTaskPaths);
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record Resumed(
      UUID commandId, ActorIdentity actor, List<String> activeTaskPaths, Instant occurredAt)
      implements EngineEvent {
    public Resumed(UUID commandId, ActorIdentity actor, Instant occurredAt) {
      this(commandId, actor, List.of(), occurredAt);
    }

    public Resumed {
      requireControl(commandId, actor, occurredAt);
      activeTaskPaths = activeTaskPaths == null ? List.of() : List.copyOf(activeTaskPaths);
    }
  }

  record CancellationRequested(UUID commandId, ActorIdentity actor, Instant occurredAt)
      implements EngineEvent {
    public CancellationRequested {
      requireControl(commandId, actor, occurredAt);
    }
  }

  record Cancelled(UUID commandId, List<String> activeTaskPaths, Instant occurredAt)
      implements EngineEvent {
    public Cancelled(UUID commandId, Instant occurredAt) {
      this(commandId, List.of(), occurredAt);
    }

    public Cancelled {
      Objects.requireNonNull(commandId, "commandId");
      activeTaskPaths = activeTaskPaths == null ? List.of() : List.copyOf(activeTaskPaths);
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record Completed(UUID commandId, JsonNode output, Instant occurredAt) implements EngineEvent {

    public Completed {
      Objects.requireNonNull(commandId, "commandId");
      output = Objects.requireNonNull(output, "output").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record Failed(UUID commandId, String message, Instant occurredAt) implements EngineEvent {

    public Failed {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(message, "message");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  private static void requireControl(UUID commandId, ActorIdentity actor, Instant occurredAt) {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(occurredAt, "occurredAt");
  }
}
