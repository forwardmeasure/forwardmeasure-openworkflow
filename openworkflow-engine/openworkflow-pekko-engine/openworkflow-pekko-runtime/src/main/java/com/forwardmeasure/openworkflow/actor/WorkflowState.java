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
package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.BlockingConstructs;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Durable state variants used directly by Pekko's persistent FSM handlers. */
public sealed interface WorkflowState
    permits WorkflowState.New,
        WorkflowState.Running,
        WorkflowState.Waiting,
        WorkflowState.Pausing,
        WorkflowState.Paused,
        WorkflowState.Cancelling,
        WorkflowState.Cancelled,
        WorkflowState.Completed,
        WorkflowState.Failed {

  int MAX_PROCESSED_COMMANDS = 256;

  ExecutionId executionId();

  long revision();

  ExecutionStatus status();

  default JsonNode data() {
    return NullNode.getInstance();
  }

  default Set<UUID> processedCommands() {
    return Set.of();
  }

  default JsonNode context() {
    return data();
  }

  default JsonNode rawWorkflowInput() {
    return data();
  }

  default List<TaskExecutionFrame> taskStack() {
    return List.of();
  }

  default Instant workflowDeadline() {
    return null;
  }

  record New(ExecutionId executionId) implements WorkflowState {
    public New {
      Objects.requireNonNull(executionId, "executionId");
    }

    @Override
    public long revision() {
      return 0;
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.NEW;
    }
  }

  record Running(
      ExecutionId executionId,
      WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processedCommands,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline)
      implements WorkflowState {

    public Running(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          data,
          data,
          List.of(),
          null);
    }

    public Running(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        JsonNode context,
        JsonNode rawWorkflowInput,
        List<TaskExecutionFrame> taskStack) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          context,
          rawWorkflowInput,
          taskStack,
          null);
    }

    public Running {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(plan, "plan");
      data = copy(data);
      if (nextStep < 0 || revision < 1) {
        throw new IllegalArgumentException("Invalid running FSM position");
      }
      processedCommands = receipts(processedCommands);
      context = copy(context == null ? data : context);
      rawWorkflowInput = copy(rawWorkflowInput == null ? data : rawWorkflowInput);
      taskStack = frames(taskStack);
    }

    @Override
    public ExecutionStatus status() {
      // A pending correlated-worker call is a real wait, not active computation - same semantic
      // WAITING already carries for the dedicated Waiting FSM state (timers/retries). Without
      // this, an execution blocked on an external worker for hours reports RUNNING the whole
      // time, indistinguishable from genuine computation through the public contract. See
      // docs/engine-construct-gap-audit.md gap #4. Routed through the same
      // BlockingConstructs.isBlocking(CORRELATED_WORKER) check openworkflow-kafka-streams-engine
      // consults, instead of a second independently-authored WAITING rule - see gap #4's Phase 4
      // note.
      for (TaskExecutionFrame frame : taskStack) {
        EventExecutionFrame event = frame.event();
        if (event != null
            && event.kind() == EventExecutionFrame.Kind.CORRELATED_WORKER
            && BlockingConstructs.isBlocking(CallPlan.Kind.CORRELATED_WORKER)) {
          return ExecutionStatus.WAITING;
        }
      }
      return ExecutionStatus.RUNNING;
    }
  }

  record Waiting(
      ExecutionId executionId,
      WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processedCommands,
      String reason,
      java.time.Instant deadline,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline)
      implements WorkflowState {
    public Waiting(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        String reason,
        java.time.Instant deadline) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          reason,
          deadline,
          data,
          data,
          List.of(),
          null);
    }

    public Waiting(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        String reason,
        Instant deadline,
        JsonNode context,
        JsonNode rawWorkflowInput,
        List<TaskExecutionFrame> taskStack) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          reason,
          deadline,
          context,
          rawWorkflowInput,
          taskStack,
          null);
    }

    public Waiting {
      requirePosition(executionId, plan, nextStep, revision);
      data = copy(data);
      processedCommands = receipts(processedCommands);
      Objects.requireNonNull(reason, "reason");
      context = copy(context == null ? data : context);
      rawWorkflowInput = copy(rawWorkflowInput == null ? data : rawWorkflowInput);
      taskStack = frames(taskStack);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.WAITING;
    }
  }

  record Pausing(
      ExecutionId executionId,
      WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processedCommands,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline)
      implements WorkflowState {
    public Pausing(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          data,
          data,
          List.of(),
          null);
    }

    public Pausing(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        JsonNode context,
        JsonNode rawWorkflowInput,
        List<TaskExecutionFrame> taskStack) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          context,
          rawWorkflowInput,
          taskStack,
          null);
    }

    public Pausing {
      requirePosition(executionId, plan, nextStep, revision);
      data = copy(data);
      processedCommands = receipts(processedCommands);
      context = copy(context == null ? data : context);
      rawWorkflowInput = copy(rawWorkflowInput == null ? data : rawWorkflowInput);
      taskStack = frames(taskStack);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.PAUSING;
    }
  }

  record Paused(
      ExecutionId executionId,
      WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processedCommands,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline)
      implements WorkflowState {
    public Paused(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          data,
          data,
          List.of(),
          null);
    }

    public Paused(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        JsonNode context,
        JsonNode rawWorkflowInput,
        List<TaskExecutionFrame> taskStack) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          context,
          rawWorkflowInput,
          taskStack,
          null);
    }

    public Paused {
      requirePosition(executionId, plan, nextStep, revision);
      data = copy(data);
      processedCommands = receipts(processedCommands);
      context = copy(context == null ? data : context);
      rawWorkflowInput = copy(rawWorkflowInput == null ? data : rawWorkflowInput);
      taskStack = frames(taskStack);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.PAUSED;
    }
  }

  record Cancelling(
      ExecutionId executionId,
      WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processedCommands,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline)
      implements WorkflowState {
    public Cancelling(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          data,
          data,
          List.of(),
          null);
    }

    public Cancelling(
        ExecutionId executionId,
        WorkflowPlan plan,
        JsonNode data,
        int nextStep,
        long revision,
        Set<UUID> processedCommands,
        JsonNode context,
        JsonNode rawWorkflowInput,
        List<TaskExecutionFrame> taskStack) {
      this(
          executionId,
          plan,
          data,
          nextStep,
          revision,
          processedCommands,
          context,
          rawWorkflowInput,
          taskStack,
          null);
    }

    public Cancelling {
      requirePosition(executionId, plan, nextStep, revision);
      data = copy(data);
      processedCommands = receipts(processedCommands);
      context = copy(context == null ? data : context);
      rawWorkflowInput = copy(rawWorkflowInput == null ? data : rawWorkflowInput);
      taskStack = frames(taskStack);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.CANCELLING;
    }
  }

  record Cancelled(
      ExecutionId executionId, JsonNode data, long revision, Set<UUID> processedCommands)
      implements WorkflowState {
    public Cancelled {
      Objects.requireNonNull(executionId, "executionId");
      data = copy(data);
      if (revision < 1) throw new IllegalArgumentException("revision must be positive");
      processedCommands = receipts(processedCommands);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.CANCELLED;
    }
  }

  record Completed(
      ExecutionId executionId, JsonNode data, long revision, Set<UUID> processedCommands)
      implements WorkflowState {

    public Completed {
      Objects.requireNonNull(executionId, "executionId");
      data = copy(data);
      if (revision < 1) {
        throw new IllegalArgumentException("revision must be positive");
      }
      processedCommands = receipts(processedCommands);
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.COMPLETED;
    }
  }

  record Failed(
      ExecutionId executionId,
      JsonNode data,
      long revision,
      Set<UUID> processedCommands,
      String message)
      implements WorkflowState {

    public Failed {
      Objects.requireNonNull(executionId, "executionId");
      data = copy(data);
      if (revision < 1) {
        throw new IllegalArgumentException("revision must be positive");
      }
      processedCommands = receipts(processedCommands);
      Objects.requireNonNull(message, "message");
    }

    @Override
    public ExecutionStatus status() {
      return ExecutionStatus.FAILED;
    }
  }

  private static JsonNode copy(JsonNode value) {
    return value == null ? NullNode.getInstance() : value.deepCopy();
  }

  private static void requirePosition(
      ExecutionId executionId, WorkflowPlan plan, int nextStep, long revision) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(plan, "plan");
    if (nextStep < 0 || revision < 1) {
      throw new IllegalArgumentException("Invalid durable FSM position");
    }
  }

  private static Set<UUID> receipts(Set<UUID> values) {
    if (values == null || values.isEmpty()) {
      return Set.of();
    }
    var bounded = new LinkedHashSet<>(values);
    while (bounded.size() > MAX_PROCESSED_COMMANDS) {
      bounded.remove(bounded.iterator().next());
    }
    return Collections.unmodifiableSet(bounded);
  }

  private static List<TaskExecutionFrame> frames(List<TaskExecutionFrame> values) {
    return values == null ? List.of() : List.copyOf(values);
  }
}
