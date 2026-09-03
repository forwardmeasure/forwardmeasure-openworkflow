package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.data.DataReference;
import java.util.Objects;

/**
 * One durable sequential container frame.
 *
 * <p>A {@code null} task path identifies the workflow's root {@code do} list. Other frames identify
 * an entered structural {@code do} task.
 */
public record ExecutionFrame(
    String taskPath,
    int nextChildIndex,
    DataReference rawInput,
    DataReference input,
    ForIterationState iteration,
    TryRuntimeState tryState,
    ExtensionRuntimeState extensionState) {

  public ExecutionFrame {
    if (taskPath != null && taskPath.isBlank()) {
      throw new IllegalArgumentException("taskPath must not be blank");
    }
    if (nextChildIndex < 0) {
      throw new IllegalArgumentException("nextChildIndex must not be negative");
    }
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(input, "input");
  }

  public static ExecutionFrame root(DataReference input) {
    return new ExecutionFrame(null, 0, input, input, null, null, null);
  }

  public ExecutionFrame(
      String taskPath, int nextChildIndex, DataReference rawInput, DataReference input) {
    this(taskPath, nextChildIndex, rawInput, input, null, null, null);
  }

  public ExecutionFrame advance() {
    return new ExecutionFrame(
        taskPath,
        Math.addExact(nextChildIndex, 1),
        rawInput,
        input,
        iteration,
        tryState,
        extensionState);
  }

  public ExecutionFrame moveTo(int childIndex) {
    return new ExecutionFrame(
        taskPath, childIndex, rawInput, input, iteration, tryState, extensionState);
  }

  public ExecutionFrame nextIteration(DataReference nextInput) {
    return nextIteration(nextInput, null);
  }

  public ExecutionFrame nextIteration(DataReference nextInput, DataReference completedOutput) {
    if (iteration == null) {
      throw new IllegalStateException("Only a for frame can advance an iteration");
    }
    return new ExecutionFrame(
        taskPath,
        0,
        rawInput,
        input,
        iteration.next(nextInput, completedOutput),
        tryState,
        extensionState);
  }

  public static ExecutionFrame enteredTry(
      String taskPath, DataReference rawInput, DataReference input, java.time.Instant startedAt) {
    return new ExecutionFrame(
        taskPath, 0, rawInput, input, null, TryRuntimeState.body(startedAt), null);
  }

  public ExecutionFrame catching(
      com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError error) {
    if (tryState == null) {
      throw new IllegalStateException("Only a try frame can enter catch");
    }
    return new ExecutionFrame(taskPath, 0, rawInput, input, null, tryState.catching(error), null);
  }

  public ExecutionFrame retrying(
      java.time.Instant nextAttemptAt, String deadlineId, java.time.Instant deadlineAt) {
    if (tryState == null || tryState.phase() != TryRuntimeState.Phase.CATCH) {
      throw new IllegalStateException("Only a catch phase can retry");
    }
    return new ExecutionFrame(
        taskPath,
        0,
        rawInput,
        input,
        null,
        tryState.retrying(nextAttemptAt, deadlineId, deadlineAt),
        null);
  }

  public static ExecutionFrame enteredExtension(
      String taskPath,
      DataReference rawInput,
      DataReference input,
      ExtensionRuntimeState extensionState) {
    return new ExecutionFrame(
        taskPath,
        0,
        rawInput,
        input,
        null,
        null,
        Objects.requireNonNull(extensionState, "extensionState"));
  }

  public ExecutionFrame deferExtensionDirective(String directive) {
    if (extensionState == null) {
      throw new IllegalStateException("Only an extension frame can defer task flow");
    }
    return new ExecutionFrame(
        taskPath,
        nextChildIndex,
        rawInput,
        input,
        iteration,
        tryState,
        extensionState.defer(directive));
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
