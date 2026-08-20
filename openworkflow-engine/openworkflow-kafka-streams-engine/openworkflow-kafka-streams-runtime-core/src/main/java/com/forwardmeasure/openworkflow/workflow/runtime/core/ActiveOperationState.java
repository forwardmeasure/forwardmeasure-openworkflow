package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import java.util.Objects;

/** One externally executed call/run operation awaiting an adapter outcome. */
public record ActiveOperationState(
    String operationId,
    String taskPath,
    String operationKind,
    DataReference rawInput,
    DataReference taskInput,
    ExecutionCursor resumeCursor,
    DataReference descriptor,
    OperationObservation terminalObservation)
    implements PendingInteraction {

  public ActiveOperationState {
    requireText(operationId, "operationId");
    requireText(taskPath, "taskPath");
    requireText(operationKind, "operationKind");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(taskInput, "taskInput");
    Objects.requireNonNull(resumeCursor, "resumeCursor");
    Objects.requireNonNull(descriptor, "descriptor");
    if (terminalObservation != null
        && terminalObservation.status() == OperationObservationStatus.PROGRESS) {
      throw new IllegalArgumentException("Only a terminal operation observation can be buffered");
    }
  }

  public ActiveOperationState(
      String operationId,
      String taskPath,
      String operationKind,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCursor resumeCursor,
      DataReference descriptor) {
    this(operationId, taskPath, operationKind, rawInput, taskInput, resumeCursor, descriptor, null);
  }

  public boolean completionReady() {
    return terminalObservation != null;
  }

  public ActiveOperationState withTerminalObservation(OperationObservation observation) {
    return new ActiveOperationState(
        operationId,
        taskPath,
        operationKind,
        rawInput,
        taskInput,
        resumeCursor,
        descriptor,
        Objects.requireNonNull(observation, "observation"));
  }

  @Override
  public String interactionId() {
    return operationId;
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
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
