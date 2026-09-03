package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservation;
import java.time.Instant;
import java.util.Objects;

/** One governed human task awaiting a durable correlated outcome. */
public record ActiveHumanTaskState(
    String humanTaskId,
    BusinessCorrelationId correlationId,
    String taskPath,
    DataReference rawInput,
    DataReference taskInput,
    ExecutionCursor resumeCursor,
    DataReference descriptor,
    String dueTimerId,
    Instant dueAt,
    HumanTaskObservation terminalObservation)
    implements PendingInteraction {

  public ActiveHumanTaskState {
    requireText(humanTaskId, "humanTaskId");
    Objects.requireNonNull(correlationId, "correlationId");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(taskInput, "taskInput");
    Objects.requireNonNull(resumeCursor, "resumeCursor");
    Objects.requireNonNull(descriptor, "descriptor");
    if ((dueTimerId == null) != (dueAt == null)) {
      throw new IllegalArgumentException("Human-task due timer ID and due time must coexist");
    }
  }

  public ActiveHumanTaskState(
      String humanTaskId,
      BusinessCorrelationId correlationId,
      String taskPath,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCursor resumeCursor,
      DataReference descriptor,
      String dueTimerId,
      Instant dueAt) {
    this(
        humanTaskId,
        correlationId,
        taskPath,
        rawInput,
        taskInput,
        resumeCursor,
        descriptor,
        dueTimerId,
        dueAt,
        null);
  }

  public boolean completionReady() {
    return terminalObservation != null;
  }

  public ActiveHumanTaskState withTerminalObservation(HumanTaskObservation observation) {
    return new ActiveHumanTaskState(
        humanTaskId,
        correlationId,
        taskPath,
        rawInput,
        taskInput,
        resumeCursor,
        descriptor,
        dueTimerId,
        dueAt,
        Objects.requireNonNull(observation, "observation"));
  }

  @Override
  public String interactionId() {
    return humanTaskId;
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
