package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Authoritative state for one command/progress/result worker lifecycle. */
public record ActiveCorrelatedWorkerState(
    String lifecycleId,
    String taskPath,
    DataReference rawInput,
    DataReference taskInput,
    ExecutionCursor resumeCursor,
    DataReference commandDescriptor,
    DataReference subscriptionDescriptor,
    DataReference cancellationDescriptor,
    Set<String> seenSourcePositions,
    String deadlineTimerId,
    Instant deadlineAt,
    boolean commandPublished,
    DataReference bufferedTerminalMessage)
    implements PendingInteraction {

  public ActiveCorrelatedWorkerState {
    requireText(lifecycleId, "lifecycleId");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(taskInput, "taskInput");
    Objects.requireNonNull(resumeCursor, "resumeCursor");
    Objects.requireNonNull(commandDescriptor, "commandDescriptor");
    Objects.requireNonNull(subscriptionDescriptor, "subscriptionDescriptor");
    seenSourcePositions =
        seenSourcePositions == null
            ? Set.of()
            : Collections.unmodifiableSortedSet(new TreeSet<>(seenSourcePositions));
    if ((deadlineTimerId == null) != (deadlineAt == null)) {
      throw new IllegalArgumentException("Worker deadline ID and due time must both be present");
    }
  }

  public ActiveCorrelatedWorkerState withCommandPublished() {
    return new ActiveCorrelatedWorkerState(
        lifecycleId,
        taskPath,
        rawInput,
        taskInput,
        resumeCursor,
        commandDescriptor,
        subscriptionDescriptor,
        cancellationDescriptor,
        seenSourcePositions,
        deadlineTimerId,
        deadlineAt,
        true,
        bufferedTerminalMessage);
  }

  public ActiveCorrelatedWorkerState observe(String sourcePosition, DataReference terminalMessage) {
    TreeSet<String> seen = new TreeSet<>(seenSourcePositions);
    seen.add(sourcePosition);
    return new ActiveCorrelatedWorkerState(
        lifecycleId,
        taskPath,
        rawInput,
        taskInput,
        resumeCursor,
        commandDescriptor,
        subscriptionDescriptor,
        cancellationDescriptor,
        seen,
        deadlineTimerId,
        deadlineAt,
        commandPublished,
        terminalMessage == null ? bufferedTerminalMessage : terminalMessage);
  }

  @Override
  public String interactionId() {
    return lifecycleId;
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
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
