package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Durable cutpoint while a trusted worker performs data-dependent reduction away from the Kafka
 * Streams thread.
 */
public record PendingWorkflowComputation(
    String computationId,
    long basisRevision,
    boolean startsExecution,
    ExecutionPhase basePhase,
    ExecutionCommand command,
    List<ExecutionCommand> queuedCommands) {
  public static final int MAX_QUEUED_COMMANDS = 64;

  public PendingWorkflowComputation {
    requireText(computationId, "computationId");
    if (basisRevision < 1) {
      throw new IllegalArgumentException("basisRevision must be positive");
    }
    Objects.requireNonNull(command, "command");
    if (startsExecution != (command instanceof StartExecutionCommand)) {
      throw new IllegalArgumentException("startsExecution must match the deferred command");
    }
    if (startsExecution && basePhase != null) {
      throw new IllegalArgumentException("A new execution has no base phase");
    }
    if (!startsExecution && basePhase == null) {
      throw new IllegalArgumentException("An existing execution requires its base phase");
    }
    if (basePhase == ExecutionPhase.COMPUTING) {
      throw new IllegalArgumentException("A computation cannot use COMPUTING as its base phase");
    }
    queuedCommands = queuedCommands == null ? List.of() : List.copyOf(queuedCommands);
    if (queuedCommands.size() > MAX_QUEUED_COMMANDS) {
      throw new IllegalArgumentException("Too many commands queued behind workflow computation");
    }
    if (queuedCommands.stream().anyMatch(StartExecutionCommand.class::isInstance)) {
      throw new IllegalArgumentException("A second start command cannot be queued");
    }
  }

  public PendingWorkflowComputation enqueue(ExecutionCommand queued) {
    Objects.requireNonNull(queued, "queued");
    if (!queued.key().equals(command.key())) {
      throw new IllegalArgumentException("Queued command targets another execution");
    }
    if (queued instanceof StartExecutionCommand) {
      throw new IllegalArgumentException("Execution already exists or is being started");
    }
    if (queuedCommands.size() == MAX_QUEUED_COMMANDS) {
      throw new IllegalArgumentException("Workflow computation command queue is full");
    }
    List<ExecutionCommand> updated = new ArrayList<>(queuedCommands);
    updated.add(queued);
    return new PendingWorkflowComputation(
        computationId, basisRevision, startsExecution, basePhase, command, updated);
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
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
