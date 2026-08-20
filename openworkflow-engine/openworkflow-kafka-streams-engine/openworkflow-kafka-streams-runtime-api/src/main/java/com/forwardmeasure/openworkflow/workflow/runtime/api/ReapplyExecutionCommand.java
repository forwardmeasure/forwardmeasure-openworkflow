package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Revision-guarded replay of a command that was durably accepted while an off-thread workflow
 * computation was in progress.
 *
 * <p>The original command ID and authenticated actor remain authoritative. This wrapper is an
 * internal continuation: it prevents a queued control or observation from being applied to any
 * state other than the computation result that released it.
 */
public record ReapplyExecutionCommand(
    ExecutionCommand command, long expectedRevision, List<ExecutionCommand> remainingCommands)
    implements ExecutionCommand {

  public ReapplyExecutionCommand(ExecutionCommand command, long expectedRevision) {
    this(command, expectedRevision, List.of());
  }

  public ReapplyExecutionCommand {
    Objects.requireNonNull(command, "command");
    validateQueued(command);
    if (expectedRevision < 1) {
      throw new IllegalArgumentException("expectedRevision must be positive");
    }
    remainingCommands = remainingCommands == null ? List.of() : List.copyOf(remainingCommands);
    if (remainingCommands.size() >= 64) {
      throw new IllegalArgumentException("Too many remaining queued workflow commands");
    }
    remainingCommands.forEach(ReapplyExecutionCommand::validateQueued);
    if (remainingCommands.stream().anyMatch(queued -> !queued.key().equals(command.key()))) {
      throw new IllegalArgumentException("Queued workflow commands target different executions");
    }
  }

  @Override
  public String commandId() {
    return command.commandId();
  }

  @Override
  public ExecutionKey key() {
    return command.key();
  }

  @Override
  public ActorContext actor() {
    return command.actor();
  }

  @Override
  public Instant requestedAt() {
    return command.requestedAt();
  }

  private static void validateQueued(ExecutionCommand command) {
    Objects.requireNonNull(command, "queued command");
    if (command instanceof StartExecutionCommand
        || command instanceof ReapplyExecutionCommand
        || command instanceof ObserveWorkflowComputationCommand
        || command instanceof ObserveWorkflowComputationFailureCommand) {
      throw new IllegalArgumentException(
          "Unsupported queued workflow command: " + command.getClass().getSimpleName());
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
