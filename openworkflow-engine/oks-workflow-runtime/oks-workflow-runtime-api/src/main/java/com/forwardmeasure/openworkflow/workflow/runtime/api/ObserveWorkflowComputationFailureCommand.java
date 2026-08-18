package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** Trusted terminal failure from the off-thread workflow computation edge. */
public record ObserveWorkflowComputationFailureCommand(
    String commandId,
    ExecutionKey key,
    String computationId,
    long basisRevision,
    String errorType,
    String message,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {
  private static final int MAX_MESSAGE_LENGTH = 2048;

  public ObserveWorkflowComputationFailureCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    requireText(computationId, "computationId");
    requireText(errorType, "errorType");
    requireText(message, "message");
    if (basisRevision < 1) {
      throw new IllegalArgumentException("basisRevision must be positive");
    }
    if (message.length() > MAX_MESSAGE_LENGTH) {
      throw new IllegalArgumentException("message exceeds " + MAX_MESSAGE_LENGTH + " characters");
    }
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException(
          "A workflow computation failure requires " + "a registered system actor");
    }
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
