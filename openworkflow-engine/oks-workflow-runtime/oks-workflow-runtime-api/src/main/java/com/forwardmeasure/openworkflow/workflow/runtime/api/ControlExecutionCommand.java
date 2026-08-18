package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** Authenticated pause, resume or cancellation request. */
public record ControlExecutionCommand(
    String commandId,
    ExecutionKey key,
    ExecutionControlAction action,
    ActorContext actor,
    Instant requestedAt,
    Long expectedRevision)
    implements ExecutionCommand {

  public ControlExecutionCommand {
    requireCommand(commandId, key, actor, requestedAt);
    Objects.requireNonNull(action, "action");
    if (expectedRevision != null && expectedRevision < 0) {
      throw new IllegalArgumentException("expectedRevision must not be negative");
    }
    if (actor.correlationId() == null) {
      actor = actor.withCorrelationId(new BusinessCorrelationId(key.executionId().value()));
    }
  }

  public ControlExecutionCommand(
      String commandId,
      ExecutionKey key,
      ExecutionControlAction action,
      ActorContext actor,
      Instant requestedAt) {
    this(commandId, key, action, actor, requestedAt, null);
  }

  static void requireCommand(
      String commandId, ExecutionKey key, ActorContext actor, Instant requestedAt) {
    Objects.requireNonNull(commandId, "commandId");
    if (commandId.isBlank()) {
      throw new IllegalArgumentException("commandId must not be blank");
    }
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    if (!key.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException("Actor tenant must match execution tenant");
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
