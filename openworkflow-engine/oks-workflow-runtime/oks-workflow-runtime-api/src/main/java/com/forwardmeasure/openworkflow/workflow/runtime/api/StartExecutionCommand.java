package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** Authenticated request to create one tenant-scoped workflow execution. */
public record StartExecutionCommand(
    String commandId,
    ExecutionKey key,
    WorkflowDefinitionReference definition,
    DataReference input,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public StartExecutionCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(input, "input");
    if (actor.correlationId() == null) {
      actor = actor.withCorrelationId(new BusinessCorrelationId(key.executionId().value()));
    }
    if (!key.tenantId().equals(definition.key().tenantId())) {
      throw new IllegalArgumentException("Execution and definition tenants must match");
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
