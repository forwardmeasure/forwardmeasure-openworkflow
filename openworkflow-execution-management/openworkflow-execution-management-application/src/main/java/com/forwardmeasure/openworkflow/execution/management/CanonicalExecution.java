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
package com.forwardmeasure.openworkflow.execution.management;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import java.time.Instant;
import java.util.Objects;

/** Canonical product truth created before a start command reaches an engine. */
public record CanonicalExecution(
    ExecutionId executionId,
    DefinitionRevision definition,
    EngineId engineId,
    ExecutionLifecycleState state,
    long version,
    String idempotencyKey,
    String correlationId,
    ActorId startedBy,
    JsonNode input,
    Instant createdAt,
    Instant updatedAt) {
  public CanonicalExecution {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(engineId, "engineId");
    Objects.requireNonNull(state, "state");
    if (version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
    Objects.requireNonNull(idempotencyKey, "idempotencyKey");
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(startedBy, "startedBy");
    input = Objects.requireNonNull(input, "input").deepCopy();
    Objects.requireNonNull(createdAt, "createdAt");
    Objects.requireNonNull(updatedAt, "updatedAt");
  }

  @Override
  public JsonNode input() {
    return input.deepCopy();
  }
}
