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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Canonical current execution view read by APIs and Studio, never directly from engine stores. */
public record ExecutionProjection(
    ExecutionId executionId,
    EngineId engineId,
    DefinitionRevision definition,
    ExecutionLifecycleState state,
    long version,
    String correlationId,
    long lastSequence,
    Instant createdAt,
    Instant updatedAt,
    Instant completedAt,
    JsonNode input,
    JsonNode output,
    ExecutionError error,
    List<ExecutionEffect> effects,
    List<ExecutionTimer> timers) {

  public ExecutionProjection {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(engineId, "engineId");
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(state, "state");
    if (version < 0 || lastSequence < 0) {
      throw new IllegalArgumentException("version and lastSequence must not be negative");
    }
    ContractSupport.requireText(correlationId, "correlationId");
    Objects.requireNonNull(createdAt, "createdAt");
    Objects.requireNonNull(updatedAt, "updatedAt");
    input = ContractSupport.copy(input, "input");
    output = output == null ? null : output.deepCopy();
    effects = List.copyOf(Objects.requireNonNull(effects, "effects"));
    timers = List.copyOf(Objects.requireNonNull(timers, "timers"));
  }

  @Override
  public JsonNode input() {
    return input.deepCopy();
  }

  @Override
  public JsonNode output() {
    return output == null ? null : output.deepCopy();
  }
}
