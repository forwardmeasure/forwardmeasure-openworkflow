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
package com.forwardmeasure.openworkflow.execution.query;

import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** Stable tenant-scoped execution filtering and cursor pagination contract. */
public record ExecutionSearch(
    TenantId tenantId,
    Set<ExecutionLifecycleState> states,
    EngineId engineId,
    String correlationId,
    Instant createdFrom,
    Instant createdUntil,
    String cursor,
    int limit) {
  public ExecutionSearch {
    Objects.requireNonNull(tenantId, "tenantId");
    states = Set.copyOf(Objects.requireNonNull(states, "states"));
    if (correlationId != null && correlationId.isBlank()) {
      throw new IllegalArgumentException("correlationId must not be blank");
    }
    if (createdFrom != null && createdUntil != null && createdUntil.isBefore(createdFrom)) {
      throw new IllegalArgumentException("createdUntil must not precede createdFrom");
    }
    if (cursor != null && cursor.isBlank()) {
      throw new IllegalArgumentException("cursor must not be blank");
    }
    if (limit < 1 || limit > 200) {
      throw new IllegalArgumentException("limit must be between 1 and 200");
    }
  }
}
