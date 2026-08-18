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

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** Tenant-scoped canonical execution filter. Pagination is owned by the later query capability. */
public record ExecutionQuery(
    TenantId tenantId,
    Set<ExecutionLifecycleState> states,
    EngineId engineId,
    Instant createdFrom,
    Instant createdUntil) {

  public ExecutionQuery {
    Objects.requireNonNull(tenantId, "tenantId");
    states = Set.copyOf(Objects.requireNonNull(states, "states"));
    if (createdFrom != null && createdUntil != null && createdUntil.isBefore(createdFrom)) {
      throw new IllegalArgumentException("createdUntil must not precede createdFrom");
    }
  }
}
