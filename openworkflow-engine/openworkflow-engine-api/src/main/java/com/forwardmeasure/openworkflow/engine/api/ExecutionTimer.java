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
import java.util.Objects;
import java.util.UUID;

/** Canonical durable timer, independent of either engine's scheduling mechanism. */
public record ExecutionTimer(
    UUID timerId,
    String taskPath,
    String purpose,
    TimerState state,
    Instant scheduledAt,
    Instant dueAt,
    Instant resolvedAt,
    JsonNode data) {

  public ExecutionTimer {
    Objects.requireNonNull(timerId, "timerId");
    ContractSupport.requireText(taskPath, "taskPath");
    ContractSupport.requireText(purpose, "purpose");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(scheduledAt, "scheduledAt");
    Objects.requireNonNull(dueAt, "dueAt");
    if (dueAt.isBefore(scheduledAt)) {
      throw new IllegalArgumentException("dueAt must not precede scheduledAt");
    }
    data = ContractSupport.copy(data, "data");
  }

  @Override
  public JsonNode data() {
    return data.deepCopy();
  }

  public enum TimerState {
    SCHEDULED,
    FIRED,
    CANCELLED,
    SUPERSEDED
  }
}
