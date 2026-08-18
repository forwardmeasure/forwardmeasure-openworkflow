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

/** Ordered, idempotently projectable fact emitted by a provider. */
public record ExecutionEvent(
    UUID eventId,
    UUID commandId,
    ExecutionId executionId,
    EngineId engineId,
    long sequence,
    EventType type,
    ExecutionLifecycleState state,
    Instant occurredAt,
    JsonNode data) {

  public ExecutionEvent {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(engineId, "engineId");
    if (sequence < 0) {
      throw new IllegalArgumentException("sequence must not be negative");
    }
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(occurredAt, "occurredAt");
    data = ContractSupport.copy(data, "data");
  }

  @Override
  public JsonNode data() {
    return data.deepCopy();
  }

  public enum EventType {
    STARTED,
    TASK_ENTERED,
    TASK_COMPLETED,
    EFFECT_REQUESTED,
    EFFECT_COMPLETED,
    TIMER_SCHEDULED,
    TIMER_FIRED,
    ERROR_RAISED,
    RETRY_SCHEDULED,
    PAUSE_REQUESTED,
    PAUSED,
    RESUMED,
    CANCELLATION_REQUESTED,
    CANCELLED,
    COMPLETED,
    FAILED
  }
}
