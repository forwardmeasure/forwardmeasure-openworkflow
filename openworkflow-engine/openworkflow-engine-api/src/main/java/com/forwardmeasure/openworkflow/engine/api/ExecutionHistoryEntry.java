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

/** Ordered user-facing timeline entry derived idempotently from engine facts. */
public record ExecutionHistoryEntry(
    UUID eventId,
    long sequence,
    ExecutionLifecycleState state,
    String type,
    String taskPath,
    Instant occurredAt,
    JsonNode data) {

  public ExecutionHistoryEntry {
    Objects.requireNonNull(eventId, "eventId");
    if (sequence < 0) {
      throw new IllegalArgumentException("sequence must not be negative");
    }
    Objects.requireNonNull(state, "state");
    ContractSupport.requireText(type, "type");
    if (taskPath != null) {
      ContractSupport.requireText(taskPath, "taskPath");
    }
    Objects.requireNonNull(occurredAt, "occurredAt");
    data = ContractSupport.copy(data, "data");
  }

  @Override
  public JsonNode data() {
    return data.deepCopy();
  }
}
