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

/** Canonical observation of an externally visible workflow effect. */
public record ExecutionEffect(
    UUID effectId,
    String taskPath,
    String kind,
    EffectState state,
    int attempt,
    Instant requestedAt,
    Instant completedAt,
    JsonNode request,
    JsonNode result,
    ExecutionError error) {

  public ExecutionEffect {
    Objects.requireNonNull(effectId, "effectId");
    ContractSupport.requireText(taskPath, "taskPath");
    ContractSupport.requireText(kind, "kind");
    Objects.requireNonNull(state, "state");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be positive");
    }
    Objects.requireNonNull(requestedAt, "requestedAt");
    request = ContractSupport.copy(request, "request");
    result = result == null ? null : result.deepCopy();
  }

  @Override
  public JsonNode request() {
    return request.deepCopy();
  }

  @Override
  public JsonNode result() {
    return result == null ? null : result.deepCopy();
  }

  public enum EffectState {
    REQUESTED,
    ACCEPTED,
    COMPLETED,
    FAILED,
    CANCELLATION_REQUESTED,
    CANCELLED
  }
}
