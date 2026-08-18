package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Objects;

/** One transport observation with a stable adapter-assigned timestamp. */
public record ProtocolOperationObservation(
    String observationId, JsonNode value, boolean failed, boolean terminal, Instant observedAt) {
  public ProtocolOperationObservation(
      JsonNode value, boolean failed, boolean terminal, Instant observedAt) {
    this(observedAt.toString(), value, failed, terminal, observedAt);
  }

  public ProtocolOperationObservation {
    observationId = Objects.requireNonNull(observationId, "observationId");
    if (observationId.isBlank())
      throw new IllegalArgumentException("observationId must not be blank");
    value = Objects.requireNonNull(value, "value").deepCopy();
    if (failed && !terminal)
      throw new IllegalArgumentException("A failed protocol observation must be terminal");
    Objects.requireNonNull(observedAt, "observedAt");
  }

  @Override
  public JsonNode value() {
    return value.deepCopy();
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
