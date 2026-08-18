package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.Iso8601Duration;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Resolves evaluated Open Workflow durations under an operator maximum. */
public final class OperationTimeouts {
  private OperationTimeouts() {}

  public static Duration configuredOrMaximum(JsonNode value, Duration maximum) {
    Objects.requireNonNull(maximum, "maximum");
    if (value == null || value.isMissingNode() || value.isNull()) {
      return maximum;
    }
    Duration configured;
    if (value.isTextual()) {
      configured = Iso8601Duration.between(Instant.now(), value.asText());
    } else if (value.isObject()) {
      configured =
          Duration.ZERO
              .plusDays(value.path("days").asLong())
              .plusHours(value.path("hours").asLong())
              .plusMinutes(value.path("minutes").asLong())
              .plusSeconds(value.path("seconds").asLong())
              .plusMillis(value.path("milliseconds").asLong());
    } else {
      throw new IllegalArgumentException(
          "Operation timeout must be an ISO 8601 string or duration object");
    }
    if (configured.isZero() || configured.isNegative()) {
      throw new IllegalArgumentException("Operation timeout must be positive");
    }
    return configured.compareTo(maximum) < 0 ? configured : maximum;
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
