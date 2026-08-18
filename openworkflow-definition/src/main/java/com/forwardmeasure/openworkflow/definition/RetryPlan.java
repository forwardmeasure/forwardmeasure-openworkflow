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
package com.forwardmeasure.openworkflow.definition;

/**
 * Fully resolved retry policy. Reusable references are removed during compilation so execution
 * never depends on mutable definition lookup.
 */
public record RetryPlan(
    String when,
    String exceptWhen,
    DurationPlan delay,
    Backoff backoff,
    Integer attemptCount,
    DurationPlan attemptDuration,
    DurationPlan totalDuration,
    DurationPlan jitterFrom,
    DurationPlan jitterTo) {

  public enum Backoff {
    CONSTANT,
    LINEAR,
    EXPONENTIAL
  }

  public RetryPlan {
    backoff = backoff == null ? Backoff.CONSTANT : backoff;
    if (attemptCount != null && attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
    if ((jitterFrom == null) != (jitterTo == null)) {
      throw new IllegalArgumentException("Both jitter bounds must be present together");
    }
  }
}
