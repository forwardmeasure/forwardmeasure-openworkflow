package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import java.time.Instant;
import java.util.Objects;

/**
 * Durable state for graceful workflow cancellation.
 *
 * <p>The reducer retains active external operations while cancellation is in progress. Their
 * terminal observations can therefore be correlated after a rebalance or process restart. The timer
 * is the deterministic upper bound after which unresolved external outcomes are audited as unknown.
 */
public record CancellationState(
    String timerId, Instant requestedAt, Instant dueAt, ActorContext requestedBy) {

  public CancellationState {
    Objects.requireNonNull(timerId, "timerId");
    Objects.requireNonNull(requestedAt, "requestedAt");
    Objects.requireNonNull(dueAt, "dueAt");
    Objects.requireNonNull(requestedBy, "requestedBy");
    if (timerId.isBlank()) {
      throw new IllegalArgumentException("timerId must not be blank");
    }
    if (dueAt.isBefore(requestedAt)) {
      throw new IllegalArgumentException("Cancellation deadline cannot precede its request");
    }
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
