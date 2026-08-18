package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.time.Instant;
import java.util.Objects;

/** Durable phase of an entered Open Workflow {@code try} task. */
public record TryRuntimeState(
    Phase phase,
    int attempt,
    Instant firstAttemptAt,
    Instant attemptStartedAt,
    WorkflowError error,
    String attemptDeadlineId,
    Instant attemptDeadlineAt) {

  public enum Phase {
    BODY,
    CATCH
  }

  public TryRuntimeState {
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(firstAttemptAt, "firstAttemptAt");
    Objects.requireNonNull(attemptStartedAt, "attemptStartedAt");
    if (attempt < 0) {
      throw new IllegalArgumentException("attempt must not be negative");
    }
    if ((phase == Phase.CATCH) != (error != null)) {
      throw new IllegalArgumentException("Only a catch phase carries an error");
    }
    if ((attemptDeadlineId == null) != (attemptDeadlineAt == null)) {
      throw new IllegalArgumentException("Attempt deadline identity and time must be set together");
    }
    if (phase == Phase.CATCH && attemptDeadlineId != null) {
      throw new IllegalArgumentException("A caught attempt cannot retain an active deadline");
    }
    if (attemptDeadlineId != null && attemptDeadlineId.isBlank()) {
      throw new IllegalArgumentException("attemptDeadlineId must not be blank");
    }
  }

  public static TryRuntimeState body(Instant startedAt) {
    return new TryRuntimeState(Phase.BODY, 0, startedAt, startedAt, null, null, null);
  }

  public TryRuntimeState catching(WorkflowError caught) {
    return new TryRuntimeState(
        Phase.CATCH, attempt, firstAttemptAt, attemptStartedAt, caught, null, null);
  }

  public TryRuntimeState retrying(Instant nextAttemptAt, String deadlineId, Instant deadlineAt) {
    return new TryRuntimeState(
        Phase.BODY,
        Math.addExact(attempt, 1),
        firstAttemptAt,
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt"),
        null,
        deadlineId,
        deadlineAt);
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
