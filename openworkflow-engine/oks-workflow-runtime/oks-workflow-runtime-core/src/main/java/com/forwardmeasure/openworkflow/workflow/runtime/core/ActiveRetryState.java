package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.time.Instant;
import java.util.Objects;

/** Durable retry delay resumed only by the shared timer adapter. */
public record ActiveRetryState(
    String timerId,
    String taskPath,
    DataReference retryInput,
    ExecutionCursor retryCursor,
    Instant dueAt,
    WorkflowError error)
    implements PendingInteraction {

  public ActiveRetryState {
    requireText(timerId, "timerId");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(retryInput, "retryInput");
    Objects.requireNonNull(retryCursor, "retryCursor");
    Objects.requireNonNull(dueAt, "dueAt");
    Objects.requireNonNull(error, "error");
  }

  @Override
  public String interactionId() {
    return timerId;
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
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
