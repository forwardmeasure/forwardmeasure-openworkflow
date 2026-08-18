package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.time.Instant;
import java.util.Objects;

/**
 * Durable workflow or task deadline.
 *
 * <p>The timer identifier is the stable correlation key used by the timer materialisation topology.
 * A {@code null} task path denotes the workflow deadline; task deadlines retain the exact task
 * input used to evaluate and audit the timeout.
 */
public record ActiveTimeoutState(
    String timerId, String taskPath, DataReference input, Instant dueAt) {

  public ActiveTimeoutState {
    Objects.requireNonNull(timerId, "timerId");
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(dueAt, "dueAt");
    if (timerId.isBlank()) {
      throw new IllegalArgumentException("timerId must not be blank");
    }
    if (taskPath != null && taskPath.isBlank()) {
      throw new IllegalArgumentException("taskPath must not be blank");
    }
  }

  public boolean workflowTimeout() {
    return taskPath == null;
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
