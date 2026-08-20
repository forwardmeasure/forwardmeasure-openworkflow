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
package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.util.List;
import java.util.Objects;

/** Concrete, wire-safe view of the durable fields needed by runtime coordinators. */
public record WorkflowRuntimeState(
    ExecutionId executionId,
    long revision,
    ExecutionStatus status,
    JsonNode data,
    List<TaskExecutionFrame> taskStack) {
  public WorkflowRuntimeState {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(status, "status");
    data = Objects.requireNonNull(data, "data").deepCopy();
    taskStack = taskStack == null ? List.of() : List.copyOf(taskStack);
  }

  public static WorkflowRuntimeState from(WorkflowState state) {
    Objects.requireNonNull(state, "state");
    return new WorkflowRuntimeState(
        state.executionId(), state.revision(), state.status(), state.data(), state.taskStack());
  }

  @Override
  public JsonNode data() {
    return data.deepCopy();
  }
}
