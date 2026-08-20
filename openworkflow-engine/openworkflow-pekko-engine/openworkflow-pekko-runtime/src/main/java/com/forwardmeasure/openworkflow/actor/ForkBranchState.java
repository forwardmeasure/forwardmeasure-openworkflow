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
import java.util.List;
import java.util.Objects;

/** Durable execution lane owned by a fork inside the parent workflow FSM. */
public record ForkBranchState(
    String name,
    JsonNode data,
    JsonNode context,
    int nextStep,
    int endStep,
    List<TaskExecutionFrame> taskStack,
    boolean completed) {
  public ForkBranchState {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    data = Objects.requireNonNull(data, "data").deepCopy();
    context = (context == null ? data : context).deepCopy();
    if (nextStep < 0 || endStep < 0 || nextStep > endStep) {
      throw new IllegalArgumentException("Invalid fork branch cursor");
    }
    taskStack = taskStack == null ? List.of() : List.copyOf(taskStack);
    if (completed && nextStep != endStep) {
      throw new IllegalArgumentException("A completed branch must be at its end cursor");
    }
  }

  public ForkBranchState(
      String name,
      JsonNode data,
      int nextStep,
      int endStep,
      List<TaskExecutionFrame> taskStack,
      boolean completed) {
    this(name, data, data, nextStep, endStep, taskStack, completed);
  }

  public ForkBranchState advance(JsonNode nextData, int cursor, List<TaskExecutionFrame> frames) {
    return new ForkBranchState(name, nextData, context, cursor, endStep, frames, cursor == endStep);
  }

  public ForkBranchState advance(
      JsonNode nextData, JsonNode nextContext, int cursor, List<TaskExecutionFrame> frames) {
    return new ForkBranchState(
        name, nextData, nextContext, cursor, endStep, frames, cursor == endStep);
  }
}
