package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Durable progress for an Open Workflow listen task. */
public record ActiveListenState(
    String subscriptionId,
    String taskPath,
    DataReference rawInput,
    DataReference taskInput,
    ExecutionCursor resumeCursor,
    List<DataReference> consumedEvents,
    Map<String, Set<Integer>> matchedStrategies,
    Map<String, JsonNode> correlations)
    implements PendingInteraction {

  public ActiveListenState {
    requireText(subscriptionId, "subscriptionId");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(taskInput, "taskInput");
    Objects.requireNonNull(resumeCursor, "resumeCursor");
    consumedEvents = consumedEvents == null ? List.of() : List.copyOf(consumedEvents);
    if (matchedStrategies == null) {
      matchedStrategies = Map.of();
    } else {
      Map<String, Set<Integer>> copied = new java.util.LinkedHashMap<>();
      matchedStrategies.forEach(
          (key, value) -> copied.put(key, Collections.unmodifiableSortedSet(new TreeSet<>(value))));
      matchedStrategies = Map.copyOf(copied);
    }
    correlations = correlations == null ? Map.of() : Map.copyOf(correlations);
  }

  public static ActiveListenState start(
      String subscriptionId,
      String taskPath,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCursor resumeCursor) {
    return new ActiveListenState(
        subscriptionId, taskPath, rawInput, taskInput, resumeCursor, List.of(), Map.of(), Map.of());
  }

  @Override
  public String interactionId() {
    return subscriptionId;
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
