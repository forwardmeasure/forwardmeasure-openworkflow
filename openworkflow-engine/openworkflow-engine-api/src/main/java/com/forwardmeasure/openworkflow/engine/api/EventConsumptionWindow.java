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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Durable partial state for a correlated Open Workflow event-consumption strategy. */
public record EventConsumptionWindow(
    List<WorkflowCloudEvent> accepted,
    Map<String, JsonNode> correlations,
    Set<Integer> matchedFilters,
    EventConsumptionWindow untilWindow) {
  public EventConsumptionWindow(
      List<WorkflowCloudEvent> accepted,
      Map<String, JsonNode> correlations,
      Set<Integer> matchedFilters) {
    this(accepted, correlations, matchedFilters, null);
  }

  public EventConsumptionWindow {
    accepted = accepted == null ? List.of() : List.copyOf(accepted);
    correlations = correlations == null ? Map.of() : Map.copyOf(correlations);
    matchedFilters = matchedFilters == null ? Set.of() : Set.copyOf(matchedFilters);
  }

  public static EventConsumptionWindow empty() {
    return new EventConsumptionWindow(List.of(), Map.of(), Set.of(), null);
  }
}
