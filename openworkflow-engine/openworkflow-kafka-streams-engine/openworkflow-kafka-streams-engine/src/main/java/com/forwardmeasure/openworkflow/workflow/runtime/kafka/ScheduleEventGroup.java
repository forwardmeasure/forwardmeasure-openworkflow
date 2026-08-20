package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One independently correlated, durable schedule-on accumulation. */
record ScheduleEventGroup(
    Map<String, Set<Integer>> matchedStrategies,
    Map<String, JsonNode> correlations,
    List<DataReference> consumed) {

  ScheduleEventGroup {
    matchedStrategies = matchedStrategies == null ? Map.of() : copyProgress(matchedStrategies);
    correlations = correlations == null ? Map.of() : copyCorrelations(correlations);
    consumed = consumed == null ? List.of() : List.copyOf(consumed);
  }

  static ScheduleEventGroup empty() {
    return new ScheduleEventGroup(Map.of(), Map.of(), List.of());
  }

  private static Map<String, Set<Integer>> copyProgress(Map<String, Set<Integer>> source) {
    Map<String, Set<Integer>> result = new LinkedHashMap<>();
    source.forEach((key, value) -> result.put(key, Set.copyOf(value)));
    return Map.copyOf(result);
  }

  private static Map<String, JsonNode> copyCorrelations(Map<String, JsonNode> source) {
    Map<String, JsonNode> result = new LinkedHashMap<>();
    source.forEach((key, value) -> result.put(key, value.deepCopy()));
    return Map.copyOf(result);
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
