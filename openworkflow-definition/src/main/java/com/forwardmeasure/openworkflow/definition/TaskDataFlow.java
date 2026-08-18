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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

/** Common condition, data transformations and completion flow for one task. */
public record TaskDataFlow(
    String condition,
    ResolvedDataSchema inputSchema,
    JsonNode inputFrom,
    JsonNode outputAs,
    ResolvedDataSchema outputSchema,
    JsonNode exportAs,
    ResolvedDataSchema exportSchema,
    String thenDirective) {

  public TaskDataFlow {
    condition = blankToNull(condition);
    inputFrom = copy(inputFrom);
    outputAs = copy(outputAs);
    exportAs = copy(exportAs);
    thenDirective =
        thenDirective == null ? "continue" : requireText(thenDirective, "thenDirective");
  }

  public static TaskDataFlow defaults() {
    return new TaskDataFlow(null, null, null, null, null, null, null, "continue");
  }

  @Override
  public JsonNode inputFrom() {
    return copy(inputFrom);
  }

  @Override
  public JsonNode outputAs() {
    return copy(outputAs);
  }

  @Override
  public JsonNode exportAs() {
    return copy(exportAs);
  }

  private static String blankToNull(String value) {
    return value == null ? null : requireText(value, "condition");
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static JsonNode copy(JsonNode value) {
    return value == null || value.isNull() ? null : value.deepCopy();
  }
}
