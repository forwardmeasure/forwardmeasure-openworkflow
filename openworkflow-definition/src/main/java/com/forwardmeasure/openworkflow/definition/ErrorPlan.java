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

/**
 * Compiled Open Workflow error whose dynamic string members are evaluated only when the error
 * occurrence is raised.
 */
public record ErrorPlan(
    JsonNode type, int status, JsonNode instance, JsonNode title, JsonNode detail) {

  public ErrorPlan {
    Objects.requireNonNull(type, "type");
    type = type.deepCopy();
    instance = copy(instance);
    title = copy(title);
    detail = copy(detail);
  }

  @Override
  public JsonNode type() {
    return type.deepCopy();
  }

  @Override
  public JsonNode instance() {
    return copy(instance);
  }

  @Override
  public JsonNode title() {
    return copy(title);
  }

  @Override
  public JsonNode detail() {
    return copy(detail);
  }

  private static JsonNode copy(JsonNode value) {
    return value == null || value.isNull() ? null : value.deepCopy();
  }
}
