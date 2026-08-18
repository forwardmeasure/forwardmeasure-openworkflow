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
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Credential-free runtime scope persisted with an authentication intent.
 *
 * <p>The adapter combines this scope with tenant-authorised secrets and evaluates the admitted
 * authentication template at the egress edge. Secret values never enter this record or the workflow
 * journal.
 */
public record AuthenticationExpressionContext(
    JsonNode context,
    JsonNode input,
    JsonNode output,
    JsonNode authorization,
    JsonNode task,
    JsonNode workflow,
    JsonNode runtime,
    Map<String, JsonNode> variables) {

  public AuthenticationExpressionContext {
    context = copy(context);
    input = copy(input);
    output = copy(output);
    authorization = copy(authorization);
    task = copy(task);
    workflow = copy(workflow);
    runtime = copy(runtime);
    variables = copyVariables(variables);
  }

  @Override
  public JsonNode context() {
    return context.deepCopy();
  }

  @Override
  public JsonNode input() {
    return input.deepCopy();
  }

  @Override
  public JsonNode output() {
    return output.deepCopy();
  }

  @Override
  public JsonNode authorization() {
    return authorization.deepCopy();
  }

  @Override
  public JsonNode task() {
    return task.deepCopy();
  }

  @Override
  public JsonNode workflow() {
    return workflow.deepCopy();
  }

  @Override
  public JsonNode runtime() {
    return runtime.deepCopy();
  }

  @Override
  public Map<String, JsonNode> variables() {
    return copyVariables(variables);
  }

  private static JsonNode copy(JsonNode value) {
    return value == null ? NullNode.getInstance() : value.deepCopy();
  }

  private static Map<String, JsonNode> copyVariables(Map<String, JsonNode> values) {
    if (values == null || values.isEmpty()) return Map.of();
    Map<String, JsonNode> result = new LinkedHashMap<>();
    values.forEach(
        (name, value) ->
            result.put(
                Objects.requireNonNull(name, "variable name"),
                Objects.requireNonNull(value, "variable " + name).deepCopy()));
    return Collections.unmodifiableMap(result);
  }
}
