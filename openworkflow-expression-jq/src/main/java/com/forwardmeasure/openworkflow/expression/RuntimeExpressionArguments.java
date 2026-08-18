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
package com.forwardmeasure.openworkflow.expression;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Standard Open Workflow arguments exposed to jq as {@code $context}, {@code $input}, {@code
 * $output}, {@code $secrets}, {@code $authorization}, {@code $task}, {@code $workflow} and {@code
 * $runtime}.
 */
public record RuntimeExpressionArguments(
    JsonNode context,
    JsonNode input,
    JsonNode output,
    JsonNode secrets,
    JsonNode authorization,
    JsonNode task,
    JsonNode workflow,
    JsonNode runtime,
    Map<String, JsonNode> variables) {
  private static final Pattern JQ_VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Set<String> STANDARD_VARIABLES =
      Set.of(
          "context", "input", "output", "secrets", "authorization", "task", "workflow", "runtime");

  public RuntimeExpressionArguments {
    context = copyOrNull(context);
    input = copyOrNull(input);
    output = copyOrNull(output);
    secrets = copyOrNull(secrets);
    authorization = copyOrNull(authorization);
    task = copyOrNull(task);
    workflow = copyOrNull(workflow);
    runtime = copyOrNull(runtime);
    variables = copyVariables(variables);
  }

  public RuntimeExpressionArguments(
      JsonNode context,
      JsonNode input,
      JsonNode output,
      JsonNode secrets,
      JsonNode authorization,
      JsonNode task,
      JsonNode workflow,
      JsonNode runtime) {
    this(context, input, output, secrets, authorization, task, workflow, runtime, Map.of());
  }

  public static RuntimeExpressionArguments empty() {
    return new RuntimeExpressionArguments(null, null, null, null, null, null, null, null, Map.of());
  }

  public static void validateUserVariableName(String name) {
    Objects.requireNonNull(name, "name");
    if (!JQ_VARIABLE.matcher(name).matches()) {
      throw new IllegalArgumentException("jq variable names must match " + JQ_VARIABLE.pattern());
    }
    if (STANDARD_VARIABLES.contains(name)) {
      throw new IllegalArgumentException(
          "jq variable name '" + name + "' is reserved by Open Workflow");
    }
  }

  private static JsonNode copyOrNull(JsonNode value) {
    return value == null ? NullNode.getInstance() : value.deepCopy();
  }

  private static Map<String, JsonNode> copyVariables(Map<String, JsonNode> values) {
    if (values == null || values.isEmpty()) return Map.of();
    Map<String, JsonNode> result = new LinkedHashMap<>();
    values.forEach(
        (name, value) -> {
          validateUserVariableName(name);
          result.put(name, Objects.requireNonNull(value, "variable " + name).deepCopy());
        });
    return Collections.unmodifiableMap(result);
  }
}
