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
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import java.util.Objects;

/**
 * Immutable, fully resolved description of one reusable-function invocation.
 *
 * <p>The descriptor is persisted before the function body runs. A catalogued function therefore
 * resumes from the exact admitted resource digest and evaluated arguments; recovery never consults
 * a mutable catalog.
 */
public record FunctionOperationDescriptor(
    String operationId,
    String functionName,
    WorkflowResourceReference resource,
    JsonNode arguments) {

  public FunctionOperationDescriptor {
    operationId = requireText(operationId, "operationId");
    functionName = requireText(functionName, "functionName");
    if (resource != null && resource.kind() != WorkflowResourceKind.FUNCTION_DEFINITION) {
      throw new IllegalArgumentException(
          "A function operation resource must be a function definition");
    }
    arguments = Objects.requireNonNull(arguments, "arguments").deepCopy();
  }

  @Override
  public JsonNode arguments() {
    return arguments.deepCopy();
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
