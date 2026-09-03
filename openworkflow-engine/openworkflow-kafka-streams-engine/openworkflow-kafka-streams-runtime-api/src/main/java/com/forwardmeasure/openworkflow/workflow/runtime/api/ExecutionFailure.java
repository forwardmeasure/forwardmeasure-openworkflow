package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.definition.SchemaViolation;
import java.util.List;
import java.util.Objects;

/** Structured durable fault retained by execution state and audit history. */
public record ExecutionFailure(
    String type,
    String message,
    String definitionPath,
    DataReference rejectedData,
    List<SchemaViolation> schemaViolations,
    Integer status,
    String instance,
    String title,
    String detail) {
  public static final String VALIDATION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/validation";
  public static final String EXPRESSION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/expression";

  public ExecutionFailure {
    type = requireText(type, "type");
    message = requireText(message, "message");
    definitionPath = requireText(definitionPath, "definitionPath");
    Objects.requireNonNull(rejectedData, "rejectedData");
    schemaViolations = List.copyOf(Objects.requireNonNull(schemaViolations, "schemaViolations"));
  }

  public ExecutionFailure(
      String type,
      String message,
      String definitionPath,
      DataReference rejectedData,
      List<SchemaViolation> schemaViolations) {
    this(type, message, definitionPath, rejectedData, schemaViolations, null, null, null, null);
  }

  public static ExecutionFailure fromWorkflowError(
      WorkflowError error, String definitionPath, DataReference rejectedData) {
    Objects.requireNonNull(error, "error");
    String message =
        error.detail() != null
            ? error.detail()
            : error.title() != null ? error.title() : "Workflow error " + error.type();
    return new ExecutionFailure(
        error.type(),
        message,
        definitionPath,
        rejectedData,
        List.of(),
        error.status(),
        error.instance(),
        error.title(),
        error.detail());
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
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
