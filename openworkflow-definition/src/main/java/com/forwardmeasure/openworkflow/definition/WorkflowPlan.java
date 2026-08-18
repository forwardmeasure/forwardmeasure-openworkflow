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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable executable artifact produced from one validated source document. */
public record WorkflowPlan(
    WorkflowCoordinates coordinates,
    String sourceSha256,
    String definitionSha256,
    String compilerSha256,
    JsonNode definition,
    WorkflowExpressionConfiguration expressions,
    WorkflowDataFlow dataFlow,
    List<ResolvedWorkflowResource> resources,
    List<PlanStep> steps,
    WorkflowDocumentMetadata metadata,
    TimeoutPlan timeout,
    SchedulePlan schedule) {

  public WorkflowPlan(
      WorkflowCoordinates coordinates,
      String sourceSha256,
      String definitionSha256,
      JsonNode definition,
      WorkflowExpressionConfiguration expressions,
      WorkflowDataFlow dataFlow,
      List<ResolvedWorkflowResource> resources,
      List<PlanStep> steps) {
    this(
        coordinates,
        sourceSha256,
        definitionSha256,
        OpenWorkflowCompiler.COMPILER_SHA256,
        definition,
        expressions,
        dataFlow,
        resources,
        steps,
        new WorkflowDocumentMetadata(null, null, null, null),
        null,
        null);
  }

  public WorkflowPlan {
    Objects.requireNonNull(coordinates, "coordinates");
    Objects.requireNonNull(sourceSha256, "sourceSha256");
    Objects.requireNonNull(definitionSha256, "definitionSha256");
    Objects.requireNonNull(compilerSha256, "compilerSha256");
    Objects.requireNonNull(definition, "definition");
    definition = definition.deepCopy();
    expressions = expressions == null ? WorkflowExpressionConfiguration.defaults() : expressions;
    dataFlow = dataFlow == null ? WorkflowDataFlow.identity() : dataFlow;
    resources = resources == null ? List.of() : List.copyOf(resources);
    metadata = metadata == null ? new WorkflowDocumentMetadata(null, null, null, null) : metadata;
    if (!sourceSha256.matches("[0-9a-f]{64}")
        || !definitionSha256.matches("[0-9a-f]{64}")
        || !compilerSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(
          "source, definition and compiler digests must be " + "lowercase SHA-256");
    }
    steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
    if (steps.isEmpty()) {
      throw new IllegalArgumentException("A workflow must contain at least one task");
    }
  }

  public Optional<PlanStep> findStep(String path) {
    Objects.requireNonNull(path, "path");
    return find(steps, path);
  }

  @Override
  public JsonNode definition() {
    return definition.deepCopy();
  }

  public PlanStep requireStep(String path) {
    return findStep(path)
        .orElseThrow(
            () -> new IllegalStateException("Compiled plan does not contain task " + path));
  }

  private static Optional<PlanStep> find(List<PlanStep> candidates, String path) {
    for (PlanStep candidate : candidates) {
      if (candidate.path().equals(path)) {
        return Optional.of(candidate);
      }
      Optional<PlanStep> nested = find(candidate.children(), path);
      if (nested.isPresent()) {
        return nested;
      }
    }
    return Optional.empty();
  }
}
