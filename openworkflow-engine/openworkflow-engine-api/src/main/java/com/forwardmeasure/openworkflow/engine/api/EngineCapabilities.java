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

import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.RunPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * What DSL constructs an execution engine actually implements at runtime. {@code
 * OpenWorkflowCompiler} is deliberately engine-agnostic and accepts every construct the DSL spec
 * allows, so without this check a compiled plan using a construct the selected engine doesn't
 * implement would crash or hang silently at runtime instead of failing at submission - see
 * forwardmeasure-openworkflow/docs/engine-construct-gap-audit.md for the audit this codifies.
 * Update the two constants below as engine support changes; nothing else needs to change to widen
 * or narrow what's checked.
 */
public record EngineCapabilities(
    EngineId engineId, Set<CallPlan.Kind> supportedCallKinds, Set<RunPlan.Kind> supportedRunKinds) {

  public EngineCapabilities {
    Objects.requireNonNull(engineId, "engineId");
    supportedCallKinds =
        Set.copyOf(Objects.requireNonNull(supportedCallKinds, "supportedCallKinds"));
    supportedRunKinds = Set.copyOf(Objects.requireNonNull(supportedRunKinds, "supportedRunKinds"));
  }

  public static final EngineCapabilities PEKKO =
      new EngineCapabilities(
          EngineId.PEKKO,
          EnumSet.complementOf(EnumSet.of(CallPlan.Kind.HUMAN_TASK)),
          EnumSet.allOf(RunPlan.Kind.class));

  public static final EngineCapabilities KAFKA_STREAMS =
      new EngineCapabilities(
          EngineId.KAFKA_STREAMS,
          EnumSet.complementOf(EnumSet.of(CallPlan.Kind.HUMAN_TASK)),
          EnumSet.allOf(RunPlan.Kind.class));

  public static EngineCapabilities forEngine(EngineId engineId) {
    Objects.requireNonNull(engineId, "engineId");
    if (EngineId.PEKKO.equals(engineId)) {
      return PEKKO;
    }
    if (EngineId.KAFKA_STREAMS.equals(engineId)) {
      return KAFKA_STREAMS;
    }
    throw new IllegalArgumentException("no declared capabilities for engine: " + engineId.value());
  }

  public boolean supports(CallPlan.Kind kind) {
    return supportedCallKinds.contains(kind);
  }

  public boolean supports(RunPlan.Kind kind) {
    return supportedRunKinds.contains(kind);
  }

  /**
   * The first step, in document order (depth-first), whose construct this engine doesn't implement
   * - {@code PlanStep.children()} is already the compiler's own exhaustive traversal (try's catch
   * steps, fork branches, for/do bodies, and extension middleware are all folded into it - see
   * {@code OpenWorkflowCompiler}), so a single recursive walk over it is complete.
   */
  public Optional<PlanStep> findUnsupportedStep(WorkflowPlan plan) {
    Objects.requireNonNull(plan, "plan");
    for (PlanStep step : plan.steps()) {
      Optional<PlanStep> found = findUnsupportedStep(step);
      if (found.isPresent()) {
        return found;
      }
    }
    return Optional.empty();
  }

  private Optional<PlanStep> findUnsupportedStep(PlanStep step) {
    if (step.callPlan() != null && !supportedCallKinds.contains(step.callPlan().kind())) {
      return Optional.of(step);
    }
    if (step.runPlan() != null && !supportedRunKinds.contains(step.runPlan().kind())) {
      return Optional.of(step);
    }
    for (PlanStep child : step.children()) {
      Optional<PlanStep> found = findUnsupportedStep(child);
      if (found.isPresent()) {
        return found;
      }
    }
    return Optional.empty();
  }
}
