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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Builds the declared data-contract graph and proves every typed edge.
 *
 * <p>The analyzer follows the compiled control-flow directives, including named jumps, scope exits
 * and workflow termination. An edge is analyzed only when both endpoints declare schemas. Missing
 * schemas remain valid OpenWorkflow, but make that edge untyped rather than falsely proven.
 */
public final class WorkflowContractAnalyzer {
  private final JsonSchemaCompatibilityChecker checker;
  private final List<SchemaCompatibilityFinding> findings = new ArrayList<>();
  private Boundary workflowOutput;

  public WorkflowContractAnalyzer(List<ResolvedWorkflowResource> resources) {
    checker = new JsonSchemaCompatibilityChecker(resources);
  }

  public WorkflowContractAnalysis analyze(WorkflowPlan plan) {
    Objects.requireNonNull(plan, "plan");
    findings.clear();
    workflowOutput =
        plan.dataFlow().outputAs() == null
            ? boundary("workflow output", plan.dataFlow().outputSchema())
            : null;
    Boundary workflowInput = boundary("workflow input", plan.dataFlow().inputSchema());
    Boundary firstInput = plan.dataFlow().inputFrom() == null ? workflowInput : null;
    analyzeScope(plan.steps(), firstInput, workflowOutput);
    return new WorkflowContractAnalysis(findings);
  }

  private void analyzeScope(List<PlanStep> steps, Boundary entry, Boundary scopeOutput) {
    if (steps.isEmpty()) {
      compare(entry, scopeOutput);
      return;
    }
    compare(entry, input(steps.getFirst()));

    for (int index = 0; index < steps.size(); index++) {
      PlanStep step = steps.get(index);
      analyzeNested(step);
      if (step.kind() == PlanStepKind.RAISE) {
        continue;
      }
      for (String directive : outgoingDirectives(step)) {
        Boundary target = target(directive, index, steps, scopeOutput);
        compare(output(step), target);
        if (step.dataFlow().condition() != null) {
          compare(input(step), target);
        }
      }
    }
  }

  private void analyzeNested(PlanStep step) {
    Boundary nestedEntry = step.dataFlow().inputFrom() == null ? input(step) : null;
    Boundary nestedOutput = step.dataFlow().outputAs() == null ? output(step) : null;
    switch (step.kind()) {
      case DO -> analyzeScope(step.children(), nestedEntry, nestedOutput);
      case TRY -> {
        analyzeScope(step.tryPlan().steps(), nestedEntry, nestedOutput);
        analyzeScope(step.tryPlan().catchPlan().steps(), null, nestedOutput);
      }
      case FOR -> analyzeScope(step.children(), nestedEntry, nestedOutput);
      case FORK ->
          step.children()
              .forEach(
                  branch -> {
                    compare(nestedEntry, input(branch));
                    analyzeNested(branch);
                  });
      case CALL -> analyzeCallChildren(step, nestedEntry, nestedOutput);
      case LISTEN -> analyzeIteratorChildren(step.children(), step.listenPlan().iteratorDataFlow());
      case EXTENSION -> analyzeExtensions(step);
      default -> {
        if (!step.children().isEmpty()) {
          analyzeScope(step.children(), null, null);
        }
      }
    }
  }

  private void analyzeCallChildren(PlanStep step, Boundary nestedEntry, Boundary nestedOutput) {
    if (step.children().isEmpty()) {
      return;
    }
    if (step.callPlan().kind() == CallPlan.Kind.FUNCTION) {
      analyzeScope(step.children(), nestedEntry, nestedOutput);
      return;
    }
    TaskDataFlow iterator = step.callPlan().asyncApiSubscription().iteratorDataFlow();
    analyzeIteratorChildren(step.children(), iterator);
  }

  private void analyzeIteratorChildren(List<PlanStep> children, TaskDataFlow iterator) {
    Boundary iteratorOutput =
        iterator == null || iterator.outputAs() != null
            ? null
            : boundary("iterator output", iterator.outputSchema());
    analyzeScope(children, null, iteratorOutput);
  }

  private void analyzeExtensions(PlanStep step) {
    ExtensionPlan extension = step.extensionPlan();
    for (ExtensionApplicationPlan application : extension.applications()) {
      analyzeScope(application.before(), null, null);
      analyzeScope(application.after(), null, null);
    }
    analyzeNested(extension.target());
  }

  private Boundary target(
      String directive, int sourceIndex, List<PlanStep> steps, Boundary scopeOutput) {
    return switch (directive) {
      case "continue" ->
          sourceIndex + 1 < steps.size() ? input(steps.get(sourceIndex + 1)) : scopeOutput;
      case "exit" -> scopeOutput;
      case "end" -> workflowOutput;
      default ->
          steps.stream()
              .filter(step -> step.name().equals(directive))
              .findFirst()
              .map(this::input)
              .orElse(null);
    };
  }

  private static Set<String> outgoingDirectives(PlanStep step) {
    Set<String> result = new LinkedHashSet<>();
    result.add(step.dataFlow().thenDirective());
    step.switchCases().forEach(switchCase -> result.add(switchCase.thenDirective()));
    if (step.kind() == PlanStepKind.TRY && step.tryPlan().catchPlan().thenDirective() != null) {
      result.add(step.tryPlan().catchPlan().thenDirective());
    }
    return Set.copyOf(result);
  }

  private void compare(Boundary producer, Boundary consumer) {
    if (producer == null
        || consumer == null
        || producer.schema() == null
        || consumer.schema() == null) {
      return;
    }
    JsonSchemaCompatibilityChecker.Result result =
        checker.compare(producer.schema(), consumer.schema());
    findings.add(
        new SchemaCompatibilityFinding(
            producer.name(),
            producer.schema().definitionPath(),
            consumer.name(),
            consumer.schema().definitionPath(),
            result.status(),
            result.reason()));
  }

  private Boundary input(PlanStep step) {
    return boundary("task " + step.path() + " input", step.dataFlow().inputSchema());
  }

  private Boundary output(PlanStep step) {
    return boundary("task " + step.path() + " output", step.dataFlow().outputSchema());
  }

  private static Boundary boundary(String name, ResolvedDataSchema schema) {
    return schema == null ? null : new Boundary(name, schema);
  }

  private record Boundary(String name, ResolvedDataSchema schema) {}
}
