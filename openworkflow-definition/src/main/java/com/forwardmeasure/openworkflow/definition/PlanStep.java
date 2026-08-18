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

/** Immutable, addressable task in a compiled plan. */
public record PlanStep(
    String name,
    String path,
    PlanStepKind kind,
    JsonNode definition,
    JsonNode configuration,
    List<PlanStep> children,
    List<SwitchCasePlan> switchCases,
    ForPlan forPlan,
    ForkPlan forkPlan,
    ListenPlan listenPlan,
    WaitPlan waitPlan,
    RaisePlan raisePlan,
    TryPlan tryPlan,
    CallPlan callPlan,
    TaskDataFlow dataFlow,
    RunPlan runPlan,
    TimeoutPlan timeout,
    ExtensionPlan extensionPlan) {

  public PlanStep(
      String name,
      String path,
      PlanStepKind kind,
      JsonNode definition,
      JsonNode configuration,
      List<PlanStep> children,
      List<SwitchCasePlan> switchCases,
      ForPlan forPlan,
      ForkPlan forkPlan,
      ListenPlan listenPlan,
      WaitPlan waitPlan,
      RaisePlan raisePlan,
      TryPlan tryPlan,
      CallPlan callPlan,
      TaskDataFlow dataFlow,
      RunPlan runPlan,
      TimeoutPlan timeout) {
    this(
        name,
        path,
        kind,
        definition,
        configuration,
        children,
        switchCases,
        forPlan,
        forkPlan,
        listenPlan,
        waitPlan,
        raisePlan,
        tryPlan,
        callPlan,
        dataFlow,
        runPlan,
        timeout,
        null);
  }

  public PlanStep(
      String name,
      String path,
      PlanStepKind kind,
      JsonNode definition,
      JsonNode configuration,
      List<PlanStep> children,
      List<SwitchCasePlan> switchCases,
      ForPlan forPlan,
      ForkPlan forkPlan,
      ListenPlan listenPlan,
      WaitPlan waitPlan,
      RaisePlan raisePlan,
      TryPlan tryPlan,
      CallPlan callPlan,
      TaskDataFlow dataFlow,
      RunPlan runPlan) {
    this(
        name,
        path,
        kind,
        definition,
        configuration,
        children,
        switchCases,
        forPlan,
        forkPlan,
        listenPlan,
        waitPlan,
        raisePlan,
        tryPlan,
        callPlan,
        dataFlow,
        runPlan,
        null,
        null);
  }

  public PlanStep(
      String name,
      String path,
      PlanStepKind kind,
      JsonNode definition,
      JsonNode configuration,
      List<PlanStep> children,
      List<SwitchCasePlan> switchCases,
      ForPlan forPlan,
      ForkPlan forkPlan,
      ListenPlan listenPlan,
      WaitPlan waitPlan,
      RaisePlan raisePlan,
      TryPlan tryPlan,
      CallPlan callPlan,
      TaskDataFlow dataFlow) {
    this(
        name,
        path,
        kind,
        definition,
        configuration,
        children,
        switchCases,
        forPlan,
        forkPlan,
        listenPlan,
        waitPlan,
        raisePlan,
        tryPlan,
        callPlan,
        dataFlow,
        null,
        null,
        null);
  }

  public PlanStep(
      String name,
      String path,
      PlanStepKind kind,
      JsonNode definition,
      JsonNode configuration,
      List<PlanStep> children,
      List<SwitchCasePlan> switchCases,
      ForPlan forPlan,
      ForkPlan forkPlan,
      ListenPlan listenPlan,
      WaitPlan waitPlan,
      RaisePlan raisePlan,
      TryPlan tryPlan,
      TaskDataFlow dataFlow) {
    this(
        name,
        path,
        kind,
        definition,
        configuration,
        children,
        switchCases,
        forPlan,
        forkPlan,
        listenPlan,
        waitPlan,
        raisePlan,
        tryPlan,
        null,
        dataFlow,
        null,
        null,
        null);
  }

  public PlanStep {
    requireText(name, "name");
    requireText(path, "path");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(definition, "definition");
    definition = definition.deepCopy();
    configuration =
        configuration == null || configuration.isNull() ? null : configuration.deepCopy();
    children = children == null ? List.of() : List.copyOf(children);
    switchCases = switchCases == null ? List.of() : List.copyOf(switchCases);
    dataFlow = dataFlow == null ? TaskDataFlow.defaults() : dataFlow;
    if (kind == PlanStepKind.EXTENSION) {
      if (extensionPlan == null
          || configuration != null
          || !switchCases.isEmpty()
          || forPlan != null
          || forkPlan != null
          || listenPlan != null
          || waitPlan != null
          || raisePlan != null
          || tryPlan != null
          || callPlan != null
          || runPlan != null
          || !children.equals(extensionPlan.allChildren())) {
        throw new IllegalArgumentException(
            "EXTENSION requires one middleware plan and its " + "addressable child tasks");
      }
    } else if (extensionPlan != null) {
      throw new IllegalArgumentException("Only EXTENSION may carry an extension plan");
    }
    if (kind == PlanStepKind.SET && configuration == null) {
      throw new IllegalArgumentException("SET requires a configuration");
    }
    if (kind == PlanStepKind.SET
        && (!children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("SET cannot contain child steps");
    }
    if (kind == PlanStepKind.DO
        && (configuration != null
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("DO has no scalar configuration");
    }
    if (kind == PlanStepKind.SWITCH
        && (configuration == null
            || !children.isEmpty()
            || switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("SWITCH requires cases and cannot contain child steps");
    }
    if (kind == PlanStepKind.FOR
        && (configuration == null
            || children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan == null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("FOR requires compiled configuration and child steps");
    }
    if (kind == PlanStepKind.FORK
        && (configuration == null
            || children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan == null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("FORK requires compiled configuration and branches");
    }
    if (kind == PlanStepKind.EMIT
        && (configuration == null
            || !children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("EMIT requires event properties and has no child steps");
    }
    if (kind == PlanStepKind.LISTEN
        && (configuration == null
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan == null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null
            || (!children.isEmpty() && !listenPlan.foreach()))) {
      throw new IllegalArgumentException(
          "LISTEN requires a compiled subscription and only foreach " + "may contain child steps");
    }
    if (kind == PlanStepKind.WAIT
        && (configuration == null
            || !children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan == null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException(
          "WAIT requires a compiled duration and has no child steps");
    }
    if (kind == PlanStepKind.RAISE
        && (configuration == null
            || !children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan == null
            || tryPlan != null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException(
          "RAISE requires one resolved error and has no child steps");
    }
    if (kind == PlanStepKind.TRY
        && (configuration == null
            || children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan == null
            || callPlan != null
            || runPlan != null)) {
      throw new IllegalArgumentException("TRY requires compiled try and catch task lists");
    }
    if (kind == PlanStepKind.CALL
        && (configuration == null
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan == null
            || runPlan != null)) {
      throw new IllegalArgumentException("CALL requires one typed call plan");
    }
    if (kind == PlanStepKind.CALL
        && callPlan != null
        && callPlan.kind() == CallPlan.Kind.FUNCTION
        && children.size() != 1) {
      throw new IllegalArgumentException("A reusable function call requires one compiled task");
    }
    if (kind == PlanStepKind.CALL
        && callPlan != null
        && callPlan.kind() != CallPlan.Kind.FUNCTION
        && !(callPlan.asyncApiSubscription() != null && callPlan.asyncApiSubscription().foreach())
        && !children.isEmpty()) {
      throw new IllegalArgumentException("A protocol call cannot contain child steps");
    }
    if (kind == PlanStepKind.RUN
        && (configuration == null
            || !children.isEmpty()
            || !switchCases.isEmpty()
            || forPlan != null
            || forkPlan != null
            || listenPlan != null
            || waitPlan != null
            || raisePlan != null
            || tryPlan != null
            || callPlan != null
            || runPlan == null)) {
      throw new IllegalArgumentException("RUN requires one typed run plan and has no child steps");
    }
  }

  @Override
  public JsonNode definition() {
    return definition.deepCopy();
  }

  @Override
  public JsonNode configuration() {
    return configuration == null ? null : configuration.deepCopy();
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
